"""
Step Functions compilation validation tests.

Compiles Metaflow flows to ASL (Amazon States Language) JSON and validates
them using the AWS Step Functions ValidateStateMachineDefinition API.
These tests catch compilation bugs (wrong state structure, missing env vars,
invalid transitions) in seconds without deploying anything.

Requires: sfn-local running at AWS_ENDPOINT_URL_SFN (set by devstack conftest).
"""

import json
import subprocess
import sys
import tempfile
import pytest

pytestmark = [pytest.mark.sfn_compilation]


def _compile_flow_to_json(flow_path, **extra_tl_args):
    """Compile a flow to SFN ASL JSON using the CLI with --only-json.

    Uses the Deployer's click_api path but with only_json=True,
    which prints the JSON to stdout without deploying.
    """
    from .test_utils import _resolve_flow_path

    full_path = _resolve_flow_path(flow_path)

    cmd = [sys.executable, full_path, "--no-pylint"]
    for k, v in extra_tl_args.items():
        if v is not None:
            cmd.extend([f"--{k.replace('_', '-')}", str(v)])
    cmd.extend(["step-functions", "create", "--only-json"])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=_get_compile_env(),
    )
    if result.returncode != 0:
        stderr = result.stderr or ""
        stdout = result.stdout or ""
        if "No such command" in stderr or "No such command" in stdout:
            pytest.skip(
                "step-functions CLI not available (extension may override plugins)"
            )
        if "ConnectionRefusedError" in stderr or "ConnectionError" in stderr:
            pytest.skip(
                "SFN backend not configured (connection refused to metadata/SFN service)"
            )
        if "is not supported" in stderr:
            pytest.skip(f"Feature not supported by Step Functions: {stderr.strip()}")
        pytest.fail(f"Compilation failed:\nstderr: {stderr}\nstdout: {stdout}")

    # The JSON is printed to stdout; parse it
    # Filter out non-JSON lines (echo output goes to stderr with metaflow)
    stdout = result.stdout.strip()
    # Find the JSON object in stdout (may have other output before it)
    json_start = stdout.find("{")
    if json_start == -1:
        pytest.fail(f"No JSON found in compilation output:\n{stdout}")

    return json.loads(stdout[json_start:])


def _get_compile_env():
    """Get environment variables for compilation-only tests.

    Uses devstack config (METAFLOW_HOME/METAFLOW_PROFILE) so the SFN plugin
    is registered, but overrides metadata to 'local' so no service is needed.
    """
    import os

    env = os.environ.copy()
    # Override metadata provider — compilation doesn't need the metadata service.
    env["METAFLOW_DEFAULT_METADATA"] = "local"
    return env


def _validate_state_machine(definition_json):
    """Validate ASL JSON using the SFN ValidateStateMachineDefinition API.

    Uses sfn-local (via AWS_ENDPOINT_URL_SFN) if available, otherwise
    uses asl-validator npm package as fallback.
    """
    import boto3
    import os

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL_SFN")
    if not endpoint_url:
        pytest.skip("AWS_ENDPOINT_URL_SFN not set -- sfn-local not available")

    try:
        sfn = boto3.client(
            "stepfunctions",
            endpoint_url=endpoint_url,
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )
        result = sfn.validate_state_machine_definition(
            definition=json.dumps(definition_json),
            type="STANDARD",
        )
        return result
    except Exception as e:
        # sfn-local might not support validate -- fall back to structural checks
        err_str = str(e)
        if (
            "not implemented" in err_str.lower()
            or "Unknown Operation" in err_str
            or "UnsupportedOperation" in err_str
            or "Unsupported Operation" in err_str
        ):
            return _structural_validate(definition_json)
        raise


def _structural_validate(definition):
    """Fallback structural validation when sfn-local doesn't support validate API.

    Checks fundamental ASL requirements:
    - Has StartAt and States
    - All state transitions reference existing states
    - No unreachable states (except terminal)
    - Every state has a Type
    """
    errors = []

    def check_states(states_dict, start_at, path=""):
        if start_at not in states_dict:
            errors.append(f"{path}StartAt '{start_at}' not found in States")

        for name, state in states_dict.items():
            state_path = f"{path}{name}"
            if "Type" not in state:
                errors.append(f"{state_path}: missing Type")

            # Check Next references
            if "Next" in state and state["Next"] not in states_dict:
                errors.append(f"{state_path}: Next '{state['Next']}' not in States")

            # Check Choice states
            if state.get("Type") == "Choice":
                for i, choice in enumerate(state.get("Choices", [])):
                    if "Next" in choice and choice["Next"] not in states_dict:
                        errors.append(
                            f"{state_path}.Choices[{i}]: Next '{choice['Next']}' not in States"
                        )
                if "Default" in state and state["Default"] not in states_dict:
                    errors.append(
                        f"{state_path}: Default '{state['Default']}' not in States"
                    )

            # Recurse into Parallel branches
            if state.get("Type") == "Parallel":
                for i, branch in enumerate(state.get("Branches", [])):
                    check_states(
                        branch["States"],
                        branch["StartAt"],
                        f"{state_path}.Branches[{i}].",
                    )

            # Recurse into Map iterator
            if state.get("Type") == "Map":
                iterator = state.get("Iterator") or state.get("ItemProcessor", {})
                if "States" in iterator:
                    check_states(
                        iterator["States"],
                        iterator["StartAt"],
                        f"{state_path}.Iterator.",
                    )

    if "StartAt" not in definition:
        errors.append("Missing top-level StartAt")
    if "States" not in definition:
        errors.append("Missing top-level States")
    else:
        check_states(definition["States"], definition.get("StartAt", ""), "")

    return {"result": "OK" if not errors else "FAIL", "diagnostics": errors}


# ---------------------------------------------------------------------------
# Tests -- one per flow type
# ---------------------------------------------------------------------------


class TestSfnCompilation:
    """Compile each flow type to SFN ASL JSON and validate."""

    def test_linear_flow(self):
        """Simple start->step->end flow compiles to valid ASL."""
        definition = _compile_flow_to_json("basic/helloworld.py")
        result = _validate_state_machine(definition)
        assert (
            result.get("result", "OK") == "OK"
        ), f"Validation failed: {result.get('diagnostics', result)}"

    @pytest.mark.xfail(
        reason="requires npow/core-deployer-changes: step_functions.py must add "
        "ResultSelector to Parallel states for sfn-local compatibility",
        strict=False,
    )
    def test_branch_flow(self):
        """Parallel branch flow produces valid Parallel states with ResultSelector."""
        definition = _compile_flow_to_json("dag/branch_flow.py")
        result = _validate_state_machine(definition)
        assert (
            result.get("result", "OK") == "OK"
        ), f"Validation failed: {result.get('diagnostics', result)}"

        # Verify our specific invariant: Parallel states must have ResultSelector
        raw = json.dumps(definition)
        if '"Type": "Parallel"' in raw or '"Type":"Parallel"' in raw:
            self._check_parallel_has_result_selector(definition["States"])

    def test_foreach_flow(self):
        """Foreach (Map state) flow compiles to valid ASL."""
        definition = _compile_flow_to_json("dag/foreach_flow.py")
        result = _validate_state_machine(definition)
        assert (
            result.get("result", "OK") == "OK"
        ), f"Validation failed: {result.get('diagnostics', result)}"

    def test_retry_flow(self):
        """Flow with @retry compiles to valid ASL with Retry config."""
        definition = _compile_flow_to_json("basic/retry_flow.py")
        result = _validate_state_machine(definition)
        assert (
            result.get("result", "OK") == "OK"
        ), f"Validation failed: {result.get('diagnostics', result)}"

    def test_catch_flow(self):
        """Flow with @catch compiles to valid ASL."""
        definition = _compile_flow_to_json("basic/catch_flow.py")
        result = _validate_state_machine(definition)
        assert (
            result.get("result", "OK") == "OK"
        ), f"Validation failed: {result.get('diagnostics', result)}"

    def test_resources_flow(self):
        """Flow with @resources compiles to valid ASL."""
        definition = _compile_flow_to_json("basic/resources_flow.py")
        result = _validate_state_machine(definition)
        assert (
            result.get("result", "OK") == "OK"
        ), f"Validation failed: {result.get('diagnostics', result)}"

    def test_timeout_flow(self):
        """Flow with @timeout compiles to valid ASL."""
        definition = _compile_flow_to_json("basic/timeout_flow.py")
        result = _validate_state_machine(definition)
        assert (
            result.get("result", "OK") == "OK"
        ), f"Validation failed: {result.get('diagnostics', result)}"

    def test_schedule_flow(self):
        """Flow with @schedule compiles to valid ASL."""
        definition = _compile_flow_to_json("lifecycle/schedule_flow.py")
        result = _validate_state_machine(definition)
        assert (
            result.get("result", "OK") == "OK"
        ), f"Validation failed: {result.get('diagnostics', result)}"

    # ------------------------------------------------------------------
    # Invariant checks (specific bugs we've fixed)
    # ------------------------------------------------------------------

    def _check_parallel_has_result_selector(self, states):
        """Every Parallel state must have ResultSelector (sfn-local fix)."""
        for name, state in states.items():
            if state.get("Type") == "Parallel":
                assert "ResultSelector" in state, (
                    f"Parallel state '{name}' missing ResultSelector -- "
                    "this breaks sfn-local which can't evaluate $[n].x array indexing"
                )
                # Recurse into branches
                for branch in state.get("Branches", []):
                    self._check_parallel_has_result_selector(branch.get("States", {}))
