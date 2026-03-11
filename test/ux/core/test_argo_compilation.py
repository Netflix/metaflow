"""
Argo Workflows compilation validation tests.

Compiles Metaflow flows to Argo WorkflowTemplate JSON and validates
them using `argo lint --offline` or structural checks.
These tests catch compilation bugs in seconds without deploying.

Requires: `argo` CLI in PATH (installed by devstack) for full validation.
"""

import json
import subprocess
import sys
import tempfile
import os
import pytest

pytestmark = [pytest.mark.argo_compilation]


def _compile_flow_to_json(flow_path, **extra_tl_args):
    """Compile a flow to Argo WorkflowTemplate JSON using CLI with --only-json."""
    from .test_utils import _resolve_flow_path

    full_path = _resolve_flow_path(flow_path)

    cmd = [sys.executable, full_path, "--no-pylint"]

    # Argo needs a namespace
    if "namespace" not in extra_tl_args:
        extra_tl_args.setdefault("kubernetes_namespace", "default")

    for k, v in extra_tl_args.items():
        if v is not None:
            cmd.extend([f"--{k.replace('_', '-')}", str(v)])
    cmd.extend(["argo-workflows", "create", "--only-json"])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=os.environ.copy(),
    )
    if result.returncode != 0:
        stderr = result.stderr or ""
        stdout = result.stdout or ""
        if "No such command" in stderr or "No such command" in stdout:
            pytest.skip(
                "argo-workflows CLI not available (extension may override plugins)"
            )
        if "ConnectionRefusedError" in stderr or "ConnectionError" in stderr:
            pytest.skip("Argo backend not configured (connection refused)")
        if "is not supported" in stderr:
            pytest.skip(f"Feature not supported by Argo: {stderr.strip()}")
        pytest.fail(f"Compilation failed:\nstderr: {stderr}\nstdout: {stdout}")

    stdout = result.stdout.strip()
    json_start = stdout.find("{")
    if json_start == -1:
        pytest.fail(f"No JSON found in compilation output:\n{stdout}")

    return json.loads(stdout[json_start:])


def _validate_workflow_template(template_json):
    """Validate WorkflowTemplate JSON using argo lint --offline."""
    # Write to temp file for argo lint
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(template_json, f)
        tmp_path = f.name

    try:
        # Try argo lint --offline
        result = subprocess.run(
            ["argo", "lint", "--offline", "--kinds", "workflowtemplates", tmp_path],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            return {"result": "OK", "output": result.stdout}
        else:
            return {"result": "FAIL", "diagnostics": result.stderr + result.stdout}
    except FileNotFoundError:
        # argo CLI not available -- fall back to structural validation
        return _structural_validate(template_json)
    finally:
        os.unlink(tmp_path)


def _structural_validate(template):
    """Fallback structural validation when argo CLI is not available.

    Checks fundamental WorkflowTemplate requirements:
    - Has apiVersion, kind, metadata, spec
    - Has at least one template
    - Has an entrypoint that references an existing template
    - All template references resolve
    """
    errors = []

    if template.get("kind") != "WorkflowTemplate":
        errors.append(
            f"kind should be 'WorkflowTemplate', got '{template.get('kind')}'"
        )

    if "metadata" not in template:
        errors.append("Missing metadata")

    spec = template.get("spec", {})
    if not spec:
        errors.append("Missing spec")
        return {"result": "FAIL", "diagnostics": errors}

    templates = {t["name"]: t for t in spec.get("templates", [])}
    if not templates:
        errors.append("No templates defined in spec")

    entrypoint = spec.get("entrypoint")
    if entrypoint and entrypoint not in templates:
        errors.append(f"Entrypoint '{entrypoint}' not found in templates")

    # Check that all template references in steps/dag resolve
    for name, tmpl in templates.items():
        # DAG tasks
        for task in tmpl.get("dag", {}).get("tasks", []):
            ref = task.get("template", "")
            if ref and ref not in templates:
                errors.append(
                    f"Template '{name}' task '{task.get('name')}' "
                    f"references unknown template '{ref}'"
                )

        # Steps (list of list)
        for step_group in tmpl.get("steps", []):
            if isinstance(step_group, list):
                for step in step_group:
                    ref = step.get("template", "")
                    if ref and ref not in templates:
                        errors.append(
                            f"Template '{name}' step references "
                            f"unknown template '{ref}'"
                        )

    return {"result": "OK" if not errors else "FAIL", "diagnostics": errors}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestArgoCompilation:
    """Compile each flow type to Argo WorkflowTemplate and validate."""

    def test_linear_flow(self):
        """Simple start->step->end flow compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("basic/helloworld.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"

    def test_branch_flow(self):
        """Parallel branch flow compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("dag/branch_flow.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"

    def test_foreach_flow(self):
        """Foreach flow compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("dag/foreach_flow.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"

    def test_nested_foreach_flow(self):
        """Nested foreach flow compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("dag/nested_foreach_flow.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"

    def test_retry_flow(self):
        """Flow with @retry compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("basic/retry_flow.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"

    def test_resources_flow(self):
        """Flow with @resources compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("basic/resources_flow.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"

    def test_trigger_flow(self):
        """Flow with @trigger compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("events/trigger_flow.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"

    def test_catch_flow(self):
        """Flow with @catch compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("basic/catch_flow.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"

    def test_timeout_flow(self):
        """Flow with @timeout compiles to valid WorkflowTemplate."""
        template = _compile_flow_to_json("basic/timeout_flow.py")
        result = _validate_workflow_template(template)
        assert (
            result["result"] == "OK"
        ), f"Validation failed: {result.get('diagnostics')}"
