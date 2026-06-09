"""
Orchestrator compliance tests — verifies the contract every backend must satisfy.

These tests are not a re-test of basic functionality; they are targeted regression
tests for the specific bugs that every new orchestrator implementation gets wrong.
Each test documents WHY the requirement exists, not just WHAT it checks.

Run with:
    pytest test/ux/core/test_compliance.py -v

Run only compliance tests across all backends:
    pytest test/ux/core/test_compliance.py -m compliance -v
"""

import uuid
import pytest

# Apply markers to all tests in this module
pytestmark = [pytest.mark.compliance, pytest.mark.scheduler_only]

from .test_utils import (
    deploy_flow_to_scheduler,
    wait_for_deployed_run,
    wait_for_deployed_run_allow_failure,
)


def _deploy_and_run_compliance(
    flow_name,
    exec_mode,
    decospecs,
    compute_env,
    tag,
    scheduler_config,
    test_suffix,
    tl_args_extra=None,
    run_kwargs=None,
    allow_failure=False,
    catch_unsupported=False,
):
    """Internal helper to remove deployment boilerplate from compliance tests."""
    if exec_mode != "deployer":
        pytest.skip("compliance test requires deployer mode")

    combined_tags = tag + [f"test_compliance_{test_suffix}_{exec_mode}"]

    env_vars = (compute_env or {}).copy()
    tl_args = {"decospecs": decospecs}

    if tl_args_extra:
        if "env" in tl_args_extra:
            env_vars.update(tl_args_extra.pop("env"))
        tl_args.update(tl_args_extra)

    tl_args["env"] = env_vars

    try:
        from metaflow.exception import MetaflowException
    except ImportError:
        MetaflowException = Exception

    try:
        deployed_flow = deploy_flow_to_scheduler(
            flow_name=flow_name,
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_config.scheduler_type,
        )
    except (MetaflowException, Exception) as e:
        msg = str(e).lower()
        if catch_unsupported and ("not supported" in msg or "not yet supported" in msg):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support this feature: {e}"
            )
        raise

    if allow_failure:
        return wait_for_deployed_run_allow_failure(deployed_flow, run_kwargs=run_kwargs)
    return wait_for_deployed_run(deployed_flow, run_kwargs=run_kwargs)


# ---------------------------------------------------------------------------
# test_run_params_multiple_values
#
# WHY: Click's multi-value options return tuples. If an orchestrator passes
# run_params directly to trigger() without converting to a list, passing two
# or more run params causes a TypeError deep inside the trigger implementation.
# This test forces the code path: deploy mutable_flow (which has trigger_param
# + param2) and trigger with both params.  If run_params is a tuple the call
# fails; if it is a list it succeeds.
# ---------------------------------------------------------------------------


def test_run_params_multiple_values(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Deployer trigger must accept a list for run_params, not a tuple."""
    trigger_param = str(uuid.uuid4())[:8]

    run = _deploy_and_run_compliance(
        flow_name="config/mutable_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_suffix="run_params",
        tl_args_extra={"env": {"METAFLOW_CLICK_API_PROCESS_CONFIG": "1"}},
        run_kwargs={"trigger_param": trigger_param, "param2": "48"},
    )

    assert (
        run.successful
    ), "Run was not successful (check that run_params is a list, not a tuple)"
    assert (
        run["start"].task.data.trigger_param == trigger_param
    ), "trigger_param not propagated — run_params may have been dropped"
    assert (
        run["start"].task.data.param2 == "48"
    ), "param2 not propagated — only the first run_param was used (tuple vs list bug)"


# ---------------------------------------------------------------------------
# test_branch_propagated_to_steps
#
# WHY: @project derives branch_name from the --branch CLI flag.  Orchestrators
# that compile a step command for the scheduler but forget to forward --branch
# to each step subprocess produce an empty or wrong branch in step tasks.
# The HelloProjectFlow stores current.branch_name in self.branch at the end
# step, so we can verify it matches the branch we passed at deploy time.
# ---------------------------------------------------------------------------


def test_branch_propagated_to_steps(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """--branch must be forwarded to each step subprocess, not just the start command."""
    branch = str(uuid.uuid4())[:8]

    run = _deploy_and_run_compliance(
        flow_name="basic/helloproject.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_suffix="branch",
        tl_args_extra={"branch": branch},
    )

    assert run.successful, "Run was not successful"
    rbranch = run["end"].task.data.branch
    expected = f"test.{branch}"
    assert rbranch == expected, (
        f"Branch name mismatch: got {rbranch!r}, expected {expected!r}. "
        "This usually means --branch was not forwarded to step subprocesses."
    )


# ---------------------------------------------------------------------------
# test_retry_count_from_scheduler
#
# WHY: Metaflow's @retry decorator uses the attempt number to decide whether
# to retry.  The attempt number must be derived from the scheduler's native
# attempt/retry counter, NOT hardcoded to 0.  When hardcoded, the flow always
# sees attempt=0 and thinks the task succeeded on the first try even when the
# scheduler is actually executing a retry.
#
# retry_flow.py has a step that deliberately fails on attempt 0 and succeeds
# on attempt 1.  If attempt is always 0, the step always fails.
# ---------------------------------------------------------------------------


def test_retry_count_from_scheduler(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Retry attempt number must come from the scheduler, not hardcoded to 0."""
    run = _deploy_and_run_compliance(
        flow_name="basic/retry_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_suffix="retry",
    )

    assert run.successful, (
        "Run was not successful — if @retry fails, the scheduler may be "
        "passing retry_count=0 instead of deriving it from the native attempt number."
    )
    attempts = run["flaky"].task.data.attempts
    assert attempts == 1, (
        f"Expected flaky step to succeed on attempt 1, but got attempts={attempts}. "
        "This means retry_count is hardcoded to 0 instead of reading the scheduler attempt."
    )


# ---------------------------------------------------------------------------
# test_config_value_propagated
#
# WHY: @config and @project use METAFLOW_FLOW_CONFIG_VALUE to carry the
# serialized config dict from the deployer into each step subprocess.  Without
# this env var, tasks run with empty/default config and @project name is wrong.
#
# config_simple.py reads a config that sets project_name; the project tag on
# the run reflects whether config was properly propagated at task runtime.
# ---------------------------------------------------------------------------


def test_config_value_propagated(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """METAFLOW_FLOW_CONFIG_VALUE must be injected so @config/@project work in tasks."""
    trigger_param = str(uuid.uuid4())[:8]

    # Override the config so project_name differs from the default.
    config_value = [
        ("cfg_default_value", {"a": {"project_name": "compliance_project", "b": "99"}})
    ]

    run = _deploy_and_run_compliance(
        flow_name="config/config_simple.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_suffix="config",
        tl_args_extra={
            "env": {"METAFLOW_CLICK_API_PROCESS_CONFIG": "1"},
            "package_suffixes": ".py,.json",
            "config_value": config_value,
        },
        run_kwargs={"trigger_param": trigger_param},
    )

    assert run.successful, "Run was not successful"

    # The project tag is set by @project using the config-derived project_name.
    expected_project_tag = "project:compliance_project"
    assert expected_project_tag in run.tags, (
        f"Expected tag {expected_project_tag!r} not found in {sorted(run.tags)}. "
        "METAFLOW_FLOW_CONFIG_VALUE was likely not injected into step subprocesses."
    )

    end_task = run["end"].task
    assert end_task.data.trigger_param == trigger_param, "trigger_param not propagated"
    assert end_task.data.config_val_2 == "99", (
        f"config_val_2 should be '99' (from override), got {end_task.data.config_val_2!r}. "
        "Config was not propagated to tasks."
    )


# ---------------------------------------------------------------------------
# test_nested_foreach_or_skip
#
# WHY: Nested foreach (foreach inside foreach) is not universally supported.
# Orchestrators that do not support it MUST raise an exception containing
# "not supported" during .create() rather than silently producing a wrong or
# partial result.  This test attempts to deploy and:
#   - if .create() raises with "not supported", skips (the orchestrator is
#     self-aware about its limitation — no hardcoded list needed here).
#   - if .create() succeeds, verifies the nested foreach result is correct.
# ---------------------------------------------------------------------------


def test_nested_foreach_or_skip(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Nested foreach must either work correctly or be rejected at deploy time with 'not supported'."""
    run = _deploy_and_run_compliance(
        flow_name="dag/nested_foreach_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_suffix="nested_foreach",
        catch_unsupported=True,
    )

    assert run.successful, "Nested foreach run was not successful"
    all_results = run["outer_join"].task.data.all_results
    assert all_results == ["x-1", "y-1"], (
        f"Nested foreach produced wrong results: {all_results!r}. "
        "Expected ['x-1', 'y-1']."
    )


# ---------------------------------------------------------------------------
# test_timeout_enforcement_behaviors
#
# WHY: The existing test_timeout only verifies that @timeout doesn't break
# normal execution (step sleeps 1s with a 10-minute timeout). We must verify
# that the orchestrator actually enforces the limit and kills the step if
# exceeded. Additionally, _get_timeout_seconds previously ignored the 'minutes'
# attribute, so we test both variants here.
# ---------------------------------------------------------------------------


@pytest.mark.skip(
    reason="@timeout enforcement on remote backends (argo/sfn/airflow) is not "
    "reliable — the run may hang instead of failing. Needs backend-specific "
    "timeout mechanisms (e.g. activeDeadlineSeconds for k8s). See #XXXX."
)
@pytest.mark.parametrize(
    "flow_name, expected_failure_msg",
    [
        pytest.param(
            "basic/timeout_enforce_flow.py",
            "Run should have failed because the 'slow' step exceeds its @timeout(seconds=5), but it succeeded. Timeout enforcement may be broken.",
            id="seconds",
        ),
        pytest.param(
            "basic/timeout_minutes_flow.py",
            "@timeout(minutes=1) was NOT enforced — the step ran for 2+ minutes without being killed. Check that _get_timeout_seconds correctly computes minutes*60+seconds.",
            id="minutes",
        ),
    ],
)
def test_timeout_enforcement_behaviors(
    exec_mode,
    decospecs,
    compute_env,
    tag,
    scheduler_config,
    flow_name,
    expected_failure_msg,
):
    """A step that exceeds its @timeout (whether set by seconds or minutes) must be killed."""
    run = _deploy_and_run_compliance(
        flow_name=flow_name,
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_suffix=f"timeout_{flow_name.split('/')[-1].split('_')[1]}",
        allow_failure=True,
    )

    assert not run.successful, expected_failure_msg


# ---------------------------------------------------------------------------
# test_run_param_not_dropped
#
# WHY: Parameters were silently dropped when trigger variables dict had None
# values or when JSON serialization lost the value.  Verify parameter values
# arrive correctly at task runtime.
# ---------------------------------------------------------------------------


def test_run_param_not_dropped(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """WHY: Parameters were silently dropped when trigger variables dict had None values
    or when JSON serialization lost the value. Verify parameter values arrive correctly.
    """
    run = _deploy_and_run_compliance(
        flow_name="basic/reserved_param_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_suffix="run_param_not_dropped",
        run_kwargs={"retry_count": 42},
    )

    assert run.successful, "Run was not successful"
    assert run["start"].task.data.stored_retry_count == 42, (
        f"Expected retry_count=42, got {run['start'].task.data.stored_retry_count}. "
        "Parameter may have been dropped or not passed correctly."
    )
