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

pytestmark = [pytest.mark.compliance, pytest.mark.scheduler_only]

from .test_utils import (
    deploy_flow_to_scheduler,
    wait_for_deployed_run,
    wait_for_deployed_run_allow_failure,
)


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


@pytest.mark.compliance
@pytest.mark.scheduler_only
def test_run_params_multiple_values(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Deployer trigger must accept a list for run_params, not a tuple."""
    if exec_mode != "deployer":
        pytest.skip("compliance test requires deployer mode")

    trigger_param = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_compliance_run_params_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {
        "env": {
            "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
            **compute_env,
        },
        "decospecs": decospecs,
    }

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="config/mutable_flow.py",
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_config.scheduler_type,
    )

    # Pass two run_params as a list.  If the orchestrator passes a tuple here,
    # the trigger() implementation raises TypeError before the run starts.
    run_kwargs = {"trigger_param": trigger_param, "param2": "48"}
    run = wait_for_deployed_run(deployed_flow, run_kwargs=run_kwargs)

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


@pytest.mark.compliance
@pytest.mark.scheduler_only
def test_branch_propagated_to_steps(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """--branch must be forwarded to each step subprocess, not just the start command."""
    if exec_mode != "deployer":
        pytest.skip("compliance test requires deployer mode")

    branch = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_compliance_branch_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {
        "env": compute_env,
        "decospecs": decospecs,
        "branch": branch,
    }

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/helloproject.py",
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_config.scheduler_type,
    )

    run = wait_for_deployed_run(deployed_flow)

    assert run.successful, "Run was not successful"
    rbranch = run["end"].task.data.branch
    expected = "test." + branch
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


@pytest.mark.compliance
@pytest.mark.scheduler_only
def test_retry_count_from_scheduler(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Retry attempt number must come from the scheduler, not hardcoded to 0."""
    if exec_mode != "deployer":
        pytest.skip("compliance test requires deployer mode")

    test_unique_tag = f"test_compliance_retry_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {
        "env": compute_env,
        "decospecs": decospecs,
    }

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/retry_flow.py",
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_config.scheduler_type,
    )

    run = wait_for_deployed_run(deployed_flow)

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


@pytest.mark.compliance
@pytest.mark.scheduler_only
def test_config_value_propagated(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """METAFLOW_FLOW_CONFIG_VALUE must be injected so @config/@project work in tasks."""
    if exec_mode != "deployer":
        pytest.skip("compliance test requires deployer mode")

    trigger_param = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_compliance_config_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    # Override the config so project_name differs from the default.
    config_value = [
        ("cfg_default_value", {"a": {"project_name": "compliance_project", "b": "99"}})
    ]

    tl_args = {
        "env": {
            "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
            **compute_env,
        },
        "package_suffixes": ".py,.json",
        "config_value": config_value,
        "decospecs": decospecs,
    }

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="config/config_simple.py",
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_config.scheduler_type,
    )

    run = wait_for_deployed_run(
        deployed_flow, run_kwargs={"trigger_param": trigger_param}
    )

    assert run.successful, "Run was not successful"

    # The project tag is set by @project using the config-derived project_name.
    # If METAFLOW_FLOW_CONFIG_VALUE was not injected, the project tag will use
    # the default project_name ("config_project") instead of "compliance_project".
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


@pytest.mark.compliance
@pytest.mark.scheduler_only
def test_nested_foreach_or_skip(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Nested foreach must either work correctly or be rejected at deploy time with 'not supported'."""
    if exec_mode != "deployer":
        pytest.skip("compliance test requires deployer mode")

    from metaflow.exception import MetaflowException

    test_unique_tag = f"test_compliance_nested_foreach_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {
        "env": compute_env,
        "decospecs": decospecs,
    }

    # Let the orchestrator tell us whether it supports nested foreach.
    # If .create() raises with "not supported", skip — the orchestrator
    # correctly rejects the unsupported graph.  No hardcoded dict needed.
    try:
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="dag/nested_foreach_flow.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_config.scheduler_type,
        )
    except (MetaflowException, Exception) as e:
        msg = str(e).lower()
        if "not supported" in msg or "not yet supported" in msg:
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support nested foreach: {e}"
            )
        raise  # unexpected error — let the test fail normally

    run = wait_for_deployed_run(deployed_flow)

    assert run.successful, "Nested foreach run was not successful"
    all_results = run["outer_join"].task.data.all_results
    assert all_results == ["x-1", "y-1"], (
        f"Nested foreach produced wrong results: {all_results!r}. "
        "Expected ['x-1', 'y-1']."
    )


# ---------------------------------------------------------------------------
# test_timeout_enforcement
#
# WHY: The existing test_timeout only verifies that @timeout doesn't break
# normal execution (step sleeps 1s with a 10-minute timeout — always passes).
# If the timeout= kwarg on subprocess.run() is completely broken, that test
# still passes.  This test deploys a flow where a step sleeps well beyond its
# @timeout(seconds=5) and verifies the run actually fails.
# ---------------------------------------------------------------------------


@pytest.mark.compliance
@pytest.mark.scheduler_only
@pytest.mark.skip(
    reason="@timeout enforcement on remote backends (argo/sfn/airflow) is not "
    "reliable — the run may hang instead of failing. Needs backend-specific "
    "timeout mechanisms (e.g. activeDeadlineSeconds for k8s). See #XXXX."
)
def test_timeout_enforcement(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """A step that exceeds its @timeout must be killed — the run must fail."""
    if exec_mode != "deployer":
        pytest.skip("compliance test requires deployer mode")

    test_unique_tag = f"test_compliance_timeout_enforce_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {
        "env": compute_env,
        "decospecs": decospecs,
    }

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/timeout_enforce_flow.py",
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_config.scheduler_type,
    )

    run = wait_for_deployed_run_allow_failure(deployed_flow)

    assert not run.successful, (
        "Run should have failed because the 'slow' step exceeds its "
        "@timeout(seconds=5), but it succeeded. Timeout enforcement may be broken."
    )
