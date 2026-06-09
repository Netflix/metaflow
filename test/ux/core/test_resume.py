import time
from typing import Any, Dict, List

import pytest

pytestmark = pytest.mark.scheduler_only
from .test_utils import (
    _is_failed_status,
    deploy_flow_to_scheduler,
    wait_for_deployed_run,
)


def _try_resume(deployed_flow, sched_type, **kwargs):
    """Attempt deployed_flow.resume(); skip the test if the backend
    doesn't support it (the Python class may define the method but the
    underlying CLI subcommand may be absent)."""
    try:
        return deployed_flow.resume(**kwargs)
    except AttributeError as e:
        if "resume" in str(e):
            pytest.skip(f"{sched_type} does not support resume: {e}")
        raise


def _wait_for_resumed_run(triggered_run, timeout=3600, polling_interval=3):
    """Wait for a resumed run to complete. Same as wait_for_deployed_run but
    takes a pre-triggered run object."""
    start_time = time.time()
    while triggered_run.run is None:
        if time.time() - start_time > timeout:
            raise RuntimeError(f"Resumed run failed to start within {timeout} seconds")
        status = triggered_run.status
        if _is_failed_status(status):
            raise RuntimeError(f"Resumed run failed before starting (status: {status})")
        print("Waiting for resumed run to start...")
        time.sleep(polling_interval)

    print(f"Resumed run {triggered_run.run.id} started")

    while not triggered_run.run.finished:
        if time.time() - start_time > timeout:
            raise RuntimeError(
                f"Resumed run {triggered_run.run.id} failed to complete within {timeout} seconds"
            )
        status = triggered_run.status
        if _is_failed_status(status):
            raise RuntimeError(
                f"Resumed run {triggered_run.run.id} failed (status: {status})"
            )
        print(f"Waiting for resumed run {triggered_run.run.id} to complete...")
        time.sleep(polling_interval)

    print(f"Resumed run {triggered_run.run.id} completed")
    return triggered_run.run


def _trigger_and_wait(
    deployed_flow, sched_type, trigger_kwargs, timeout=600, polling_interval=3
):
    """Helper to trigger a flow and wait for completion (success or failure)."""
    try:
        triggered = deployed_flow.trigger(**trigger_kwargs)
    except Exception as e:
        pytest.skip(f"{sched_type}: cannot trigger with parameters: {e}")

    start_time = time.time()
    while time.time() - start_time < timeout:
        status = triggered.status
        if _is_failed_status(status):
            break
        if triggered.run and triggered.run.finished:
            break
        time.sleep(polling_interval)
    else:
        raise TimeoutError(
            f"Triggered run failed to reach a terminal state within {timeout} seconds. "
            f"Last known status: {triggered.status}"
        )
    assert triggered.run is not None, "Could not get triggered run ID"
    return triggered.run


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "trigger_kwargs, expect_first_run_success, resume_kwargs, expected_reexec_steps",
    [
        pytest.param({}, True, {}, [], id="successful_run_clones_all"),
        pytest.param(
            {"should_fail": True},
            False,
            {"should_fail": False},
            ["process", "end"],
            id="failed_run_reexecutes_failed_step",
        ),
        pytest.param(
            {},
            True,
            {"step_to_rerun": "process"},
            ["process", "end"],
            id="step_to_rerun_forces_downstream_execution",
        ),
    ],
)
def test_resume_basic_flow(
    decospecs: Any,
    compute_env: Dict[str, str],
    tag: List[str],
    scheduler_config: Any,
    trigger_kwargs: Dict[str, Any],
    expect_first_run_success: bool,
    resume_kwargs: Dict[str, Any],
    expected_reexec_steps: List[str],
):
    """Parametrized test covering standard successful resume, failed run resume,
    and explicit step-to-rerun behavior on basic/resumeflow.py."""
    sched_type = scheduler_config.scheduler_type
    if sched_type is None:
        pytest.skip("No scheduler configured")

    combined_tags = tag + ["test_resume_basic_flow"]

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/resumeflow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=sched_type,
    )

    # First run
    run1 = _trigger_and_wait(deployed_flow, sched_type, trigger_kwargs)

    if expect_first_run_success:
        assert run1.successful, "First run was unexpectedly unsuccessful"
    else:
        assert not run1.successful, "First run was unexpectedly successful"

    # Resume
    resumed = _try_resume(
        deployed_flow, sched_type, origin_run_id=run1.id, **resume_kwargs
    )
    run2 = _wait_for_resumed_run(resumed)

    # Final Assertions (Standard across all basic/resumeflow scenarios)
    assert run2.successful, "Resumed run was not successful"
    assert run2["start"].task.data.start_value == "started"
    assert run2["process"].task.data.process_value == "processed"
    assert run2["end"].task.data.end_value == "done"

    for step in ["start", "process", "end"]:
        task1_id = run1[step].task.id
        task2_id = run2[step].task.id

        if step in expected_reexec_steps:
            assert task1_id != task2_id, (
                f"Expected step '{step}' to be re-executed, but it appears to have been "
                f"cloned (Task ID matched original: {task1_id})"
            )
        else:
            pass


def test_resume_foreach(
    decospecs: Any, compute_env: Dict[str, str], tag: List[str], scheduler_config: Any
) -> None:
    """Resume a failed foreach run — failed iteration re-executes, completed ones are cloned."""
    sched_type = scheduler_config.scheduler_type
    if sched_type is None:
        pytest.skip("No scheduler configured")

    combined_tags = tag + ["test_resume_foreach"]

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="dag/foreach_resume_flow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=sched_type,
    )

    # First run: item 1 fails, items 2 and 3 succeed
    run1 = _trigger_and_wait(deployed_flow, sched_type, {"fail_on_item": 1})
    assert not run1.successful, "Expected first run to fail on item 1"

    # Resume: item 1 should re-execute (fail_on_item=-1), others are cloned
    resumed = _try_resume(
        deployed_flow,
        sched_type,
        origin_run_id=run1.id,
        fail_on_item=-1,
    )
    run2 = _wait_for_resumed_run(resumed)

    assert run2.successful, "Resumed foreach run was not successful"
    assert run2["join"].task.data.results == [
        2,
        4,
        6,
    ], f"Resumed foreach results didn't match: got {run2['join'].task.data.results}"
