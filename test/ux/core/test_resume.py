import time
import pytest

pytestmark = pytest.mark.scheduler_only
from .test_utils import (
    deploy_flow_to_scheduler,
    wait_for_deployed_run,
    _is_failed_status,
)


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


def test_resume_hello_world(decospecs, compute_env, tag, scheduler_config):
    """Resume a successful run — all steps should be cloned."""
    sched_type = scheduler_config.scheduler_type
    if sched_type is None:
        pytest.skip("No scheduler configured")

    test_unique_tag = "test_resume_hello_world"
    combined_tags = tag + [test_unique_tag]

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/resumeflow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=sched_type,
    )

    # First run: should succeed (should_fail defaults to False)
    run1 = wait_for_deployed_run(deployed_flow)
    assert run1.successful, "First run was not successful"
    assert run1["start"].task.data.start_value == "started"
    assert run1["process"].task.data.process_value == "processed"
    assert run1["end"].task.data.end_value == "done"

    # Resume: all steps should be cloned from the successful run
    resumed = deployed_flow.resume(origin_run_id=run1.id)
    run2 = _wait_for_resumed_run(resumed)
    assert run2.successful, "Resumed run was not successful"
    assert run2["start"].task.data.start_value == "started"
    assert run2["process"].task.data.process_value == "processed"
    assert run2["end"].task.data.end_value == "done"


def test_resume_failed_flow(decospecs, compute_env, tag, scheduler_config):
    """Resume a failed run — failed step should re-execute, earlier steps cloned."""
    sched_type = scheduler_config.scheduler_type
    if sched_type is None:
        pytest.skip("No scheduler configured")

    test_unique_tag = "test_resume_failed_flow"
    combined_tags = tag + [test_unique_tag]

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/resumeflow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=sched_type,
    )

    # First run: trigger with should_fail=True — process step will fail
    triggered = deployed_flow.trigger(should_fail=True)
    start_time = time.time()
    while time.time() - start_time < 600:
        status = triggered.status
        if _is_failed_status(status):
            break
        if triggered.run and triggered.run.finished:
            break
        time.sleep(3)

    failed_run_id = triggered.run.id if triggered.run else None
    assert failed_run_id is not None, "Could not get failed run ID"

    # Resume: process and end should re-execute, start should be cloned
    resumed = deployed_flow.resume(origin_run_id=failed_run_id, should_fail=False)
    run2 = _wait_for_resumed_run(resumed)
    assert run2.successful, "Resumed run was not successful"
    assert run2["start"].task.data.start_value == "started"
    assert run2["process"].task.data.process_value == "processed"
    assert run2["end"].task.data.end_value == "done"


def test_resume_step_to_rerun(decospecs, compute_env, tag, scheduler_config):
    """Resume with --step-to-rerun forces re-execution of specified step and downstream."""
    sched_type = scheduler_config.scheduler_type
    if sched_type is None:
        pytest.skip("No scheduler configured")

    test_unique_tag = "test_resume_step_to_rerun"
    combined_tags = tag + [test_unique_tag]

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/resumeflow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=sched_type,
    )

    # First run: succeed
    run1 = wait_for_deployed_run(deployed_flow)
    assert run1.successful, "First run was not successful"

    # Resume with step_to_rerun="process" — process and end should re-execute
    resumed = deployed_flow.resume(origin_run_id=run1.id, step_to_rerun="process")
    run2 = _wait_for_resumed_run(resumed)
    assert run2.successful, "Resumed run was not successful"
    # start should be cloned (not in rerun set)
    assert run2["start"].task.data.start_value == "started"
    # process and end should have been re-executed
    assert run2["process"].task.data.process_value == "processed"
    assert run2["end"].task.data.end_value == "done"
