import uuid
import pytest

pytestmark = pytest.mark.basic
from .test_utils import (
    execute_test_flow,
    deploy_flow_to_scheduler,
    wait_for_deployed_run,
    verify_run_provenance,
)


def test_hello_world(exec_mode, decospecs, compute_env, tag, scheduler_config):
    run = execute_test_flow(
        flow_name="basic/helloworld.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="hello_world",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert (
        run["hello"].task.data.message == "Metaflow says: Hi!"
    ), "Hello world message didn't match"


def test_hello_project(exec_mode, decospecs, compute_env, tag, scheduler_config):
    branch = str(uuid.uuid4())[:8]
    run = execute_test_flow(
        flow_name="basic/helloproject.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=[branch],
        scheduler_config=scheduler_config,
        test_name="hello_project",
        tl_args_extra={"env": compute_env, "branch": branch},
    )

    assert run.successful, "Run was not successful"
    rbranch = run["end"].task.data.branch
    assert "test." + branch == rbranch, "Branch name does not match expected"


@pytest.mark.scheduler_only
def test_from_deployment(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify DeployedFlow.from_deployment() works for all schedulers."""
    from metaflow.runner.deployer import DeployedFlow

    test_unique_tag = "test_from_deployment_%s" % exec_mode
    combined_tags = tag + [test_unique_tag]

    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("No scheduler configured — deployer tests require a scheduler_type")
    # Normalize to the impl key used by DeployedFlow.from_deployment(impl=...)
    impl = scheduler_type.replace("-", "_")

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/hello_from_deployment.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags},
        scheduler_type=scheduler_type,
    )

    # First run — verify the flow itself works
    run1 = wait_for_deployed_run(deployed_flow)
    assert run1.successful, "First run was not successful"
    assert run1["start"].task.data.message == "Metaflow says: Hi!"
    verify_run_provenance(run1, decospecs)

    # Recover the deployment via from_deployment and trigger a second run
    deployment_id = deployed_flow.deployer.name
    recovered = DeployedFlow.from_deployment(deployment_id, impl=impl)
    run2 = wait_for_deployed_run(recovered)
    assert run2.successful, "Run from recovered deployment was not successful"
    assert run2["start"].task.data.message == "Metaflow says: Hi!"
    verify_run_provenance(run2, decospecs)


def test_retry(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @retry retries a failing step and succeeds on the second attempt."""
    run = execute_test_flow(
        flow_name="basic/retry_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="retry",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert run["flaky"].task.data.attempts == 1, "Expected success on retry attempt 1"


def test_resources(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @resources decorator does not break execution across backends."""
    run = execute_test_flow(
        flow_name="basic/resources_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="resources",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert run["join"].task.data.labels == [
        "medium",
        "small",
    ], "Resource branch labels didn't match"


def test_catch(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @catch stores the exception and allows the flow to continue."""
    run = execute_test_flow(
        flow_name="basic/catch_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="catch",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert (
        run["failing"].task.data.error is not None
    ), "@catch did not store the exception"


def test_timeout(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @timeout decorator does not break normal execution."""
    run = execute_test_flow(
        flow_name="basic/timeout_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="timeout",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert run["work"].task.data.done is True, "Timeout step did not complete"


@pytest.mark.conda
def test_hello_conda(exec_mode, decospecs, compute_env, tag, scheduler_config):
    run = execute_test_flow(
        flow_name="basic/helloconda.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="hello_conda",
        tl_args_extra={
            "environment": "conda",
            "env": compute_env,
        },
    )

    assert run.successful, "Run was not successful"
    assert run["v1"].task.data.lib_version == "2.5.148", "v1 version incorrect"
    assert run["v2"].task.data.lib_version == "2.5.147", "v2 version incorrect"
    assert run["combo"].task.data.lib_version == "2.5.148", "combo version incorrect"
    assert (
        run["combo"].task.data.itsdangerous_version == "2.2.0"
    ), "itsdangerous version incorrect"
