import time
import uuid
import pytest

from .test_utils import (
    deploy_flow_to_scheduler,
    execute_test_flow,
    verify_run_provenance,
    wait_for_deployed_run,
)

pytestmark = pytest.mark.basic

# ---------------------------------------------------------------------------
# Assertion Callbacks for Basic Flows
# ---------------------------------------------------------------------------


def _assert_hello_world(run):
    assert run.successful, "Run was not successful"
    assert (
        run["hello"].task.data.message == "Metaflow says: Hi!"
    ), "Hello world message didn't match"


def _assert_retry(run):
    assert run.successful, "Run was not successful"
    assert run["flaky"].task.data.attempts == 1, "Expected success on retry attempt 1"


def _assert_resources(run):
    assert run.successful, "Run was not successful"
    assert run["join"].task.data.labels == [
        "medium",
        "small",
    ], "Resource branch labels didn't match"


def _assert_catch(run):
    assert run.successful, "Run was not successful"
    assert (
        run["failing"].task.data.error is not None
    ), "@catch did not store the exception"


def _assert_timeout(run):
    assert run.successful, "Run was not successful"
    assert run["work"].task.data.done is True, "Timeout step did not complete"


def _assert_resources_cpu(run):
    assert run.successful, "Run was not successful"
    assert (
        run["end"].task.data.message == "Metaflow says: Hi Resources CPU!"
    ), "Message didn't match"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flow_name, test_name, assertion_fn, extra_marks",
    [
        pytest.param(
            "basic/helloworld.py",
            "hello_world",
            _assert_hello_world,
            [],
            id="hello_world",
        ),
        pytest.param("basic/retry_flow.py", "retry", _assert_retry, [], id="retry"),
        pytest.param(
            "basic/resources_flow.py",
            "resources",
            _assert_resources,
            [],
            id="resources",
        ),
        pytest.param("basic/catch_flow.py", "catch", _assert_catch, [], id="catch"),
        pytest.param(
            "basic/timeout_flow.py", "timeout", _assert_timeout, [], id="timeout"
        ),
        pytest.param(
            "basic/resources_cpu_flow.py",
            "resources_cpu",
            _assert_resources_cpu,
            marks=pytest.mark.scheduler_only,
            id="resources_cpu",
        ),
    ],
)
def test_basic_flow_behaviors(
    exec_mode,
    decospecs,
    compute_env,
    tag,
    scheduler_config,
    request,
    flow_name,
    test_name,
    assertion_fn,
    extra_marks,
):
    """Parametrized test for standard flow features."""

    run = execute_test_flow(
        flow_name=flow_name,
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=test_name,
        tl_args_extra={"env": compute_env},
    )

    assertion_fn(run)


def test_hello_project(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify branch propagation."""
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
    assert f"test.{branch}" == rbranch, "Branch name does not match expected"


@pytest.mark.scheduler_only
def test_from_deployment(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify DeployedFlow.from_deployment() works for all schedulers."""
    from metaflow.runner.deployer import DeployedFlow

    test_unique_tag = f"test_from_deployment_{exec_mode}"
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
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_type,
    )

    # First run — verify the flow itself works
    run1 = wait_for_deployed_run(deployed_flow)
    assert run1.successful, "First run was not successful"
    assert run1["start"].task.data.message == "Metaflow says: Hi!"
    verify_run_provenance(run1, decospecs)

    # Recover via the plain deployer name (all schedulers support this)
    deployment_id = deployed_flow.deployer.name
    recovered = DeployedFlow.from_deployment(deployment_id, impl=impl)
    run2 = wait_for_deployed_run(recovered)
    assert run2.successful, "Run from recovered deployment was not successful"
    assert run2["start"].task.data.message == "Metaflow says: Hi!"
    verify_run_provenance(run2, decospecs)

    # Recover via the full JSON id (scheduler-specific; skip if not supported)
    full_id = getattr(deployed_flow, "id", None)
    if full_id and full_id != deployment_id:
        recovered2 = DeployedFlow.from_deployment(full_id, impl=impl)
        run3 = wait_for_deployed_run(recovered2)
        assert run3.successful, "Run from JSON id recovery was not successful"
        assert run3["start"].task.data.message == "Metaflow says: Hi!"


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


@pytest.mark.scheduler_only
@pytest.mark.deployer
def test_fail_flow_reports_failed_status(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Verify schedulers report FAILED (not RUNNING/PENDING) when a step raises."""
    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("No scheduler configured — requires a scheduler_type")

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/fail_flow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": tag, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_type,
    )

    triggered = deployed_flow.trigger()
    deadline = time.time() + 300
    final_status = None

    while time.time() < deadline:
        s = triggered.status
        # Normalize to uppercase — Argo returns "Failed"/"Succeeded", SFN "FAILED"/"SUCCEEDED"
        if s and s.upper() in ("FAILED", "SUCCEEDED"):
            final_status = s.upper()
            break
        time.sleep(5)

    assert (
        final_status == "FAILED"
    ), f"A flow that raises RuntimeError mid-step should report FAILED, got {final_status}"


@pytest.mark.scheduler_only
@pytest.mark.deployer
def test_split_in_branch_deployer(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Verify a split nested inside a branch compiles and executes correctly."""
    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("No scheduler configured — requires a scheduler_type")

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/split_in_branch_flow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": tag, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_type,
    )

    run = wait_for_deployed_run(deployed_flow)
    assert run.successful, "SplitInBranchFlow should complete successfully"
    assert sorted(run["outer_join"].task.data.labels) == [
        "a",
        "b",
    ], "outer_join should receive results from both branch_a and branch_b"
    assert sorted(run["inner_join"].task.data.sub_labels) == [
        "x",
        "y",
    ], "inner_join should receive results from inner_x and inner_y"


# ---------------------------------------------------------------------------
# Custom Endpoint Verification
# ---------------------------------------------------------------------------


def _assert_custom_steps(run):
    assert (
        run["finish"].task.data.result
        == "Hello from custom start step -> processed -> done"
    ), "Data did not flow through custom-named steps"


def _assert_single_step(run):
    assert run["only"].task.data.result == 42, "Single step data incorrect"


def _assert_custom_branch(run):
    assert sorted(run["done"].task.data.result) == [
        "left",
        "right",
    ], "Branch data did not merge correctly"


@pytest.mark.parametrize(
    "flow_name, test_name, expected_steps, expected_start, expected_end, assertion_fn",
    [
        pytest.param(
            "basic/hello_custom_steps.py",
            "custom_step_names",
            {"begin", "process", "finish"},
            "begin",
            "finish",
            _assert_custom_steps,
            id="custom_step_names",
        ),
        pytest.param(
            "basic/single_step_flow.py",
            "single_step",
            {"only"},
            "only",
            "only",
            _assert_single_step,
            id="single_step",
        ),
        pytest.param(
            "basic/custom_branch_flow.py",
            "custom_branch",
            {"entry", "left", "right", "merge", "done"},
            "entry",
            "done",
            _assert_custom_branch,
            id="custom_branch",
        ),
    ],
)
def test_custom_endpoints_behaviors(
    exec_mode,
    decospecs,
    compute_env,
    tag,
    scheduler_config,
    flow_name,
    test_name,
    expected_steps,
    expected_start,
    expected_end,
    assertion_fn,
):
    """Verify various flow structures with @step(start=True)/@step(end=True) annotations."""
    run = execute_test_flow(
        flow_name=flow_name,
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=test_name,
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    step_names = {step.id for step in run}
    assert (
        step_names == expected_steps
    ), f"Expected custom step names {expected_steps}, got {step_names}"

    assertion_fn(run)

    # Verify graph endpoint metadata is persisted and readable via client API.
    start, end = run._graph_endpoints
    assert start == expected_start, f"Expected start_step={expected_start}, got {start}"
    assert end == expected_end, f"Expected end_step={expected_end}, got {end}"
    assert run.end_task is not None, "end_task should resolve for custom terminal step"


@pytest.mark.scheduler_only
@pytest.mark.skip(reason="devstack has no GPU nodes")
def test_resources_gpu(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @resources(gpu=1) deploys successfully (no actual GPU validation)."""
    run = execute_test_flow(
        flow_name="basic/resources_gpu_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="resources_gpu",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert run["gpu_step"].task.data.gpu_requested is True
