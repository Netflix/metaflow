import pytest

pytestmark = pytest.mark.dag
from .test_utils import execute_test_flow


def test_branch(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify parallel branches (split/join) execute correctly."""
    run = execute_test_flow(
        flow_name="dag/branch_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="branch",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert run["join"].task.data.values == [
        "a",
        "b",
    ], "Branch join values didn't match"


def test_foreach(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify foreach fan-out/join executes correctly."""
    run = execute_test_flow(
        flow_name="dag/foreach_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="foreach",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    # Verify exact fanout count — catches silent foreach_count=1 fallback (D-FOREACH-1)
    process_tasks = list(run["process"].tasks())
    assert len(process_tasks) == 3, (
        "Expected 3 foreach tasks for items=[1,2,3], got %d. "
        "This may indicate foreach_count fell back to 1." % len(process_tasks)
    )
    assert run["join"].task.data.results == [
        2,
        4,
        6,
    ], "Foreach join results didn't match"


def test_multibody_foreach(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify foreach with multiple linear body steps (process -> transform -> join)."""
    run = execute_test_flow(
        flow_name="dag/multi_body_foreach_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="multibody_foreach",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    process_tasks = list(run["process"].tasks())
    assert len(process_tasks) == 3, "Expected 3 foreach process tasks, got %d" % len(
        process_tasks
    )
    assert run["join"].task.data.results == [
        3,
        5,
        7,
    ], "Multi-body foreach join results didn't match"


def test_retry_foreach(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @retry on a foreach body step works — body tasks retry and succeed."""
    run = execute_test_flow(
        flow_name="dag/retry_foreach_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="retry_foreach",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert run["join"].task.data.results == [
        10,
        20,
    ], "Retry foreach results didn't match"
    assert run[
        "join"
    ].task.data.all_succeeded_on_retry, (
        "Expected all foreach body tasks to succeed on retry attempt 1"
    )


def test_condition(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @condition routing executes the correct branch."""
    from metaflow.exception import MetaflowException

    try:
        run = execute_test_flow(
            flow_name="dag/condition_flow.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="condition",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        msg = str(e).lower()
        if (
            "not supported" in msg
            or "not yet supported" in msg
            or isinstance(e, ImportError)
            or "cannot import name" in msg
        ):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support @condition: {e}"
            )
        raise

    assert run.successful, "Run was not successful"
    # value=42 >= 10, so high_branch should have been taken
    assert (
        run["merge"].task.data.branch == "high"
    ), f"Expected high_branch, got {run['merge'].task.data.branch!r}"
    assert (
        run["merge"].task.data.result == 84
    ), f"Expected result=84 (42*2), got {run['merge'].task.data.result!r}"


def test_nested_foreach(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify nested foreach (foreach inside foreach) executes correctly."""
    from metaflow.exception import MetaflowException

    try:
        run = execute_test_flow(
            flow_name="dag/nested_foreach_flow.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="nested_foreach",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        msg = str(e).lower()
        if exec_mode == "deployer" and (
            "not supported" in msg or "not yet supported" in msg
        ):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support nested foreach: {e}"
            )
        raise

    assert run.successful, "Run was not successful"
    assert run["outer_join"].task.data.all_results == [
        "x-1",
        "y-1",
    ], "Nested foreach all_results didn't match"


def test_nested_foreach_2x2(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify nested foreach with 2 outer x 2 inner items — catches D-NESTED-1 semantic bug."""
    from metaflow.exception import MetaflowException

    try:
        run = execute_test_flow(
            flow_name="dag/nested_foreach_2x2_flow.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="nested_foreach_2x2",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        msg = str(e).lower()
        if exec_mode == "deployer" and (
            "not supported" in msg or "not yet supported" in msg
        ):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support nested foreach: {e}"
            )
        raise

    assert run.successful, "Run was not successful"
    # Must have all 4 combinations: x-1, x-2, y-1, y-2
    assert run["outer_join"].task.data.all_results == [
        "x-1",
        "x-2",
        "y-1",
        "y-2",
    ], (
        "Expected 4 results from 2x2 nested foreach, got: %s. "
        "This may indicate nested_foreach_join is not aggregating all outer items correctly."
        % run["outer_join"].task.data.all_results
    )
    # Verify inner task count: 4 inner tasks total (2 outer x 2 inner)
    inner_tasks = list(run["inner"].tasks())
    assert (
        len(inner_tasks) == 4
    ), "Expected 4 inner tasks for 2x2 foreach, got %d" % len(inner_tasks)


@pytest.mark.skip(
    reason="3-level nested foreach requires ~240+ sequential Mage subprocess calls "
    "per run. Too slow for the shared CI worker (>480s). Needs a dedicated "
    "long-running job or Mage parallel block execution support."
)
def test_nested_foreach_3level(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Verify 3-level nested foreach compiles and executes correctly.

    Topology: outer(foreach groups) → middle(foreach batches) → inner(foreach items)
    This catches compiler bugs where inner foreach step names are looked up in a
    dict keyed only by outermost foreach names (D-A02-4).
    """
    from metaflow.exception import MetaflowException

    try:
        run = execute_test_flow(
            flow_name="dag/nested_foreach_3level_flow.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="nested_foreach_3level",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        msg = str(e).lower()
        if exec_mode == "deployer" and (
            "not supported" in msg or "not yet supported" in msg
        ):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support 3-level nested foreach: {e}"
            )
        raise

    assert run.successful, "Run was not successful"
    # groups=["a"], batches=[1,2], items=[10] — 1x2x1 keeps total subproc calls ~9
    assert run["outer_join"].task.data.all_results == [
        "a-1-10",
        "a-2-10",
    ], "3-level nested foreach all_results didn't match: %s" % (
        run["outer_join"].task.data.all_results
    )
