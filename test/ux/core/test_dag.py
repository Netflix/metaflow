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
    assert run["join"].task.data.results == [
        2,
        4,
        6,
    ], "Foreach join results didn't match"


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
