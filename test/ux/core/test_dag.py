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
    if exec_mode == "deployer" and scheduler_config.scheduler_type == "step-functions":
        pytest.skip(
            "Foreach deployer is skipped for step-functions: sfn-local v2 does not "
            "support the DynamoDB ResultPath integration used for foreach cardinality."
        )
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
    if exec_mode == "deployer" and scheduler_config.scheduler_type == "airflow":
        pytest.skip("Nested foreach is not supported by the Airflow deployer")
    if exec_mode == "deployer" and scheduler_config.scheduler_type == "flyte":
        pytest.skip(
            "Nested foreach is not supported by the Flyte deployer: the codegen "
            "wires foreach-body steps as fixed tasks inside a @dynamic expander and "
            "cannot recurse to produce a second level of @dynamic fan-out for a body "
            "step that is itself a foreach."
        )
    if exec_mode == "deployer" and scheduler_config.scheduler_type == "kestra":
        pytest.skip(
            "Nested foreach is not supported by the Kestra deployer: inner join steps "
            "require per-outer-iteration task IDs that cannot be expressed with "
            "Kestra's static task-ID scheme (each ForEach body runs with a fixed task "
            "ID, so a second-level join cannot address the per-iteration inner tasks)."
        )
    if exec_mode == "deployer" and scheduler_config.scheduler_type == "step-functions":
        pytest.skip(
            "Nested foreach deployer is skipped for step-functions: sfn-local v2 does "
            "not support the DynamoDB ResultPath integration used for foreach cardinality."
        )
    run = execute_test_flow(
        flow_name="dag/nested_foreach_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="nested_foreach",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert run["outer_join"].task.data.all_results == [
        "x-1",
        "y-1",
    ], "Nested foreach all_results didn't match"
