"""
DAG topology tests — branch, foreach, nested foreach, condition, retry.

Verifies that orchestrators correctly map Metaflow DAG structures
into their native representations and execute them successfully.
"""

from typing import Any, Callable, Dict, List

import pytest
from metaflow import Run

from .test_utils import execute_test_flow

# Apply marker to all tests in this module
pytestmark = pytest.mark.dag


def _assert_branch(run: Run):
    """Verify parallel branches (split/join) execute correctly."""
    assert run.successful, "Run was not successful"
    assert run["join"].task.data.values == ["a", "b"], "Branch join values didn't match"


def _assert_foreach(run: Run):
    """Verify foreach fan-out/join executes correctly."""
    assert run.successful, "Run was not successful"
    process_tasks = list(run["process"].tasks())
    assert len(process_tasks) == 3, (
        f"Expected 3 foreach tasks for items=[1,2,3], got {len(process_tasks)}. "
        "This may indicate foreach_count fell back to 1."
    )
    assert run["join"].task.data.results == [
        2,
        4,
        6,
    ], "Foreach join results didn't match"


def _assert_multibody_foreach(run: Run):
    """Verify foreach with multiple linear body steps (process -> transform -> join)."""
    assert run.successful, "Run was not successful"
    process_tasks = list(run["process"].tasks())
    assert (
        len(process_tasks) == 3
    ), f"Expected 3 foreach process tasks, got {len(process_tasks)}"
    assert run["join"].task.data.results == [
        3,
        5,
        7,
    ], "Multi-body foreach join results didn't match"


def _assert_retry_foreach(run: Run):
    """Verify @retry on a foreach body step works — body tasks retry and succeed."""
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


def _assert_condition(run: Run):
    """Verify @condition routing executes the correct branch."""
    assert run.successful, "Run was not successful"
    # value=42 >= 10, so high_branch should have been taken
    assert (
        run["merge"].task.data.branch == "high"
    ), f"Expected high_branch, got {run['merge'].task.data.branch!r}"
    assert (
        run["merge"].task.data.result == 84
    ), f"Expected result=84 (42*2), got {run['merge'].task.data.result!r}"


def _assert_nested_foreach(run: Run):
    """Verify nested foreach (foreach inside foreach) executes correctly."""
    assert run.successful, "Run was not successful"
    assert run["outer_join"].task.data.all_results == [
        "x-1",
        "y-1",
    ], "Nested foreach all_results didn't match"


def _assert_nested_foreach_2x2(run: Run):
    """Verify nested foreach with 2 outer x 2 inner items — catches D-NESTED-1 bug."""
    assert run.successful, "Run was not successful"
    # Must have all 4 combinations: x-1, x-2, y-1, y-2
    assert run["outer_join"].task.data.all_results == ["x-1", "x-2", "y-1", "y-2"], (
        f"Expected 4 results from 2x2 nested foreach, got: {run['outer_join'].task.data.all_results}. "
        "This may indicate nested_foreach_join is not aggregating all outer items correctly."
    )
    # Verify inner task count: 4 inner tasks total (2 outer x 2 inner)
    inner_tasks = list(run["inner"].tasks())
    assert (
        len(inner_tasks) == 4
    ), f"Expected 4 inner tasks for 2x2 foreach, got {len(inner_tasks)}"


def _assert_nested_foreach_3level(run: Run):
    """Verify 3-level nested foreach compiles and executes correctly."""
    assert run.successful, "Run was not successful"
    assert run["outer_join"].task.data.all_results == [
        "a-1-10",
        "a-1-20",
        "a-2-10",
        "a-2-20",
        "b-1-10",
        "b-1-20",
        "b-2-10",
        "b-2-20",
    ], f"3-level nested foreach all_results didn't match: {run['outer_join'].task.data.all_results}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flow_name, test_name, assertion_fn, allow_unsupported",
    [
        # Standard DAG topologies
        pytest.param(
            "dag/branch_flow.py", "branch", _assert_branch, False, id="branch"
        ),
        pytest.param(
            "dag/foreach_flow.py", "foreach", _assert_foreach, False, id="foreach"
        ),
        pytest.param(
            "dag/multi_body_foreach_flow.py",
            "multibody_foreach",
            _assert_multibody_foreach,
            False,
            id="multibody_foreach",
        ),
        pytest.param(
            "dag/retry_foreach_flow.py",
            "retry_foreach",
            _assert_retry_foreach,
            False,
            id="retry_foreach",
        ),
        # Advanced/Beta topologies (may skip if unsupported by orchestrator)
        pytest.param(
            "dag/condition_flow.py",
            "condition",
            _assert_condition,
            True,
            id="condition",
        ),
        pytest.param(
            "dag/nested_foreach_flow.py",
            "nested_foreach",
            _assert_nested_foreach,
            True,
            id="nested_foreach",
        ),
        pytest.param(
            "dag/nested_foreach_2x2_flow.py",
            "nested_foreach_2x2",
            _assert_nested_foreach_2x2,
            True,
            id="nested_foreach_2x2",
        ),
        # Skipped topologies due to compute limits
        pytest.param(
            "dag/nested_foreach_3level_flow.py",
            "nested_foreach_3level",
            _assert_nested_foreach_3level,
            True,
            marks=pytest.mark.skip(
                reason="3-level 2x2x2 nested foreach = 24 sequential Mage block executions. "
                "Too slow for the 2-CPU GitHub Actions runner even with ThreadPoolExecutor. "
                "Needs larger runner or reduced topology."
            ),
            id="nested_foreach_3level",
        ),
    ],
)
def test_dag_behaviors(
    exec_mode: str,
    decospecs: Any,
    compute_env: Dict[str, str],
    tag: List[str],
    scheduler_config: Any,
    flow_name: str,
    test_name: str,
    assertion_fn: Callable[[Run], None],
    allow_unsupported: bool,
):
    """Parametrized test for all DAG structural capabilities."""

    # Lazy import to handle MetaflowException catching
    try:
        from metaflow.exception import MetaflowException
    except ImportError:
        MetaflowException = Exception

    try:
        run = execute_test_flow(
            flow_name=flow_name,
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name=test_name,
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        msg = str(e).lower()
        if allow_unsupported and (
            "not supported" in msg
            or "not yet supported" in msg
            or isinstance(e, ImportError)
            or "cannot import name" in msg
        ):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support {test_name}: {e}"
            )
        raise

    assertion_fn(run)
