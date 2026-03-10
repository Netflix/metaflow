"""
Tests for the @parallel decorator, which enables multi-node (gang-scheduled)
execution via Kubernetes JobSets.

These tests require:
- The argo-kubernetes backend with jobset enabled
- METAFLOW_KUBERNETES_JOBSET_ENABLED=true in the devstack config

Run with:
    pytest test/ux/core/test_parallel.py -v --only-backend argo-kubernetes
"""

import pytest

pytestmark = pytest.mark.parallel
from .test_utils import (
    execute_test_flow,
)


def test_parallel_basic(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @parallel creates multiple tasks with correct node indices."""
    # @parallel uses Kubernetes JobSets, so it only works on kubernetes backends.
    if not decospecs or not any("kubernetes" in str(d) for d in decospecs):
        pytest.skip("@parallel requires a kubernetes backend (JobSet)")

    run = execute_test_flow(
        flow_name="parallel/parallel_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="parallel_basic",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"

    # The join step should have collected node indices from all parallel tasks
    join_task = run["join"].task
    assert join_task.data.node_indices == [0, 1], (
        "Expected node_indices [0, 1], got %s" % join_task.data.node_indices
    )
    assert join_task.data.total_nodes == 2, (
        "Expected 2 total nodes, got %s" % join_task.data.total_nodes
    )
    assert join_task.data.all_have_main_ip, "Not all parallel tasks had main_ip set"
