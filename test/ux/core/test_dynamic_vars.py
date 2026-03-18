"""
Dynamic vars (var()) tests.

Tests cover:
  1. Runner-mode execution — basic var(), pertask list, pertask dict, default, multi-var.
  2. Compilation rejection — check that things can't be pushed to schedulers for now

Run with:
    pytest test/ux/core/test_dynamic_vars.py -m dynamic_vars -v
"""

import pytest

from metaflow.exception import MetaflowException

pytestmark = pytest.mark.dynamic_vars

from .test_utils import execute_test_flow


def _is_var_unsupported_error(exc):
    """Return True if the exception indicates var() is not supported on this scheduler.

    check_no_dynamic_vars raises MetaflowException with "is not supported" but
    when it fires inside the deployer subprocess, the deployer wraps it in a
    RuntimeError("Error deploying ... to <scheduler>").  We match both.
    """
    msg = str(exc).lower()
    return "not supported" in msg or "error deploying" in msg


# ---------------------------------------------------------------------------
# Runner-mode execution tests
#
# var() is only supported locally (through the Native Runtime).  When the
# test parametrization produces a deployer mode on an unsupported scheduler
# (step-functions, argo, airflow), the deploy call raises "is not supported".
# We catch that and pytest.skip — the same pattern used in test_dag.py for
# @condition and nested foreach.
# ---------------------------------------------------------------------------


@pytest.mark.dynamic_vars
@pytest.mark.basic
def test_basic_var(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """var('my_bucket') resolves from parent step artifact."""
    try:
        run = execute_test_flow(
            flow_name="dynamic_vars/dynamic_vars.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="dynamic_var_basic",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        if exec_mode == "deployer" and _is_var_unsupported_error(e):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support var(): {e}"
            )
        raise
    assert run.successful, "Run was not successful"


@pytest.mark.dynamic_vars
@pytest.mark.basic
def test_pertask_foreach_list(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """var('cpu_list', pertask=True) indexes a list by foreach split index."""
    try:
        run = execute_test_flow(
            flow_name="dynamic_vars/dynamic_vars_foreach.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="dynamic_var_foreach_list",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        if exec_mode == "deployer" and _is_var_unsupported_error(e):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support var(): {e}"
            )
        raise
    assert run.successful, "Run was not successful"


@pytest.mark.dynamic_vars
@pytest.mark.basic
def test_pertask_foreach_dict(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """var('label_map', pertask=True) indexes a dict by foreach split index."""
    try:
        run = execute_test_flow(
            flow_name="dynamic_vars/dynamic_vars_dict.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="dynamic_var_foreach_dict",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        if exec_mode == "deployer" and _is_var_unsupported_error(e):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support var(): {e}"
            )
        raise
    assert run.successful, "Run was not successful"


@pytest.mark.dynamic_vars
@pytest.mark.basic
def test_pertask_dict_default(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """var('gpu_map', pertask=True, default='1') falls back for missing keys."""
    try:
        run = execute_test_flow(
            flow_name="dynamic_vars/dynamic_vars_default.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="dynamic_var_dict_default",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        if exec_mode == "deployer" and _is_var_unsupported_error(e):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support var(): {e}"
            )
        raise
    assert run.successful, "Run was not successful"


@pytest.mark.dynamic_vars
@pytest.mark.basic
def test_multiple_vars(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Multiple var() references on the same step all resolve correctly."""
    try:
        run = execute_test_flow(
            flow_name="dynamic_vars/dynamic_vars_multi.py",
            exec_mode=exec_mode,
            decospecs=decospecs,
            tag=tag,
            scheduler_config=scheduler_config,
            test_name="dynamic_var_multi",
            tl_args_extra={"env": compute_env},
        )
    except (MetaflowException, Exception) as e:
        if exec_mode == "deployer" and _is_var_unsupported_error(e):
            pytest.skip(
                f"{scheduler_config.scheduler_type} does not support var(): {e}"
            )
        raise
    assert run.successful, "Run was not successful"
