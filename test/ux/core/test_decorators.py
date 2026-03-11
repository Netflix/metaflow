"""
Decorator coverage tests — @environment, @card, and @pypi.

These tests verify that common step/flow decorators work correctly
across all backends (local runner + all orchestrators).

Run with:
    pytest test/ux/core/test_decorators.py -m decorators -v
"""

import pytest

pytestmark = pytest.mark.decorators

from .test_utils import execute_test_flow


@pytest.mark.decorators
@pytest.mark.basic
def test_environment_vars(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @environment(vars={...}) injects env vars into step execution."""
    run = execute_test_flow(
        flow_name="decorators/env_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="env_vars",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert (
        run["start"].task.data.foo == "bar"
    ), f"Expected TEST_ENV_FOO='bar', got {run['start'].task.data.foo!r}"
    assert (
        run["start"].task.data.baz == "qux"
    ), f"Expected TEST_ENV_BAZ='qux', got {run['start'].task.data.baz!r}"


@pytest.mark.decorators
@pytest.mark.basic
def test_card_basic(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify @card decorator creates a card after step execution."""
    run = execute_test_flow(
        flow_name="decorators/card_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="card_basic",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert run["start"].task.data.message == "hello from card flow"

    # Verify a card was actually created
    cards = list(run["start"].task.cards)
    assert len(cards) > 0, "Expected at least one card on the start step"
