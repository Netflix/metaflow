"""
Decorator coverage tests — @environment, @card, and @pypi.

These tests verify that common step/flow decorators work correctly
across all backends (local runner + all orchestrators).

Run with:
    pytest test/ux/core/test_decorators.py -m decorators -v
"""

from typing import Any, Callable, Dict, List

import pytest
from metaflow import Run

from .test_utils import execute_test_flow

# Apply markers to all tests in this module
pytestmark = [pytest.mark.decorators, pytest.mark.basic]


# ---------------------------------------------------------------------------
# Assertion Callbacks
# ---------------------------------------------------------------------------


def _assert_env_vars(run: Run):
    """Validate @environment(vars={...}) standard injection."""
    assert run.successful, "Run was not successful"
    assert (
        run["start"].task.data.foo == "bar"
    ), f"Expected TEST_ENV_FOO='bar', got {run['start'].task.data.foo!r}"
    assert (
        run["start"].task.data.baz == "qux"
    ), f"Expected TEST_ENV_BAZ='qux', got {run['start'].task.data.baz!r}"


def _assert_env_vars_foreach(run: Run):
    """Validate @environment(vars={...}) injection in a foreach body."""
    assert run.successful, "Run was not successful"
    # Every foreach body task must have received the injected env var.
    assert all(
        v == "injected" for v in run["join"].task.data.env_vals
    ), f"@environment var not injected into foreach body: {run['join'].task.data.env_vals!r}"


def _assert_card_basic(run: Run):
    """Validate @card decorator generates a card."""
    assert run.successful, "Run was not successful"
    assert run["start"].task.data.message == "hello from card flow"

    # Verify a card was actually created using the card client API
    from metaflow.cards import get_cards

    cards = get_cards(run["start"].task)
    assert len(cards) > 0, "Expected at least one card on the start step"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flow_name, test_name, assertion_fn",
    [
        pytest.param(
            "decorators/env_flow.py",
            "env_vars",
            _assert_env_vars,
            id="environment_vars",
        ),
        pytest.param(
            "decorators/env_foreach_flow.py",
            "env_vars_foreach",
            _assert_env_vars_foreach,
            id="environment_vars_foreach",
        ),
        pytest.param(
            "decorators/card_flow.py", "card_basic", _assert_card_basic, id="card_basic"
        ),
    ],
)
def test_decorator_behaviors(
    exec_mode: str,
    decospecs: Any,
    compute_env: Dict[str, str],
    tag: List[str],
    scheduler_config: Any,
    flow_name: str,
    test_name: str,
    assertion_fn: Callable[[Run], None],
):
    """Verify various decorators function properly across all execution modes."""
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
