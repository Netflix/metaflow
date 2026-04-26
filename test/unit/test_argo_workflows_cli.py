import types

import pytest

from metaflow.plugins.argo.argo_workflows_cli import sanitize_for_argo
from metaflow.plugins.argo.argo_workflows import ArgoWorkflows


@pytest.mark.parametrize(
    "name, expected",
    [
        ("a-valid-name", "a-valid-name"),
        ("removing---@+_characters@_+", "removing---characters"),
        ("numb3rs-4r3-0k-123", "numb3rs-4r3-0k-123"),
        ("proj3ct.br4nch.flow_name", "proj3ct.br4nch.flowname"),
        # should not break RFC 1123 subdomain requirements,
        # though trailing characters do not need to be sanitized due to a hash being appended to them.
        (
            "---1breaking1---.--2subdomain2--.-3rules3----",
            "1breaking1.2subdomain2.3rules3----",
        ),
        (
            "1brea---king1.2sub---domain2.-3ru-les3--",
            "1brea---king1.2sub---domain2.3ru-les3--",
        ),
        ("project.branch-cut-short-.flowname", "project.branch-cut-short.flowname"),
        ("test...name", "test.name"),
    ],
)
def test_sanitize_for_argo(name, expected):
    sanitized = sanitize_for_argo(name)
    assert sanitized == expected


@pytest.fixture
def make_argo_with_schedule():
    """
    Factory fixture: returns a callable that builds a minimal ArgoWorkflows-like
    object whose _get_schedule() can be called without instantiating the full
    class (which requires a live graph, environment, datastore, etc.).

    The method only accesses self.flow._flow_decorators, so we build the
    smallest possible stand-in for each layer.
    """

    def _make(schedule_value, timezone_value=None):
        decorator = types.SimpleNamespace(
            schedule=schedule_value, timezone=timezone_value
        )
        flow = types.SimpleNamespace(_flow_decorators={"schedule": [decorator]})
        instance = object.__new__(ArgoWorkflows)
        instance.flow = flow
        return instance

    return _make


@pytest.fixture
def argo_without_schedule():
    """ArgoWorkflows stand-in with no @schedule decorator at all."""
    flow = types.SimpleNamespace(_flow_decorators={})
    instance = object.__new__(ArgoWorkflows)
    instance.flow = flow
    return instance


def test_get_schedule_no_decorator_returns_none(argo_without_schedule):
    """No @schedule decorator → (None, None)."""
    assert argo_without_schedule._get_schedule() == (None, None)


@pytest.mark.parametrize(
    "schedule_value, timezone_value, expected",
    [
        # schedule resolved to None → (None, None) regardless of timezone.
        # The schedule_none case is the regression test for the AttributeError
        # bug where None.split() was called when all scheduling flags were falsy.
        (None, None, (None, None)),
        (None, "America/Los_Angeles", (None, None)),
        # 6-field quartz cron → trimmed to 5 fields, timezone passed through.
        ("0 0 * * ? *", None, ("0 0 * * ?", None)),
        ("0 0 * * ? *", "Europe/Berlin", ("0 0 * * ?", "Europe/Berlin")),
    ],
    ids=[
        "schedule_none",
        "schedule_none_with_timezone",
        "cron_expression",
        "cron_expression_with_timezone",
    ],
)
def test_get_schedule(
    make_argo_with_schedule, schedule_value, timezone_value, expected
):
    argo = make_argo_with_schedule(
        schedule_value=schedule_value, timezone_value=timezone_value
    )
    assert argo._get_schedule() == expected


def test_trigger_explanation_no_schedule_does_not_claim_cronworkflow(
    argo_without_schedule,
):
    """With no schedule, trigger_explanation() must not mention CronWorkflow."""
    argo_without_schedule._schedule = None
    argo_without_schedule.triggers = []
    result = argo_without_schedule.trigger_explanation()
    assert "CronWorkflow" not in result


def test_trigger_explanation_schedule_none_does_not_claim_cronworkflow(
    make_argo_with_schedule,
):
    """
    When @schedule is present but resolved to None, trigger_explanation()
    must not claim the workflow triggers via a CronWorkflow.
    """
    argo = make_argo_with_schedule(schedule_value=None)
    argo._schedule = None  # mirrors what _get_schedule() would set
    argo.triggers = []
    result = argo.trigger_explanation()
    assert "CronWorkflow" not in result


def test_trigger_explanation_active_schedule_claims_cronworkflow(
    make_argo_with_schedule,
):
    """When a real schedule is set, trigger_explanation() names the CronWorkflow."""
    argo = make_argo_with_schedule(schedule_value="0 0 * * ? *")
    argo._schedule = "0 0 * * ?"
    argo.name = "myflow"
    result = argo.trigger_explanation()
    assert result is not None
    assert "CronWorkflow" in result
    assert "myflow" in result
