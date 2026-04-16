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


def _make_argo_with_schedule(schedule_value, timezone_value=None):
    """
    Return a minimal ArgoWorkflows-like object whose _get_schedule() can be
    called without instantiating the full class (which requires a live graph,
    environment, datastore, etc.).

    The method only accesses self.flow._flow_decorators, so we build the
    smallest possible stand-in for each layer.
    """
    decorator = types.SimpleNamespace(schedule=schedule_value, timezone=timezone_value)
    flow = types.SimpleNamespace(_flow_decorators={"schedule": [decorator]})
    instance = object.__new__(ArgoWorkflows)
    instance.flow = flow
    return instance


def _make_argo_without_schedule():
    """Return an ArgoWorkflows stand-in with no @schedule decorator at all."""
    flow = types.SimpleNamespace(_flow_decorators={})
    instance = object.__new__(ArgoWorkflows)
    instance.flow = flow
    return instance


class TestGetSchedule:
    def test_no_decorator_returns_none(self):
        """No @schedule decorator → (None, None)."""
        argo = _make_argo_without_schedule()
        assert argo._get_schedule() == (None, None)

    def test_schedule_none_returns_none(self):
        """
        @schedule decorator present but schedule resolves to None
        (e.g. @schedule(daily=False)) → (None, None), no crash.

        This is the regression test for the AttributeError bug where
        None.split() was called when all scheduling flags were falsy.
        """
        argo = _make_argo_with_schedule(schedule_value=None)
        assert argo._get_schedule() == (None, None)

    def test_schedule_none_with_timezone_returns_none(self):
        """timezone is irrelevant when schedule itself is None."""
        argo = _make_argo_with_schedule(
            schedule_value=None, timezone_value="America/Los_Angeles"
        )
        assert argo._get_schedule() == (None, None)

    def test_cron_expression_is_returned(self):
        """A valid 6-field quartz cron expression is trimmed to 5 fields."""
        argo = _make_argo_with_schedule(schedule_value="0 0 * * ? *")
        schedule, tz = argo._get_schedule()
        assert schedule == "0 0 * * ?"
        assert tz is None

    def test_cron_expression_with_timezone(self):
        """Timezone is passed through unchanged alongside the cron string."""
        argo = _make_argo_with_schedule(
            schedule_value="0 0 * * ? *", timezone_value="Europe/Berlin"
        )
        schedule, tz = argo._get_schedule()
        assert schedule == "0 0 * * ?"
        assert tz == "Europe/Berlin"


class TestTriggerExplanation:
    def test_no_schedule_does_not_claim_cronworkflow(self):
        """With no schedule, trigger_explanation() must not mention CronWorkflow."""
        argo = _make_argo_without_schedule()
        argo._schedule = None
        argo.triggers = []
        result = argo.trigger_explanation()
        assert "CronWorkflow" not in result

    def test_schedule_none_does_not_claim_cronworkflow(self):
        """
        When @schedule is present but resolved to None, trigger_explanation()
        must not claim the workflow triggers via a CronWorkflow.
        """
        argo = _make_argo_with_schedule(schedule_value=None)
        argo._schedule = None  # mirrors what _get_schedule() would set
        argo.triggers = []
        result = argo.trigger_explanation()
        assert "CronWorkflow" not in result

    def test_active_schedule_claims_cronworkflow(self):
        """When a real schedule is set, trigger_explanation() names the CronWorkflow."""
        argo = _make_argo_with_schedule(schedule_value="0 0 * * ? *")
        argo._schedule = "0 0 * * ?"
        argo.name = "myflow"
        result = argo.trigger_explanation()
        assert result is not None
        assert "CronWorkflow" in result
        assert "myflow" in result
