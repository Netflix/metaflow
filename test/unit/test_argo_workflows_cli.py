import types

import pytest

from metaflow.plugins.argo.argo_workflows_cli import sanitize_for_argo
from metaflow.plugins.argo.argo_workflows import ArgoWorkflows
from metaflow.plugins.argo.argo_workflows_deployer_objects import (
    ArgoWorkflowsDeployedFlow,
)

# ---------------------------------------------------------------------------
# Shared Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def make_argo_with_schedule():
    """Factory fixture: returns a callable that builds a minimal ArgoWorkflows-like

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


# ---------------------------------------------------------------------------
# Argo Sanitization and Scheduling Tests
# ---------------------------------------------------------------------------


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
    ids=[
        "valid-string",
        "strip-special-chars",
        "alphanumeric-with-dashes",
        "strip-subdomain-underscores",
        "rfc1123-subdomain-edge-dashes",
        "rfc1123-subdomain-internal-dashes",
        "strip-trailing-dash-before-dot",
        "collapse-consecutive-dots",
    ],
)
def test_sanitize_for_argo(name, expected):
    """Verify string processing safely conforms to Argo resource naming limitations."""
    assert sanitize_for_argo(name) == expected


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
    """Verify cron parsing extraction, payload truncation, and timezone options."""
    argo = make_argo_with_schedule(
        schedule_value=schedule_value, timezone_value=timezone_value
    )
    assert argo._get_schedule() == expected


@pytest.mark.parametrize(
    "has_decorator, schedule_value, internal_schedule, flow_name, expected_substrings, unexpected_substrings",
    [
        (False, None, None, None, [], ["CronWorkflow"]),
        (True, None, None, None, [], ["CronWorkflow"]),
        (
            True,
            "0 0 * * ? *",
            "0 0 * * ?",
            "myflow",
            ["CronWorkflow", "myflow"],
            [],
        ),
    ],
    ids=[
        "no-schedule-decorator",
        "schedule-decorator-resolves-to-none",
        "active-schedule-claims-cronworkflow",
    ],
)
def test_trigger_explanation_behavior(
    make_argo_with_schedule,
    argo_without_schedule,
    has_decorator,
    schedule_value,
    internal_schedule,
    flow_name,
    expected_substrings,
    unexpected_substrings,
):
    """Verify if trigger explanations correctly list or omit CronWorkflow rules based on state."""
    argo = (
        make_argo_with_schedule(schedule_value=schedule_value)
        if has_decorator
        else argo_without_schedule
    )

    argo._schedule = internal_schedule
    argo.triggers = []
    if flow_name:
        argo.name = flow_name

    result = argo.trigger_explanation()

    for substring in expected_substrings:
        assert substring in result
    for substring in unexpected_substrings:
        assert substring not in result


# ---------------------------------------------------------------------------
# Deployed Flow Object Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "additional_info, expected_template",
    [
        (
            {
                "workflow_template": {
                    "kind": "WorkflowTemplate",
                    "metadata": {"name": "myflow"},
                }
            },
            {"kind": "WorkflowTemplate", "metadata": {"name": "myflow"}},
        ),
        (None, None),
    ],
    ids=["with-json-payload", "without-payload"],
)
def test_deployed_flow_workflow_template_resolution(additional_info, expected_template):
    """Verify workflow template extraction handles missing or present payloads cleanly."""
    fields = {
        "name": "myflow",
        "flow_name": "MyFlow",
        "metadata": "local@user:test",
    }
    if additional_info is not None:
        fields["additional_info"] = additional_info

    deployer = types.SimpleNamespace(**fields)
    deployed_flow = ArgoWorkflowsDeployedFlow(deployer)

    assert deployed_flow.workflow_template == expected_template
