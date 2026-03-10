"""
Tests for `argo-workflows create --only-json` manifest export.

Verifies that export_all_json() outputs all Argo manifests (workflow template,
cron workflow, and sensor) as a single JSON object, not just the workflow
template.

These tests exercise the ArgoWorkflows.export_all_json() method directly,
bypassing the CLI and cluster access requirements.
"""

import json
from unittest.mock import MagicMock

import pytest

from metaflow.plugins.argo.argo_workflows import (
    ArgoWorkflows,
    Sensor,
    WorkflowTemplate,
)


def _make_argo_workflows(schedule=None, timezone=None, sensor=None):
    """Create an ArgoWorkflows instance with pre-set internal state via mocking.

    We bypass __init__ to avoid the many dependencies (graph, flow, datastore, etc.)
    and directly set the three internal attributes that export_all_json() reads.
    """
    obj = object.__new__(ArgoWorkflows)

    # Build a minimal WorkflowTemplate
    wt = WorkflowTemplate()
    wt.payload = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "test-flow"},
        "spec": {},
    }
    obj._workflow_template = wt
    obj.name = "test-flow"
    obj._schedule = schedule
    obj._timezone = timezone
    obj._sensor = sensor

    return obj


class TestExportAllJsonScheduled:
    """A flow with @schedule should export both workflow_template and cron_workflow."""

    def test_contains_workflow_template(self):
        argo = _make_argo_workflows(schedule="0 10 * * *", timezone="US/Pacific")
        data = json.loads(argo.export_all_json())
        assert "workflow_template" in data
        assert data["workflow_template"]["kind"] == "WorkflowTemplate"

    def test_contains_cron_workflow(self):
        argo = _make_argo_workflows(schedule="0 10 * * *", timezone="US/Pacific")
        data = json.loads(argo.export_all_json())
        assert "cron_workflow" in data
        cron = data["cron_workflow"]
        assert cron["kind"] == "CronWorkflow"
        assert cron["apiVersion"] == "argoproj.io/v1alpha1"
        assert cron["spec"]["schedule"] == "0 10 * * *"
        assert cron["spec"]["timezone"] == "US/Pacific"
        assert cron["spec"]["suspend"] is False
        assert cron["metadata"]["name"] == "test-flow"
        assert (
            cron["spec"]["workflowSpec"]["workflowTemplateRef"]["name"] == "test-flow"
        )

    def test_no_sensor_without_trigger(self):
        argo = _make_argo_workflows(schedule="0 10 * * *", timezone="US/Pacific")
        data = json.loads(argo.export_all_json())
        assert "sensor" not in data


class TestExportAllJsonTriggered:
    """A flow with @trigger should export both workflow_template and sensor."""

    def _make_sensor(self):
        sensor = Sensor()
        sensor.payload = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Sensor",
            "metadata": {"name": "test-flow"},
            "spec": {"dependencies": [], "triggers": []},
        }
        return sensor

    def test_contains_workflow_template(self):
        argo = _make_argo_workflows(sensor=self._make_sensor())
        data = json.loads(argo.export_all_json())
        assert "workflow_template" in data
        assert data["workflow_template"]["kind"] == "WorkflowTemplate"

    def test_contains_sensor(self):
        argo = _make_argo_workflows(sensor=self._make_sensor())
        data = json.loads(argo.export_all_json())
        assert "sensor" in data
        assert data["sensor"]["kind"] == "Sensor"

    def test_no_cron_without_schedule(self):
        argo = _make_argo_workflows(sensor=self._make_sensor())
        data = json.loads(argo.export_all_json())
        assert "cron_workflow" not in data


class TestExportAllJsonPlain:
    """A plain flow (no @schedule, no @trigger) exports only the workflow template."""

    def test_only_workflow_template(self):
        argo = _make_argo_workflows()
        data = json.loads(argo.export_all_json())
        assert "workflow_template" in data
        assert "cron_workflow" not in data
        assert "sensor" not in data

    def test_output_is_valid_json(self):
        argo = _make_argo_workflows()
        raw = argo.export_all_json()
        # Should parse without error
        data = json.loads(raw)
        assert isinstance(data, dict)


class TestExportAllJsonBothScheduleAndTrigger:
    """A flow with both @schedule and @trigger exports all three manifests."""

    def _make_sensor(self):
        sensor = Sensor()
        sensor.payload = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Sensor",
            "metadata": {"name": "test-flow"},
            "spec": {"dependencies": [], "triggers": []},
        }
        return sensor

    def test_all_three_present(self):
        argo = _make_argo_workflows(
            schedule="*/5 * * * *",
            timezone="UTC",
            sensor=self._make_sensor(),
        )
        data = json.loads(argo.export_all_json())
        assert "workflow_template" in data
        assert "cron_workflow" in data
        assert "sensor" in data

    def test_cron_schedule_value(self):
        argo = _make_argo_workflows(
            schedule="*/5 * * * *",
            timezone="UTC",
            sensor=self._make_sensor(),
        )
        data = json.loads(argo.export_all_json())
        assert data["cron_workflow"]["spec"]["schedule"] == "*/5 * * * *"
        assert data["cron_workflow"]["spec"]["timezone"] == "UTC"


class TestCronWorkflowJson:
    """Tests for the _cron_workflow_json() helper method."""

    def test_returns_none_when_no_schedule(self):
        argo = _make_argo_workflows()
        assert argo._cron_workflow_json() is None

    def test_returns_dict_when_schedule_set(self):
        argo = _make_argo_workflows(schedule="0 0 * * *", timezone="UTC")
        cron = argo._cron_workflow_json()
        assert isinstance(cron, dict)
        assert cron["kind"] == "CronWorkflow"

    def test_cron_spec_matches_argo_client(self):
        """Verify the cron body matches what ArgoClient.schedule_workflow_template builds."""
        argo = _make_argo_workflows(schedule="0 10 * * *", timezone="US/Pacific")
        cron = argo._cron_workflow_json()
        assert cron["spec"]["failedJobsHistoryLimit"] == 10000
        assert cron["spec"]["successfulJobsHistoryLimit"] == 10000
        assert cron["spec"]["startingDeadlineSeconds"] == 3540
        assert cron["spec"]["workflowSpec"] == {
            "workflowTemplateRef": {"name": "test-flow"}
        }
