"""
Tests for `argo-workflows create --only-json` manifest export.

Verifies that the CLI outputs all Argo manifests (workflow template,
cron workflow, and sensor) as a single JSON object, not just the workflow
template.

Includes both:
- Unit tests exercising ArgoWorkflows.export_all_json() directly
- Integration tests running the actual CLI via subprocess against the devstack
"""

import json
import os
import subprocess
import sys

import pytest

from metaflow.plugins.argo.argo_workflows import (
    ArgoWorkflows,
    Sensor,
    WorkflowTemplate,
)

pytestmark = [pytest.mark.argo_manifests]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_argo_workflows(schedule=None, timezone=None, sensor=None):
    """Create an ArgoWorkflows instance with pre-set internal state via mocking.

    We bypass __init__ to avoid the many dependencies (graph, flow, datastore, etc.)
    and directly set the three internal attributes that export_all_json() reads.
    """
    obj = object.__new__(ArgoWorkflows)

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


def _get_compile_env():
    """Get environment variables for compilation-only tests.

    Overrides metadata to 'local' so no metadata service is needed, and
    ensures the source tree is on PYTHONPATH so the subprocess can import
    metaflow from the repo.
    """
    env = os.environ.copy()
    env["METAFLOW_DEFAULT_METADATA"] = "local"
    # Ensure the repo root is on PYTHONPATH for subprocess imports.
    repo_root = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")
    # Keep METAFLOW_HOME/METAFLOW_PROFILE if set (e.g. devstack config that
    # provides S3/cloud datastore settings needed by --only-json).
    return env


def _compile_flow_to_json(flow_path, **extra_tl_args):
    """Compile a flow to Argo JSON using the CLI with --only-json.

    Runs `python <flow> --no-pylint argo-workflows create --only-json`
    and parses the JSON from stdout.
    """
    from .test_utils import _resolve_flow_path

    full_path = _resolve_flow_path(flow_path)

    cmd = [sys.executable, full_path, "--no-pylint"]
    for k, v in extra_tl_args.items():
        if v is not None:
            cmd.extend([f"--{k.replace('_', '-')}", str(v)])
    cmd.extend(["argo-workflows", "create", "--only-json"])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=_get_compile_env(),
    )
    if result.returncode != 0:
        stderr = result.stderr or ""
        stdout = result.stdout or ""
        if "No such command" in stderr or "No such command" in stdout:
            pytest.skip(
                "argo-workflows CLI not available (extension may override plugins)"
            )
        if "ConnectionRefusedError" in stderr or "ConnectionError" in stderr:
            pytest.skip(
                "Argo backend not configured (connection refused to metadata service)"
            )
        if "requires --datastore=" in stderr:
            pytest.skip(
                "Cloud datastore not configured (--only-json requires s3/azure/gs)"
            )
        pytest.fail(f"Compilation failed:\nstderr: {stderr}\nstdout: {stdout}")

    stdout = result.stdout.strip()
    json_start = stdout.find("{")
    if json_start == -1:
        pytest.fail(f"No JSON found in compilation output:\n{stdout}")

    return json.loads(stdout[json_start:])


# ---------------------------------------------------------------------------
# Unit tests — exercise export_all_json() directly with mock objects
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Integration tests — run the actual CLI via subprocess
# ---------------------------------------------------------------------------


class TestArgoCliOnlyJsonPlainFlow:
    """Run `argo-workflows create --only-json` on a plain flow via subprocess."""

    def test_plain_flow_has_workflow_template(self):
        data = _compile_flow_to_json("basic/helloworld.py")
        assert "workflow_template" in data
        assert data["workflow_template"]["kind"] == "WorkflowTemplate"

    def test_plain_flow_no_cron_or_sensor(self):
        data = _compile_flow_to_json("basic/helloworld.py")
        assert "cron_workflow" not in data
        assert "sensor" not in data

    def test_output_is_valid_json_object(self):
        data = _compile_flow_to_json("basic/helloworld.py")
        assert isinstance(data, dict)


class TestArgoCliOnlyJsonScheduledFlow:
    """Run `argo-workflows create --only-json` on a @schedule flow via subprocess."""

    def test_scheduled_flow_has_cron_workflow(self):
        data = _compile_flow_to_json("lifecycle/schedule_flow.py")
        assert "cron_workflow" in data
        cron = data["cron_workflow"]
        assert cron["kind"] == "CronWorkflow"
        assert cron["apiVersion"] == "argoproj.io/v1alpha1"

    def test_scheduled_flow_has_workflow_template(self):
        data = _compile_flow_to_json("lifecycle/schedule_flow.py")
        assert "workflow_template" in data
        assert data["workflow_template"]["kind"] == "WorkflowTemplate"

    def test_cron_schedule_matches_decorator(self):
        data = _compile_flow_to_json("lifecycle/schedule_flow.py")
        cron = data["cron_workflow"]
        assert cron["spec"]["schedule"] == "0 0 * * *"

    def test_cron_references_workflow_template(self):
        data = _compile_flow_to_json("lifecycle/schedule_flow.py")
        cron = data["cron_workflow"]
        wt_name = data["workflow_template"]["metadata"]["name"]
        assert cron["spec"]["workflowSpec"]["workflowTemplateRef"]["name"] == wt_name
