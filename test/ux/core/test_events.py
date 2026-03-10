"""
Tests for Argo Events integration: @trigger, @trigger_on_finish, and ArgoEvent.

These tests exercise the argo-events devstack component (EventBus, EventSource,
Sensors) by deploying flows with event decorators to Argo Workflows and verifying
that:
  1. Deploying a @trigger flow creates a Sensor and WorkflowTemplate.
  2. Publishing an ArgoEvent through the webhook triggers the deployed flow and
     the event payload is correctly mapped to flow parameters.
  3. Deploying a @trigger_on_finish flow creates a Sensor.
  4. ArgoEvent.publish() successfully posts to the webhook endpoint.

These tests only run on the argo-kubernetes backend (they require argo-events).
"""

import json
import subprocess
import time
import uuid
from datetime import datetime

import pytest

from metaflow import Run, namespace

from .test_utils import deploy_flow_to_scheduler

pytestmark = pytest.mark.events

# Timeout constants (seconds)
SENSOR_READY_TIMEOUT = 60
SENSOR_POLL_INTERVAL = 5
EVENT_TRIGGERED_RUN_TIMEOUT = 600
EVENT_TRIGGERED_RUN_POLL_INTERVAL = 10
KUBECTL_TIMEOUT = 60


def _skip_unless_argo_events(scheduler_config):
    """Skip the test unless argo-events config is present."""
    if scheduler_config.scheduler_type != "argo-workflows":
        pytest.skip("argo-events tests require the argo-kubernetes backend")

    from metaflow.metaflow_config import ARGO_EVENTS_WEBHOOK_URL

    if not ARGO_EVENTS_WEBHOOK_URL:
        pytest.skip("METAFLOW_ARGO_EVENTS_WEBHOOK_URL not configured")


def _sensor_exists(name, namespace="default"):
    """Check if a sensor exists in the cluster via kubectl."""
    try:
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "sensor",
                name,
                "-n",
                namespace,
                "-o",
                "jsonpath={.metadata.name}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0 and result.stdout.strip() == name
    except Exception:
        return False


def _get_sensor_json(name, namespace="default"):
    """Get the full sensor JSON from the cluster."""
    try:
        result = subprocess.run(
            ["kubectl", "get", "sensor", name, "-n", namespace, "-o", "json"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception:
        pass
    return None


def _wait_for_sensor_pod_ready(sensor_name, timeout=120, namespace="default"):
    """Wait for the sensor pod to be Running and the eventbus connected."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            # Check sensor pod status
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    "sensor-name=%s" % sensor_name,
                    "-o",
                    "jsonpath={.items[0].status.phase}",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            pod_phase = result.stdout.strip()
            if pod_phase == "Running":
                print("Sensor pod for %s is Running" % sensor_name)
                # Also check eventbus pods
                eb_result = subprocess.run(
                    [
                        "kubectl",
                        "get",
                        "pods",
                        "-n",
                        namespace,
                        "-l",
                        "controller=eventbus-controller",
                        "-o",
                        "jsonpath={.items[*].status.phase}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                print("Eventbus pod phases: %s" % eb_result.stdout.strip())
                # Give a few seconds for the sensor to connect to eventbus
                time.sleep(10)
                return
            print(
                "Sensor pod for %s in phase %s, waiting..." % (sensor_name, pod_phase)
            )
        except Exception as e:
            print("Error checking sensor pod: %s" % e)
        time.sleep(SENSOR_POLL_INTERVAL)
    print(
        "WARNING: Sensor pod for %s did not become Running within %ds"
        % (sensor_name, timeout)
    )
    # Dump diagnostics
    try:
        diag = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace, "-o", "wide"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        print("All pods:\n%s" % diag.stdout)
        sensor_logs = subprocess.run(
            [
                "kubectl",
                "logs",
                "-n",
                namespace,
                "-l",
                "sensor-name=%s" % sensor_name,
                "--tail=50",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        print("Sensor logs:\n%s" % sensor_logs.stdout)
    except Exception:
        pass


def _wait_for_event_triggered_run(
    deployed_flow,
    flow_name,
    not_before,
    timeout=EVENT_TRIGGERED_RUN_TIMEOUT,
    polling_interval=EVENT_TRIGGERED_RUN_POLL_INTERVAL,
):
    """Wait for a run triggered by an Argo Events sensor (not a direct trigger).

    Polls for Argo Workflow objects matching the deployed flow's name prefix
    and waits for one to reach a Succeeded phase, then returns the
    corresponding Metaflow Run object.

    Parameters
    ----------
    deployed_flow : ArgoWorkflowsDeployedFlow
        The deployed flow object.
    flow_name : str
        The Metaflow flow name (e.g. "TriggerFlow") for constructing the Run
        pathspec.  Sensor-created workflows do not carry the metaflow/flow_name
        annotation, so we pass it explicitly.
    not_before : float
        Unix timestamp; only consider workflows created after this time.
    timeout : int
        Maximum time in seconds to wait.
    polling_interval : int
        Seconds between polling attempts.
    """
    wf_name_prefix = deployed_flow.name + "-"
    k8s_namespace = "default"
    deadline = time.time() + timeout

    while time.time() < deadline:
        # List workflows whose name starts with the deployed flow's prefix
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "workflows",
                    "-n",
                    k8s_namespace,
                    "-o",
                    "json",
                ],
                capture_output=True,
                text=True,
                timeout=KUBECTL_TIMEOUT,
            )
        except Exception:
            time.sleep(polling_interval)
            continue

        if result.returncode != 0:
            time.sleep(polling_interval)
            continue

        wf_list = json.loads(result.stdout)
        for wf in wf_list.get("items", []):
            wf_name = wf.get("metadata", {}).get("name", "")
            if not wf_name.startswith(wf_name_prefix):
                continue

            # Skip workflows created before our event was published
            creation_ts = wf.get("metadata", {}).get("creationTimestamp", "")
            if creation_ts:
                try:
                    created_at = datetime.fromisoformat(
                        creation_ts.replace("Z", "+00:00")
                    ).timestamp()
                    if created_at < not_before:
                        continue
                except (ValueError, TypeError):
                    pass

            # Check if the workflow has completed successfully
            phase = wf.get("status", {}).get("phase", "")
            if phase != "Succeeded":
                print(
                    "Found workflow %s in phase %s, waiting for Succeeded..."
                    % (wf_name, phase)
                )
                continue

            # Found a matching succeeded workflow - get the Metaflow run
            run_id = "argo-%s" % wf_name
            print("Found event-triggered workflow: %s (run_id: %s)" % (wf_name, run_id))
            namespace(None)
            run = Run("%s/%s" % (flow_name, run_id), _namespace_check=False)
            return run

        print("No event-triggered workflow found yet, waiting...")
        time.sleep(polling_interval)

    # Dump diagnostics before failing
    try:
        diag = subprocess.run(
            ["kubectl", "get", "workflows", "-n", k8s_namespace, "-o", "wide"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        print("All workflows:\n%s" % diag.stdout)
        # Check sensor logs for errors
        sensor_name = deployed_flow.name.replace(".", "-")
        sensor_logs = subprocess.run(
            [
                "kubectl",
                "logs",
                "-n",
                k8s_namespace,
                "-l",
                "sensor-name=%s" % sensor_name,
                "--tail=100",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        print("Sensor logs:\n%s" % sensor_logs.stdout)
        if sensor_logs.stderr:
            print("Sensor logs stderr:\n%s" % sensor_logs.stderr)
    except Exception:
        pass

    raise RuntimeError(
        "Timed out waiting for event-triggered workflow with prefix '%s'"
        % wf_name_prefix
    )


@pytest.mark.scheduler_only
def test_trigger_deploy_creates_sensor(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Deploy a @trigger flow and verify a Sensor is created in the cluster."""
    _skip_unless_argo_events(scheduler_config)

    test_tag = "test_trigger_sensor_%s" % str(uuid.uuid4())[:8]
    combined_tags = tag + [test_tag]

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="events/trigger_flow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags},
        scheduler_type=scheduler_config.scheduler_type,
    )

    # The sensor name is the workflow name with dots replaced by dashes
    sensor_name = deployed_flow.name.replace(".", "-")
    print("Checking for sensor: %s" % sensor_name)

    # Wait for sensor to appear (Argo Events controller needs to process it)
    deadline = time.time() + SENSOR_READY_TIMEOUT
    while time.time() < deadline:
        if _sensor_exists(sensor_name):
            break
        time.sleep(SENSOR_POLL_INTERVAL)

    assert _sensor_exists(sensor_name), (
        "Sensor '%s' was not created after deploying @trigger flow" % sensor_name
    )

    # Verify the sensor spec references the correct event
    sensor_json = _get_sensor_json(sensor_name)
    assert sensor_json is not None, "Could not fetch sensor JSON"

    # Check that the sensor has dependencies referencing our event source
    deps = sensor_json.get("spec", {}).get("dependencies", [])
    assert len(deps) > 0, "Sensor should have at least one event dependency"

    # Verify the dependency references the configured event source
    from metaflow.metaflow_config import ARGO_EVENTS_EVENT_SOURCE

    dep_event_sources = [d.get("eventSourceName") for d in deps]
    assert (
        ARGO_EVENTS_EVENT_SOURCE in dep_event_sources
    ), "Sensor dependency should reference event source '%s', got %s" % (
        ARGO_EVENTS_EVENT_SOURCE,
        dep_event_sources,
    )


@pytest.mark.scheduler_only
def test_trigger_event_triggers_run(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Deploy a @trigger flow, publish an ArgoEvent, verify the triggered run."""
    _skip_unless_argo_events(scheduler_config)

    test_tag = "test_trigger_run_%s" % str(uuid.uuid4())[:8]
    combined_tags = tag + [test_tag]

    # Deploy the @trigger flow
    deployed_flow = deploy_flow_to_scheduler(
        flow_name="events/trigger_flow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags},
        scheduler_type=scheduler_config.scheduler_type,
    )

    # Wait for the sensor to be ready before publishing the event
    sensor_name = deployed_flow.name.replace(".", "-")
    deadline = time.time() + SENSOR_READY_TIMEOUT
    while time.time() < deadline:
        if _sensor_exists(sensor_name):
            break
        time.sleep(SENSOR_POLL_INTERVAL)
    assert _sensor_exists(sensor_name), "Sensor not ready"

    # Wait for the sensor pod to become ready, not just the CRD
    _wait_for_sensor_pod_ready(sensor_name, timeout=120)

    # Record time before publishing so we can filter out stale workflows.
    # Subtract a small buffer for potential clock skew between CI and k8s.
    publish_time = time.time() - 30

    # Publish an event to trigger the flow
    from metaflow.plugins.argo.argo_events import ArgoEvent

    greeting_value = "hello from test %s" % test_tag
    event = ArgoEvent("test-event")
    event.add_to_payload("greeting", greeting_value)
    event_id = event.publish(force=True, ignore_errors=False)
    assert event_id is not None, "ArgoEvent.publish() returned None"
    print("Published event with id: %s" % event_id)

    # Wait for the sensor-triggered run to complete.
    # We must NOT use deployed_flow.trigger() here -- that would bypass the
    # sensor/event path and create a direct run without the event payload.
    run = _wait_for_event_triggered_run(
        deployed_flow, flow_name="TriggerFlow", not_before=publish_time
    )
    assert run.successful, "Triggered run was not successful"
    assert run["start"].task.data.message == (
        "TriggerFlow received: %s" % greeting_value
    ), "Event payload was not passed through to the flow parameter"


@pytest.mark.scheduler_only
def test_trigger_on_finish_creates_sensor(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    """Deploy a @trigger_on_finish flow and verify sensor creation."""
    _skip_unless_argo_events(scheduler_config)

    test_tag = "test_tof_sensor_%s" % str(uuid.uuid4())[:8]
    combined_tags = tag + [test_tag]

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="events/trigger_on_finish_flow.py",
        tl_args={"decospecs": decospecs, "env": compute_env},
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags},
        scheduler_type=scheduler_config.scheduler_type,
    )

    sensor_name = deployed_flow.name.replace(".", "-")
    print("Checking for sensor: %s" % sensor_name)

    deadline = time.time() + SENSOR_READY_TIMEOUT
    while time.time() < deadline:
        if _sensor_exists(sensor_name):
            break
        time.sleep(SENSOR_POLL_INTERVAL)

    assert _sensor_exists(sensor_name), (
        "Sensor '%s' was not created after deploying @trigger_on_finish flow"
        % sensor_name
    )

    # Verify sensor dependencies reference the correct event for HelloFlow completion
    sensor_json = _get_sensor_json(sensor_name)
    assert sensor_json is not None, "Could not fetch sensor JSON"
    deps = sensor_json.get("spec", {}).get("dependencies", [])
    assert len(deps) > 0, "Sensor should have at least one event dependency"


@pytest.mark.scheduler_only
def test_argo_event_publish(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Verify ArgoEvent.publish() successfully posts to the webhook endpoint."""
    _skip_unless_argo_events(scheduler_config)

    from metaflow.plugins.argo.argo_events import ArgoEvent

    event_name = "test-publish-%s" % str(uuid.uuid4())[:8]
    event = ArgoEvent(event_name)
    event.add_to_payload("key1", "value1")
    event.add_to_payload("key2", "value2")

    # publish() should succeed (webhook is accepting POST requests)
    event_id = event.publish(force=True, ignore_errors=False)
    assert event_id is not None, "Event publish should return an event ID"
    assert isinstance(event_id, str), "Event ID should be a string (UUID)"
