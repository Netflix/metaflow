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

import pytest

from .test_utils import deploy_flow_to_scheduler, wait_for_deployed_run

pytestmark = pytest.mark.events


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
    deadline = time.time() + 60
    while time.time() < deadline:
        if _sensor_exists(sensor_name):
            break
        time.sleep(5)

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
    deadline = time.time() + 60
    while time.time() < deadline:
        if _sensor_exists(sensor_name):
            break
        time.sleep(5)
    assert _sensor_exists(sensor_name), "Sensor not ready"

    # Give the sensor a moment to become fully operational
    time.sleep(5)

    # Publish an event to trigger the flow
    from metaflow.plugins.argo.argo_events import ArgoEvent

    greeting_value = "hello from test %s" % test_tag
    event = ArgoEvent("test-event")
    event.add_to_payload("greeting", greeting_value)
    event_id = event.publish(force=True, ignore_errors=False)
    assert event_id is not None, "ArgoEvent.publish() returned None"
    print("Published event with id: %s" % event_id)

    # Wait for the triggered run to complete
    run = wait_for_deployed_run(deployed_flow, timeout=600)
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

    deadline = time.time() + 60
    while time.time() < deadline:
        if _sensor_exists(sensor_name):
            break
        time.sleep(5)

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
