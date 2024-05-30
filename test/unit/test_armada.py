from unittest.mock import patch

import pytest

# TODO: Skip these tests if armada_client isn't present and print a warning.
from armada_client.event import EventType
from armada_client.typings import JobSubmittedEvent, JobSucceededEvent

from metaflow.plugins.armada.armada import create_armada_pod_spec, wait_for_job_finish


def test_armada_create_armada_pod_spec():
    pod_spec = create_armada_pod_spec(["sleep 10"], {"test": "value"}, [])
    assert pod_spec is not None
    assert pod_spec[0].pod_spec.containers[0].args == ["sleep 10"]
    assert pod_spec[0].pod_spec.containers[0].env[0].name == "test"
    assert pod_spec[0].pod_spec.containers[0].env[0].value == "value"
    print(len(pod_spec))
    print(pod_spec)
    # assert False


class MockEvent(object):
    def __init__(self, msg_type, message, event_id):
        self.type = msg_type
        self.message = message
        self.id = event_id


class MockMessage(object):
    def __init__(self, job_id):
        self.job_id = job_id


@patch("metaflow.plugins.armada.armada._get_client")
def test_wait_for_job_finish(get_client):
    job_id = "fake_job_id"
    job_set_id = "fake_job_set_id"
    get_client.return_value.get_job_events_stream.return_value = list(
        ["event1", "event2", "event3", "event4"]
    )
    unmarshalled_events = [
        MockEvent(EventType.submitted, MockMessage(job_id), "1"),
        MockEvent(EventType.queued, MockMessage(job_id), "2"),
        MockEvent(EventType.running, MockMessage(job_id), "3"),
        MockEvent(EventType.succeeded, MockMessage(job_id), "4"),
    ]
    get_client.return_value.unmarshal_event_response.side_effect = unmarshalled_events

    last_event = wait_for_job_finish(
        "localhost", "1337", "test", job_set_id, job_id, False
    )

    assert get_client.called
    get_client.assert_called_with("localhost", "1337", False)
    assert get_client.return_value.get_job_events_stream.called
    get_client.return_value.get_job_events_stream.assert_called_with("test", job_set_id)
    assert get_client.return_value.unmarshal_event_response.called
    assert last_event == unmarshalled_events[-1]
