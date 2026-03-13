import json
import os

from metaflow.event_provider import MetaflowEvent, MetaflowEventException


class SFNEventException(MetaflowEventException):
    headline = "SFN Event Exception"


class SFNEvent(MetaflowEvent):
    """
    SFNEvent sends a trigger event via AWS EventBridge to start Step Functions
    workflows deployed with @trigger.

    Parameters
    ----------
    name : str
        Event name (must match the @trigger event name on the deployed flow).
    payload : dict, optional
        Key-value pairs delivered with the event, used to set parameters of
        triggered flows.
    """

    TYPE = "step-functions"
    LABEL = "SFN Event"

    @classmethod
    def is_configured(cls):
        return bool(
            os.environ.get("METAFLOW_SFN_IAM_ROLE")
            or os.environ.get("METAFLOW_SFN_STATE_MACHINE_PREFIX")
        )

    def _do_publish(self, payload):
        import boto3

        event_bus = os.environ.get("METAFLOW_SFN_EVENT_BUS_ARN", "default")
        event_payload = self._build_payload(payload)
        event_id = event_payload["id"]

        client = boto3.client("events")
        response = client.put_events(
            Entries=[
                {
                    "Source": "metaflow",
                    "DetailType": self._name,
                    "Detail": json.dumps(event_payload),
                    "EventBusName": event_bus,
                }
            ]
        )

        if response.get("FailedEntryCount", 0) > 0:
            err = response["Entries"][0].get("ErrorMessage", "Unknown error")
            raise SFNEventException(
                "Failed to publish event %s: %s" % (self._name, err)
            )

        return event_id

    def _make_exception(self, msg):
        return SFNEventException(msg)
