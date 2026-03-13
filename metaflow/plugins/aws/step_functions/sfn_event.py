import json
import os
import sys
import time
import uuid
from datetime import datetime

from metaflow.exception import MetaflowException


class SFNEventException(MetaflowException):
    headline = "SFN Event Exception"


class SFNEvent(object):
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

    @classmethod
    def is_configured(cls):
        return bool(
            os.environ.get("METAFLOW_SFN_IAM_ROLE")
            or os.environ.get("METAFLOW_SFN_STATE_MACHINE_PREFIX")
        )

    def __init__(self, name, payload=None):
        self._name = name
        self._payload = payload or {}

    def add_to_payload(self, key, value):
        """Add a key-value pair to the event payload."""
        self._payload[key] = str(value)
        return self

    def publish(self, payload=None, ignore_errors=True):
        """
        Publish an event to EventBridge that will trigger any Step Functions
        workflow listening for this event name.

        Parameters
        ----------
        payload : dict, optional
            Additional key-value pairs to merge into the payload.
        ignore_errors : bool, default True
            If True, errors are silently ignored.

        Returns
        -------
        str or None
            The event ID if published successfully, None otherwise.
        """
        if payload is None:
            payload = {}

        try:
            import boto3

            event_bus = os.environ.get("METAFLOW_SFN_EVENT_BUS_ARN", "default")

            event_id = str(uuid.uuid4())
            event_payload = {
                "name": self._name,
                "id": event_id,
                "timestamp": int(time.time()),
                "utc_date": datetime.utcnow().strftime("%Y%m%d"),
                "generated-by-metaflow": True,
                **self._payload,
                **payload,
            }

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

            print("SFN Event (%s) published." % self._name, file=sys.stderr)
            return event_id

        except Exception as e:
            msg = "Unable to publish SFN Event (%s): %s" % (self._name, e)
            if ignore_errors:
                print(msg, file=sys.stderr)
                return None
            else:
                raise SFNEventException(msg)
