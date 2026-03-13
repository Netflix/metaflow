import sys
import time
import uuid
from datetime import datetime

from metaflow.exception import MetaflowException


class MetaflowEventException(MetaflowException):
    headline = "Metaflow Event Exception"


class MetaflowEvent(object):
    """Base class for event providers that publish trigger events.

    Subclasses must set ``TYPE`` (str) and implement ``is_configured()``
    and ``_do_publish(payload)``.
    """

    TYPE = None  # e.g. "argo-workflows", "step-functions", "airflow"
    LABEL = "Event"  # Human-readable label for log messages

    @classmethod
    def is_configured(cls):
        """Return True if this provider's required env vars / config are set."""
        return False

    def __init__(self, name, payload=None, **kwargs):
        self._name = name
        self._payload = payload or {}

    def add_to_payload(self, key, value):
        """Add a key-value pair to the event payload.

        Parameters
        ----------
        key : str
            Key
        value : str
            Value
        """
        self._payload[key] = str(value)
        return self

    def _build_payload(self, extra_payload=None):
        """Build the standard event payload dict.

        All providers share this envelope: name, id, timestamp, utc_date,
        generated-by-metaflow flag, plus user-supplied fields.
        """
        return {
            "name": self._name,
            "id": str(uuid.uuid4()),
            "timestamp": int(time.time()),
            "utc_date": datetime.utcnow().strftime("%Y%m%d"),
            "generated-by-metaflow": True,
            **self._payload,
            **(extra_payload or {}),
        }

    def _do_publish(self, payload):
        """Transport-specific publish logic.

        Subclasses implement this to send the event over their backend
        (webhook, EventBridge, REST API, etc.).

        Parameters
        ----------
        payload : dict
            Extra payload to merge (already handled by _build_payload).

        Returns
        -------
        str
            Event/run ID on success.

        Raises
        ------
        Exception
            On failure (caller wraps with ignore_errors logic).
        """
        raise NotImplementedError

    def _make_exception(self, msg):
        """Return the provider-specific exception type."""
        return MetaflowEventException(msg)

    def publish(self, payload=None, ignore_errors=True):
        """Publish an event with standard error handling.

        Parameters
        ----------
        payload : dict, optional
            Additional key-value pairs to merge into the payload.
        ignore_errors : bool, default True
            If True, errors are silently ignored.

        Returns
        -------
        str or None
            Event ID if published successfully, None on ignored error.
        """
        if payload is None:
            payload = {}
        try:
            event_id = self._do_publish(payload)
            print(
                "%s (%s) published." % (self.LABEL, self._name),
                file=sys.stderr,
            )
            return event_id
        except Exception as e:
            msg = "Unable to publish %s (%s): %s" % (self.LABEL, self._name, e)
            if ignore_errors:
                print(msg, file=sys.stderr)
                return None
            else:
                raise self._make_exception(msg)
