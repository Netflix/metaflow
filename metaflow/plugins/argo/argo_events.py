import json
import os
import sys
import time
import urllib
import uuid

from metaflow.event_provider import MetaflowEvent, MetaflowEventException
from metaflow.metaflow_config import (
    ARGO_EVENTS_WEBHOOK_AUTH,
    ARGO_EVENTS_WEBHOOK_URL,
    SERVICE_HEADERS,
    SERVICE_RETRY_COUNT,
)


class ArgoEventException(MetaflowEventException):
    headline = "Argo Event Exception"


class ArgoEvent(MetaflowEvent):
    """
    ArgoEvent is a small event, a message, that can be published to Argo Workflows. The
    event will eventually start all flows which have been previously deployed with `@trigger`
    to wait for this particular named event.

    Parameters
    ----------
    name : Union[str, Callable[[], str]]
        Name of the event, or a callable (invoked with no arguments) that returns the event name (e.g., `namespaced_event_name('foo')`).
    url : str, optional
        Override the event endpoint from `ARGO_EVENTS_WEBHOOK_URL`.
    payload : Dict, optional
        A set of key-value pairs delivered in this event. Used to set parameters of triggered flows.
    """

    TYPE = "argo-workflows"
    LABEL = "Argo Event"

    @classmethod
    def is_configured(cls):
        # Check the env var directly (not the module-level constant) so that
        # env vars set after import are picked up.  Also fall back to the
        # already-loaded config value for non-env-var configuration sources.
        return bool(
            os.environ.get("ARGO_EVENTS_WEBHOOK_URL") or ARGO_EVENTS_WEBHOOK_URL
        )

    def __init__(self, name, url=None, payload=None, access_token=None):
        # TODO: Introduce support for NATS
        if callable(name):
            name = name()
            if not isinstance(name, str):
                raise ArgoEventException(
                    "Callable for 'name' must return a string, got %s"
                    % type(name).__name__
                )
        super().__init__(name, payload=payload)
        # Resolve URL: explicit arg > env var > config constant
        self._url = (
            url or os.environ.get("ARGO_EVENTS_WEBHOOK_URL") or ARGO_EVENTS_WEBHOOK_URL
        )
        self._access_token = access_token

    def safe_publish(self, payload=None, ignore_errors=True):
        """
        Publishes an event when called inside a deployed workflow. Outside a deployed workflow
        this function does nothing.

        Use this function inside flows to create events safely. As this function is a no-op
        for local runs, you can safely call it during local development without causing unintended
        side-effects. It takes effect only when deployed on Argo Workflows.

        Parameters
        ----------
        payload : dict
            Additional key-value pairs to add to the payload.
        ignore_errors : bool, default True
            If True, events are created on a best effort basis - errors are silently ignored.
        """

        return self.publish(payload=payload, force=False, ignore_errors=ignore_errors)

    def _do_publish(self, payload):
        headers = {}
        if self._access_token:
            # TODO: Test with bearer tokens
            headers = {"Authorization": "Bearer {}".format(self._access_token)}
        if ARGO_EVENTS_WEBHOOK_AUTH == "service":
            headers.update(SERVICE_HEADERS)

        # Argo wraps the payload in a {"name": ..., "payload": {...}} envelope
        data = {
            "name": self._name,
            "payload": self._build_payload(payload),
        }
        request = urllib.request.Request(
            self._url,
            method="POST",
            headers={"Content-Type": "application/json", **headers},
            data=json.dumps(data).encode("utf-8"),
        )

        for i in range(SERVICE_RETRY_COUNT):
            try:
                urllib.request.urlopen(request, timeout=60)
                return data["payload"]["id"]
            except urllib.error.HTTPError as e:
                # TODO: Retry retryable HTTP error codes
                raise e
            except urllib.error.URLError as e:
                if i == SERVICE_RETRY_COUNT - 1:
                    raise e
                else:
                    time.sleep(2**i)

    def publish(self, payload=None, force=True, ignore_errors=True):
        """
        Publishes an event.

        Note that the function returns immediately after the event has been sent. It
        does not wait for flows to start, nor it guarantees that any flows will start.

        Parameters
        ----------
        payload : dict
            Additional key-value pairs to add to the payload.
        force : bool, default True
            If False, only publishes when running inside an Argo Workflow.
        ignore_errors : bool, default True
            If True, events are created on a best effort basis - errors are silently ignored.
        """
        if payload is None:
            payload = {}
        # Publish event iff forced or running on Argo Workflows
        if force or os.environ.get("ARGO_WORKFLOW_TEMPLATE"):
            return super().publish(payload=payload, ignore_errors=ignore_errors)
        else:
            msg = (
                "Argo Event (%s) was not published. Use "
                + "ArgoEvent(...).publish(...) "
                + "to force publish."
            ) % self._name

            if ignore_errors:
                print(msg, file=sys.stderr)
            else:
                raise ArgoEventException(msg)

    def _make_exception(self, msg):
        return ArgoEventException(msg)
