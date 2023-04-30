import json
import os
import sys
import time
import urllib
import uuid
from datetime import datetime

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import ARGO_EVENTS_WEBHOOK_URL


class ArgoEventException(MetaflowException):
    headline = "Argo Event Exception"


class ArgoEvent(object):
    def __init__(
        self, name, url=ARGO_EVENTS_WEBHOOK_URL, payload=None, access_token=None
    ):
        # TODO: Introduce support for NATS
        self._name = name
        self._url = url
        self._payload = payload or {}
        self._access_token = access_token

    def add_to_payload(self, key, value):
        self._payload[key] = str(value)
        return self

    def publish(self, payload=None, force=False, ignore_errors=True):
        if payload == None:
            payload = {}
        # Publish event iff forced or running on Argo Workflows
        if force or os.environ.get("ARGO_WORKFLOW_TEMPLATE"):
            try:
                headers = {}
                if self._access_token:
                    # TODO: Test with bearer tokens
                    headers = {"Authorization": "Bearer {}".format(self._access_token)}
                # TODO: do we need to worry about certs?

                # Use urllib to avoid introducing any dependency in Metaflow
                request = urllib.request.Request(
                    self._url,
                    method="POST",
                    headers={"Content-Type": "application/json", **headers},
                    data=json.dumps(
                        {
                            "name": self._name,
                            "payload": {
                                # Add default fields here...
                                "name": self._name,
                                "id": str(uuid.uuid4()),
                                "timestamp": int(time.time()),
                                "utc_date": datetime.utcnow().strftime("%Y%m%d"),
                                "generated-by-metaflow": True,
                                **self._payload,
                                **payload,
                            },
                        }
                    ).encode("utf-8"),
                )
                retries = 3
                backoff_factor = 2

                for i in range(retries):
                    try:
                        urllib.request.urlopen(request, timeout=10.0)
                        print(
                            "Argo Event (%s) published." % self._name, file=sys.stderr
                        )
                        break
                    except urllib.error.HTTPError as e:
                        # TODO: Retry retryable HTTP error codes
                        raise e
                    except urllib.error.URLError as e:
                        if i == retries - 1:
                            raise e
                        else:
                            time.sleep(backoff_factor**i)
            except Exception as e:
                msg = "Unable to publish Argo Event (%s): %s" % (self._name, e)
                if ignore_errors:
                    print(msg, file=sys.stderr)
                else:
                    raise ArgoEventException(msg)
        else:
            msg = (
                "Argo Event (%s) was not published. Use "
                + "ArgoEvent(...).publish(..., force=True) "
                + "to force publish."
            ) % self._name

            if ignore_errors:
                print(msg, file=sys.stderr)
            else:
                raise ArgoEventException(msg)
