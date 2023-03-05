import json
import os
import sys
import time
import urllib
from datetime import datetime
import uuid

from metaflow.exception import MetaflowException


class ArgoEventException(MetaflowException):
    headline = "Argo Event Exception"


class ArgoEvent(object):
    def __init__(self, name, payload={}):
        # TODO: Introduce support for NATS
        self.name = name
        self._payload = payload

    def add_to_payload(self, key, value):
        self._payload[key] = str(value)
        return self

    def publish(self, payload={}, force=False, access_token=None, ignore_errors=False):
        # Publish event iff forced or running on Argo Workflows
        if force or os.environ["ARGO_WORKFLOW_TEMPLATE"]:
            try:
                # TODO: Do away with this hard code before shipping
                url = "http://10.10.29.11:12000/event"
                headers = {}
                if access_token:
                    # TODO: Test with bearer tokens
                    headers = {"Authorization": "Bearer {}".format(access_token)}
                # TODO: do we need to worry about certs?

                # Use urllib to avoid introducing any dependency in Metaflow
                request = urllib.request.Request(
                    url,
                    method="POST",
                    headers={"Content-Type": "application/json", **headers},
                    data=json.dumps(
                        {
                            "name": self.name,
                            "payload": {
                                # Add default fields here...
                                "id": uuid.uuid4(),
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
                            "Argo Event for %s published." % self.name, file=sys.stderr
                        )
                        break
                    except urllib.error.HTTPError as e:
                        raise e
                    except urllib.error.URLError as e:
                        if i == retries - 1:
                            raise e
                        else:
                            time.sleep(backoff_factor**i)
            except Exception as e:
                msg = "Unable to publish Argo Event for '%s': %s" % (self.name, e)
                if ignore_errors:
                    print(msg, file=sys.stderr)
                else:
                    raise ArgoEventException(msg)
        else:
            msg = (
                "Argo Event for '%s' was not published. Use "
                + "ArgoEvent(...).publish(..., force=True) "
                + "to force publish." % self.name
            )
            if ignore_errors:
                print(msg, file=sys.stderr)
            else:
                raise ArgoEventException(msg)
