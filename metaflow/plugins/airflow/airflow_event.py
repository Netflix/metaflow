import base64
import json
import os
import sys
import time
import urllib
import uuid
from datetime import datetime

from metaflow.exception import MetaflowException


class AirflowEventException(MetaflowException):
    headline = "Airflow Event Exception"


class AirflowEvent(object):
    """
    AirflowEvent triggers an Airflow DAG run via the Airflow REST API,
    used to start flows deployed with @trigger.

    Parameters
    ----------
    name : str
        Event name — maps to the DAG ID of the deployed flow.
    url : str, optional
        Airflow webserver base URL. Defaults to METAFLOW_AIRFLOW_WEBSERVER_URL env var.
    payload : dict, optional
        Key-value pairs delivered as DAG run conf parameters.
    """

    TYPE = "airflow"

    @classmethod
    def is_configured(cls):
        return bool(os.environ.get("METAFLOW_AIRFLOW_WEBSERVER_URL"))

    def __init__(self, name, url=None, payload=None):
        self._name = name
        self._url = url or os.environ.get(
            "METAFLOW_AIRFLOW_WEBSERVER_URL", "http://localhost:8080"
        )
        self._payload = payload or {}

    def add_to_payload(self, key, value):
        """Add a key-value pair to the event payload."""
        self._payload[key] = str(value)
        return self

    def publish(self, payload=None, ignore_errors=True):
        """
        Trigger an Airflow DAG run via the REST API.

        Parameters
        ----------
        payload : dict, optional
            Additional key-value pairs to merge into the conf.
        ignore_errors : bool, default True
            If True, errors are silently ignored.

        Returns
        -------
        str or None
            The DAG run ID if triggered successfully, None otherwise.
        """
        if payload is None:
            payload = {}

        try:
            dag_run_id = "metaflow__%s__%s" % (
                self._name,
                str(uuid.uuid4())[:8],
            )
            conf = {
                "name": self._name,
                "id": str(uuid.uuid4()),
                "timestamp": int(time.time()),
                "utc_date": datetime.utcnow().strftime("%Y%m%d"),
                "generated-by-metaflow": True,
                **self._payload,
                **payload,
            }

            api_url = "%s/api/v1/dags/%s/dagRuns" % (
                self._url.rstrip("/"),
                self._name,
            )
            data = json.dumps({"dag_run_id": dag_run_id, "conf": conf}).encode("utf-8")

            headers = {"Content-Type": "application/json"}
            # Support basic auth via env vars
            username = os.environ.get("METAFLOW_AIRFLOW_REST_API_USERNAME")
            password = os.environ.get("METAFLOW_AIRFLOW_REST_API_PASSWORD")
            if username and password:
                credentials = base64.b64encode(
                    ("%s:%s" % (username, password)).encode("utf-8")
                ).decode("utf-8")
                headers["Authorization"] = "Basic %s" % credentials

            request = urllib.request.Request(
                api_url,
                method="POST",
                headers=headers,
                data=data,
            )

            urllib.request.urlopen(request, timeout=60)
            print("Airflow Event (%s) published." % self._name, file=sys.stderr)
            return dag_run_id

        except Exception as e:
            msg = "Unable to publish Airflow Event (%s): %s" % (self._name, e)
            if ignore_errors:
                print(msg, file=sys.stderr)
                return None
            else:
                raise AirflowEventException(msg)
