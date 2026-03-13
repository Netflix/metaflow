import base64
import json
import os
import urllib
import uuid

from metaflow.event_provider import MetaflowEvent, MetaflowEventException


class AirflowEventException(MetaflowEventException):
    headline = "Airflow Event Exception"


class AirflowEvent(MetaflowEvent):
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
    LABEL = "Airflow Event"

    @classmethod
    def is_configured(cls):
        return bool(os.environ.get("METAFLOW_AIRFLOW_WEBSERVER_URL"))

    def __init__(self, name, url=None, payload=None):
        super().__init__(name, payload=payload)
        self._url = url or os.environ.get(
            "METAFLOW_AIRFLOW_WEBSERVER_URL", "http://localhost:8080"
        )

    def _do_publish(self, payload):
        dag_run_id = "metaflow__%s__%s" % (
            self._name,
            str(uuid.uuid4())[:8],
        )
        conf = self._build_payload(payload)

        api_url = "%s/api/v1/dags/%s/dagRuns" % (
            self._url.rstrip("/"),
            self._name,
        )
        data = json.dumps({"dag_run_id": dag_run_id, "conf": conf}).encode("utf-8")

        headers = {"Content-Type": "application/json"}
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
        return dag_run_id

    def _make_exception(self, msg):
        return AirflowEventException(msg)
