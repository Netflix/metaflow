"""
Thin wrapper around the Airflow 2.x REST API (api/v1).

All methods raise ``AirflowClientError`` on non-2xx responses so callers
don't have to inspect status codes themselves.
"""

import json
import time
import urllib.request
import urllib.error
import base64
from typing import Any, Dict, List, Optional

from .exception import AirflowException


class AirflowClientError(AirflowException):
    headline = "Airflow REST API error"


class AirflowClient:
    """
    Minimal Airflow REST API client (Airflow >= 2.0).

    Parameters
    ----------
    rest_api_url : str
        Base URL of the Airflow REST API, e.g. ``http://localhost:8090/api/v1``.
    username : str
        Basic-auth username (default: ``"admin"``).
    password : str
        Basic-auth password (default: ``"admin"``).
    """

    def __init__(
        self,
        rest_api_url: str,
        username: str = "admin",
        password: str = "admin",
    ):
        self._base = rest_api_url.rstrip("/")
        credentials = base64.b64encode(
            ("%s:%s" % (username, password)).encode()
        ).decode()
        self._auth_header = "Basic %s" % credentials

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[Dict] = None,
    ) -> Any:
        url = "%s/%s" % (self._base, path.lstrip("/"))
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": self._auth_header,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            raise AirflowClientError(
                "Airflow API %s %s returned HTTP %d: %s" % (method, url, e.code, body)
            )

    # ------------------------------------------------------------------
    # DAG operations
    # ------------------------------------------------------------------

    def get_dag(self, dag_id: str) -> Dict:
        """Return DAG metadata dict, or None if not found."""
        try:
            return self._request("GET", "dags/%s" % dag_id)
        except AirflowClientError as e:
            if "HTTP 404" in str(e):
                return None
            raise

    def patch_dag(self, dag_id: str, **fields) -> Dict:
        """Patch DAG fields (e.g. ``is_paused=False``)."""
        return self._request("PATCH", "dags/%s" % dag_id, body=fields)

    def delete_dag(self, dag_id: str) -> bool:
        """Delete a DAG. Returns True on success."""
        try:
            self._request("DELETE", "dags/%s" % dag_id)
            return True
        except AirflowClientError:
            return False

    def list_dags(self, tags: Optional[List[str]] = None) -> List[Dict]:
        """List all visible DAGs, optionally filtered by tags."""
        params = ""
        if tags:
            params = "?" + "&".join("tags=%s" % t for t in tags)
        result = self._request("GET", "dags%s" % params)
        return result.get("dags", [])

    # ------------------------------------------------------------------
    # DAG run operations
    # ------------------------------------------------------------------

    def trigger_dag_run(
        self,
        dag_id: str,
        conf: Optional[Dict] = None,
        run_id: Optional[str] = None,
    ) -> Dict:
        """Trigger a DAG run. Returns the dag_run dict."""
        body: Dict[str, Any] = {}
        if conf:
            body["conf"] = conf
        if run_id:
            body["dag_run_id"] = run_id
        return self._request("POST", "dags/%s/dagRuns" % dag_id, body=body)

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict:
        """Return dag_run dict for a specific run."""
        return self._request("GET", "dags/%s/dagRuns/%s" % (dag_id, dag_run_id))

    def list_dag_runs(self, dag_id: str, limit: int = 25) -> List[Dict]:
        """List recent dag runs for a DAG."""
        result = self._request(
            "GET",
            "dags/%s/dagRuns?limit=%d&order_by=-execution_date" % (dag_id, limit),
        )
        return result.get("dag_runs", [])

    def patch_dag_run(self, dag_id: str, dag_run_id: str, **fields) -> Dict:
        """Patch a dag run (e.g. set state to 'failed' to terminate it)."""
        return self._request(
            "PATCH",
            "dags/%s/dagRuns/%s" % (dag_id, dag_run_id),
            body=fields,
        )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def wait_for_dag(
        self,
        dag_id: str,
        timeout: int = 120,
        polling_interval: int = 5,
    ) -> Dict:
        """
        Poll until the DAG is visible in Airflow (after kubectl-cp / file copy).

        Returns the DAG metadata dict when found.

        Raises
        ------
        TimeoutError
            If the DAG is not discovered within *timeout* seconds.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                dag = self.get_dag(dag_id)
            except OSError:
                # Transient connection error (e.g. RemoteDisconnected) —
                # the webserver may still be starting up.  Retry silently.
                time.sleep(polling_interval)
                continue
            if dag is not None:
                return dag
            time.sleep(polling_interval)
        raise TimeoutError(
            "DAG '%s' did not appear in Airflow within %d seconds. "
            "Ensure the DAG file was copied to the dags folder and "
            "that the Airflow scheduler is running." % (dag_id, timeout)
        )
