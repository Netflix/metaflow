import hashlib
import json
import sys
import time
import tempfile
import os
from typing import ClassVar, Optional

from metaflow.client.core import get_metadata
from metaflow.exception import MetaflowException
from metaflow.runner.deployer import (
    DeployedFlow,
    TriggeredRun,
)
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo


class AirflowTriggeredRun(TriggeredRun):
    """
    A class representing a triggered Airflow DAG run execution.
    """

    @property
    def status(self) -> Optional[str]:
        """
        Get the status of the triggered run via the Airflow REST API.

        Returns
        -------
        str, optional
            The Airflow dag_run state (e.g. ``"running"``, ``"success"``,
            ``"failed"``), or None if it could not be retrieved.
        """
        try:
            from metaflow.metaflow_config import (
                AIRFLOW_REST_API_URL,
                AIRFLOW_REST_API_USERNAME,
                AIRFLOW_REST_API_PASSWORD,
            )
            from .airflow_client import AirflowClient

            if not AIRFLOW_REST_API_URL:
                return None

            client = AirflowClient(
                AIRFLOW_REST_API_URL,
                username=AIRFLOW_REST_API_USERNAME,
                password=AIRFLOW_REST_API_PASSWORD,
            )
            content = json.loads(self.content)
            dag_run_id = content.get("name")
            dag_id = content.get("dag_id") or self.deployer.name
            dag_run = client.get_dag_run(dag_id, dag_run_id)
            state = dag_run.get("state")
            # Map Airflow states to conventional casing used by other deployers
            if state is not None:
                return state.upper()
            return None
        except Exception:
            return None

    @property
    def is_running(self) -> bool:
        """
        Check if the DAG run is currently running or queued.

        Returns
        -------
        bool
        """
        status = self.status
        return status is not None and status in ("RUNNING", "QUEUED")

    def wait_for_completion(
        self, check_interval: int = 5, timeout: Optional[int] = None
    ):
        """
        Wait for the DAG run to complete.

        Parameters
        ----------
        check_interval : int, default 5
            Polling interval in seconds.
        timeout : int, optional, default None
            Maximum wait time in seconds. Waits indefinitely if None.

        Raises
        ------
        TimeoutError
            If the run does not complete within *timeout* seconds.
        """
        start_time = time.time()
        while self.is_running:
            if timeout is not None and (time.time() - start_time) > timeout:
                raise TimeoutError(
                    "Airflow DAG run did not complete within specified timeout."
                )
            time.sleep(check_interval)


class AirflowDeployedFlow(DeployedFlow):
    """
    A class representing a deployed Airflow DAG.
    """

    TYPE: ClassVar[Optional[str]] = "airflow"

    def delete(self, **kwargs) -> bool:
        """
        Delete the deployed DAG via the Airflow REST API.

        Returns
        -------
        bool
            True if deletion succeeded, False otherwise.
        """
        try:
            from metaflow.metaflow_config import (
                AIRFLOW_REST_API_URL,
                AIRFLOW_REST_API_USERNAME,
                AIRFLOW_REST_API_PASSWORD,
            )
            from .airflow_client import AirflowClient

            if not AIRFLOW_REST_API_URL:
                return False

            client = AirflowClient(
                AIRFLOW_REST_API_URL,
                username=AIRFLOW_REST_API_USERNAME,
                password=AIRFLOW_REST_API_PASSWORD,
            )
            return client.delete_dag(self.deployer.name)
        except Exception:
            return False

    def trigger(self, **kwargs) -> AirflowTriggeredRun:
        """
        Trigger a new DAG run via the Airflow REST API.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments. Flow ``Parameters`` can be passed as keyword
            arguments and will be forwarded as DAG ``conf``.

        Returns
        -------
        AirflowTriggeredRun
            The triggered run instance.

        Raises
        ------
        Exception
            If there is an error during the trigger process.
        """
        from metaflow.metaflow_config import (
            AIRFLOW_REST_API_URL,
            AIRFLOW_REST_API_USERNAME,
            AIRFLOW_REST_API_PASSWORD,
        )
        from .airflow_client import AirflowClient, AirflowClientError

        if not AIRFLOW_REST_API_URL:
            raise MetaflowException(
                "METAFLOW_AIRFLOW_REST_API_URL is not set. "
                "Cannot trigger Airflow DAG run."
            )

        client = AirflowClient(
            AIRFLOW_REST_API_URL,
            username=AIRFLOW_REST_API_USERNAME,
            password=AIRFLOW_REST_API_PASSWORD,
        )

        dag_id = self.deployer.name
        # Pass any flow parameters as DAG conf
        conf = {k: v for k, v in kwargs.items() if v is not None} or None

        try:
            dag_run = client.trigger_dag_run(dag_id, conf=conf)
        except AirflowClientError as e:
            raise Exception("Error triggering DAG %s on Airflow: %s" % (dag_id, str(e)))

        dag_run_id = dag_run.get("dag_run_id") or dag_run.get("run_id", "")
        flow_name = self.deployer.flow_name

        # Compute the Metaflow run ID from the Airflow DAG run ID.
        # This mirrors AIRFLOW_MACROS.RUN_ID in airflow_utils.py:
        #   run_id_creator([run_id, dag_id]) = md5(run_id + "-" + dag_id)[:12]
        # prefixed with "airflow-".
        run_hash = hashlib.md5(
            ("%s-%s" % (dag_run_id, dag_id)).encode("utf-8")
        ).hexdigest()[:12]
        metaflow_run_id = "airflow-%s" % run_hash
        pathspec = "%s/%s" % (flow_name, metaflow_run_id)

        content = json.dumps(
            {
                "name": dag_run_id,
                "dag_id": dag_id,
                "metadata": self.deployer.metadata,
                "pathspec": pathspec,
            }
        )

        return AirflowTriggeredRun(deployer=self.deployer, content=content)

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None):
        """
        Retrieve an ``AirflowDeployedFlow`` for an existing Airflow DAG.

        Parameters
        ----------
        identifier : str
            The Airflow DAG ID.
        metadata : str, optional, default None
            Optional metadata string.

        Returns
        -------
        AirflowDeployedFlow
        """
        from metaflow.runner.deployer import Deployer, generate_fake_flow_file_contents
        from metaflow.metaflow_config import (
            AIRFLOW_REST_API_URL,
            AIRFLOW_REST_API_USERNAME,
            AIRFLOW_REST_API_PASSWORD,
        )
        from .airflow_client import AirflowClient, AirflowClientError

        if not AIRFLOW_REST_API_URL:
            raise MetaflowException("METAFLOW_AIRFLOW_REST_API_URL is not set.")

        client = AirflowClient(
            AIRFLOW_REST_API_URL,
            username=AIRFLOW_REST_API_USERNAME,
            password=AIRFLOW_REST_API_PASSWORD,
        )

        dag = client.get_dag(identifier)
        if dag is None:
            raise MetaflowException("No deployed flow found for DAG: %s" % identifier)

        # Extract flow_name from DAG tags (set by `airflow create`).
        # The Airflow DAG ID has the form "project.branch.FlowName" (dotted).
        # The flow class name (CamelCase, no dots) is always the last component.
        flow_name = identifier.split(".")[-1]  # safe default
        for tag in dag.get("tags", []):
            tag_name = tag.get("name", "")
            if tag_name.startswith("metaflow_flow_name:"):
                flow_name = tag_name.split(":", 1)[1]
                break

        fake_flow_file_contents = generate_fake_flow_file_contents(
            flow_name=flow_name, param_info={}, project_name=None
        )

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as fake_flow_file:
            with open(fake_flow_file.name, "w") as fp:
                fp.write(fake_flow_file_contents)

            d = Deployer(fake_flow_file.name).airflow(name=identifier)
            d.name = identifier
            d.flow_name = flow_name
            d.metadata = metadata if metadata is not None else get_metadata()

        return cls(deployer=d)

    @classmethod
    def get_triggered_run(
        cls, identifier: str, run_id: str, metadata: Optional[str] = None
    ):
        """
        Retrieve an ``AirflowTriggeredRun`` for an existing DAG run.

        Parameters
        ----------
        identifier : str
            The Airflow DAG ID.
        run_id : str
            The Airflow DAG run ID.
        metadata : str, optional, default None

        Returns
        -------
        AirflowTriggeredRun
        """
        deployed_flow_obj = cls.from_deployment(identifier, metadata)
        run_hash = hashlib.md5(
            ("%s-%s" % (run_id, identifier)).encode("utf-8")
        ).hexdigest()[:12]
        metaflow_run_id = "airflow-%s" % run_hash
        pathspec = "%s/%s" % (deployed_flow_obj.deployer.flow_name, metaflow_run_id)
        content = json.dumps(
            {
                "name": run_id,
                "dag_id": identifier,
                "metadata": deployed_flow_obj.deployer.metadata,
                "pathspec": pathspec,
            }
        )
        return AirflowTriggeredRun(
            deployer=deployed_flow_obj.deployer,
            content=content,
        )
