import os
import subprocess
import tempfile
from typing import Any, ClassVar, Dict, Optional, TYPE_CHECKING, Type

from metaflow.exception import MetaflowException
from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    import metaflow.plugins.airflow.airflow_deployer_objects


class AirflowDeployer(DeployerImpl):
    """
    Deployer implementation for Apache Airflow.

    The DAG file produced by ``airflow create`` is copied to the Airflow
    scheduler pod with ``kubectl cp``, and the deployer waits for Airflow to
    discover it before returning.

    Parameters
    ----------
    name : str, optional, default None
        Airflow DAG name. The flow name is used instead if this option is
        not specified.
    """

    TYPE: ClassVar[Optional[str]] = "airflow"

    def __init__(self, deployer_kwargs: Dict[str, str], **kwargs):
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> Dict[str, Any]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> (
        Type["metaflow.plugins.airflow.airflow_deployer_objects.AirflowDeployedFlow"]
    ):
        from .airflow_deployer_objects import AirflowDeployedFlow

        return AirflowDeployedFlow

    def create(
        self, **kwargs
    ) -> "metaflow.plugins.airflow.airflow_deployer_objects.AirflowDeployedFlow":
        """
        Compile and deploy this flow as an Airflow DAG.

        The DAG Python file is written locally by ``airflow create``, then
        copied to the Airflow scheduler pod with ``kubectl cp``.  The deployer
        waits until Airflow discovers the DAG before returning.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize using this production token.
        generate_new_token : bool, optional, default False
            Generate a new production token for this flow.
        given_token : str, optional, default None
            Use the given production token for this flow.
        tags : List[str], optional, default None
            Annotate all objects produced by Airflow DAG runs with these tags.
        user_namespace : str, optional, default None
            Change the namespace from the default to the given tag.
        is_paused_upon_creation : bool, optional, default False
            Create the DAG in a paused state.
        max_workers : int, optional, default 100
            Maximum number of parallel processes.
        workflow_timeout : int, optional, default None
            Workflow timeout in seconds.
        worker_pool : str, optional, default None
            Worker pool for Airflow DAG execution.

        Returns
        -------
        AirflowDeployedFlow
            The Flow deployed to Airflow.
        """
        from metaflow.metaflow_config import (
            AIRFLOW_KUBERNETES_DAGS_PATH,
            AIRFLOW_KUBERNETES_NAMESPACE,
        )
        from .airflow_deployer_objects import AirflowDeployedFlow

        # Write the compiled DAG to a temp file; subprocess fills it in.
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as dag_file:
            dag_file_path = dag_file.name

        try:
            deployed_flow = self._create(
                AirflowDeployedFlow, file=dag_file_path, **kwargs
            )

            dag_id = self.name

            # Copy DAG file to the Airflow scheduler pod.
            self._kubectl_cp_dag(
                dag_file_path,
                dag_id,
                AIRFLOW_KUBERNETES_NAMESPACE,
                AIRFLOW_KUBERNETES_DAGS_PATH,
            )

            # Wait until Airflow discovers the DAG.
            from .airflow_deployer_objects import _get_airflow_client

            client, _ = _get_airflow_client()
            if client is not None:
                client.wait_for_dag(dag_id)
        finally:
            try:
                os.unlink(dag_file_path)
            except OSError:
                pass

        return deployed_flow

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_scheduler_pod(namespace: str) -> str:
        """Return the name of the Airflow scheduler pod in *namespace*."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    "component=scheduler",
                    "-o",
                    "jsonpath={.items[0].metadata.name}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            pod_name = result.stdout.strip()
            if not pod_name:
                raise MetaflowException(
                    "No Airflow scheduler pod found in namespace '%s'. "
                    "Is Airflow running?" % namespace
                )
            return pod_name
        except subprocess.CalledProcessError as e:
            raise MetaflowException("kubectl get pods failed: %s" % e.stderr)

    @classmethod
    def _kubectl_cp_dag(
        cls,
        local_dag_path: str,
        dag_id: str,
        namespace: str,
        dags_path: str,
    ) -> None:
        """Copy the compiled DAG file into the Airflow scheduler pod."""
        pod_name = cls._get_scheduler_pod(namespace)
        remote_path = "%s/%s.py" % (dags_path.rstrip("/"), dag_id)
        try:
            subprocess.run(
                [
                    "kubectl",
                    "cp",
                    local_dag_path,
                    "%s:%s" % (pod_name, remote_path),
                    "-n",
                    namespace,
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise MetaflowException(
                "kubectl cp failed while deploying DAG '%s' to pod '%s': %s"
                % (dag_id, pod_name, e.stderr)
            )


_addl_stubgen_modules = ["metaflow.plugins.airflow.airflow_deployer_objects"]
