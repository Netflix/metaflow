import math
import time
import datetime
from typing import Optional, Union, Dict, Any, Tuple

from metaflow.metaflow_config import ARGO_RUN_URL_PREFIX, METAFLOW_RUN_URL_PREFIX
from metaflow.plugins.aip.argo_client import ArgoClient
from metaflow.plugins.aip.aip_decorator import AIPException
from metaflow.plugins.aip.aip_utils import _get_aip_logger


logger = _get_aip_logger()


def run_argo_workflow(
    kubernetes_namespace: str,
    template_name: str,
    parameters: Optional[dict] = None,
    wait_timeout: Union[int, float, datetime.timedelta] = 0,
    **kwarg,  # Other parameters for wait function
) -> Tuple[str, str]:
    try:
        # TODO(talebz): add tag of origin-run-id to correlate parent flow
        workflow_manifest: Dict[str, Any] = ArgoClient(
            namespace=kubernetes_namespace,
        ).trigger_workflow_template(template_name, parameters)
    except Exception as e:
        raise AIPException(str(e))

    argo_run_id = workflow_manifest["metadata"]["name"]
    argo_run_uid = workflow_manifest["metadata"]["uid"]

    if wait_timeout:  # int, float and datetime.timedelta all evaluates to False when 0
        wait_for_argo_run_completion(
            run_id=argo_run_id,
            kubernetes_namespace=kubernetes_namespace,
            wait_timeout=wait_timeout,
            **kwarg,
        )

    return argo_run_id, argo_run_uid


def delete_argo_workflow(
    kubernetes_namespace: str,
    template_name: str,
):
    try:
        ArgoClient(namespace=kubernetes_namespace).delete_workflow_template(
            template_name
        )
    except Exception as e:
        raise AIPException(str(e))


def to_metaflow_run_id(argo_run_uid: str):
    """Return metaflow run id useful for querying metadata, datastore, log etc."""
    return (
        f"argo-{argo_run_uid}" if not argo_run_uid.startswith("argo-") else argo_run_uid
    )


def run_id_to_url(argo_run_id: str, kubernetes_namespace: str, argo_workflow_uid: str):
    argo_ui_url = f"{ARGO_RUN_URL_PREFIX}/argo-ui/workflows/{kubernetes_namespace}/{argo_run_id}?uid={argo_workflow_uid}"
    return argo_ui_url


def run_id_to_metaflow_url(flow_name: str, argo_run_id: str):
    metaflow_ui_url = (
        f"{METAFLOW_RUN_URL_PREFIX}/{flow_name}/{to_metaflow_run_id(argo_run_id)}"
    )
    return metaflow_ui_url


def _is_finished_run(run_status: Optional[str]):
    return run_status and run_status.lower() in [
        "succeeded",
        "failed",
        "skipped",
        "error",
    ]


def _assert_run_success(run_id: str, runs_status: str):
    if runs_status is None:
        # None status usually occurs when run is recently started and has not been scheduled
        # Raise different error, allowing user to catch them.
        raise ValueError(
            f"Run {run_id=} status not available. Run might not have been scheduled."
        )
    else:
        assert (
            runs_status.lower() == "succeeded"
        ), f"Run {run_id=} finished with non-successful state {runs_status}."


def wait_for_argo_run_completion(
    run_id: str,
    kubernetes_namespace: str,
    wait_timeout: Union[int, float, datetime.timedelta] = 0,
    min_check_delay: int = 10,
    max_check_delay: int = 30,
    assert_success: bool = True,
) -> str:
    """Check for Argo run status. Returns status as a string.

    If timeout (in second) is positive this function waits for flow to complete.
    Raise timeout if run is not finished after <timeout> seconds
    - finished or not (bool)
    - success or not (bool)
    - run info (kfp_server_api.ApiRun)

    Status check frequency will be close to min_check_delay for the first 11 minutes,
    and gradually approaches max_check_delay after 23 minutes.

    A close mimic to async is to use _get_kfp_run above.
    Implementation for async is not prioritized until specifically requested.

    TODO(yunw)(AIP-5671): Async version
    """

    def get_delay(secs_since_start, min_delay, max_delay):
        """
        this sigmoid function reaches
        - 0.1 after 11 minutes
        - 0.5 after 15 minutes
        - 1.0 after 23 minutes
        in other words, the delay is close to min_delay during the first 10 minutes
        """
        sigmoid = 1.0 / (1.0 + math.exp(-0.01 * secs_since_start + 9.0))
        return min_delay + sigmoid * (max_delay - min_delay)

    client = ArgoClient(
        namespace=kubernetes_namespace,
    )
    run_status: str = client.get_workflow_run_status(run_id)

    if not _is_finished_run(run_status) and wait_timeout:
        if isinstance(wait_timeout, datetime.timedelta):
            wait_timeout = wait_timeout.total_seconds()

        # A mimic of aip.Client.wait_for_run_completion with customized logging
        logger.info(
            f"Waiting for workflow {run_id} to finish. Timeout: {wait_timeout} second(s)"
        )
        start_time = datetime.datetime.now()
        while not _is_finished_run(run_status):
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.info(
                f"Waiting for the workflow {run_id} to complete... {elapsed_time:.2f}s / {wait_timeout}s"
            )
            if elapsed_time > wait_timeout:
                raise TimeoutError(
                    f"Timeout while waiting for workflow {run_id} to finish."
                )
            time.sleep(
                get_delay(
                    elapsed_time, min_delay=min_check_delay, max_delay=max_check_delay
                )
            )
            run_status = client.get_workflow_run_status(run_id)

        if assert_success:
            _assert_run_success(run_id, run_status)

    return run_status
