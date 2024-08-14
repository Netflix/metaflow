import math
import time
import datetime
from typing import Optional, Union, Dict, Any, Tuple, Callable

from metaflow.metaflow_config import (
    ARGO_RUN_URL_PREFIX,
    METAFLOW_RUN_URL_PREFIX,
    KUBERNETES_NAMESPACE,
)
from metaflow.plugins.aip.argo_client import ArgoClient
from metaflow.plugins.aip.aip_decorator import AIPException
from metaflow.plugins.aip.aip_utils import _get_aip_logger


logger = _get_aip_logger()


class ArgoHelper:
    def __init__(self, kubernetes_namespace: str = KUBERNETES_NAMESPACE):
        """
        Args:
            kubernetes_namespace: Namespace where Argo is running.
                Required as the defaults provided in the ArgoClient is usually not what customers desire.
                TODO: This namespace can be default to the current namespace if the script is ran within a cluster.
        """
        self._client = ArgoClient(namespace=kubernetes_namespace)

    def trigger(
        self,
        template_name: Optional[str] = None,
        parameters: Optional[dict] = None,
        wait_timeout: Union[int, float, datetime.timedelta] = 0,
        **kwarg,
    ) -> Tuple[str, str]:
        """
        Trigger an existing workflow template.
        Optionally this function can wait (blocking) for the workflow to complete.

        Args:
            template_name: Name of the workflow template to trigger.
            parameters: Parameters to pass to the workflow template.
            wait_timeout: Time to wait for the workflow to complete. Set to 0 to skip waiting.
            **kwarg: Other parameters for the watch function. See `ArgoHelper.watch`.
        """

        try:
            # TODO(talebz): add tag of origin-run-id to correlate parent flow
            logger.info(f"Triggering workflow template: {template_name}")
            workflow_manifest: Dict[str, Any] = self._client.trigger_workflow_template(
                template_name, parameters
            )
        except Exception as e:
            raise AIPException(str(e))

        argo_run_id = workflow_manifest["metadata"]["name"]
        argo_run_uid = workflow_manifest["metadata"]["uid"]

        if (
            wait_timeout
        ):  # int, float and datetime.timedelta all evaluates to False when 0
            self.watch(
                run_id=argo_run_id,
                wait_timeout=wait_timeout,
                **kwarg,
            )

        return argo_run_id, argo_run_uid

    def trigger_latest(
        self,
        template_prefix: Optional[str] = None,
        project_name: Optional[str] = None,
        branch_name: Optional[str] = None,
        flow_name: Optional[str] = None,
        filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None,
        parameters: Optional[dict] = None,
        wait_timeout: Union[int, float, datetime.timedelta] = 0,
        **kwarg,
    ) -> Tuple[str, str]:
        """
        Trigger the latest workflow template that matches the specified filters.
        Optionally this function can wait (blocking) for the workflow to complete.

        Args:
            template_prefix: Prefix of the template name to match.
            project_name: Project name to match.
            branch_name: Branch name to match.
            filter_func: Custom filter function that is passed template, and should return boolean value
                indicating if the template can be used.
            parameters: Parameters to pass to the workflow template.
            wait_timeout: Time to wait for the workflow to complete. Set to 0 to skip waiting.
            **kwarg: Other parameters for the watch function. See `ArgoHelper.watch`.
        """
        template_name: str = self.template_get_latest(
            project_name=project_name,
            branch_name=branch_name,
            template_prefix=template_prefix,
            flow_name=flow_name,
            filter_func=filter_func,
        )

        return self.trigger(
            template_name=template_name,
            parameters=parameters,
            wait_timeout=wait_timeout,
            **kwarg,
        )

    def template_get_latest(
        self,
        template_prefix: Optional[str] = None,
        project_name: Optional[str] = None,
        branch_name: Optional[str] = None,
        flow_name: Optional[str] = None,
        filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None,
        name_only: bool = True,
    ) -> Union[str, Dict[str, Any]]:
        """
        Args:
            template_prefix: Prefix of the template name to match.
            project_name: Project name to match.
            branch_name: Branch name to match.
            flow_name: Flow name to match.
            filter_func: Custom filter function that is passed template, and should return boolean value
                indicating if the template can be used.
            name_only: Whether to return only the name of the template or the full manifest. Defaults to True.

        Returns:
            The name of the latest workflow template, or the full manifest if name_only is set to False.
        """
        if not any(
            [template_prefix, project_name, branch_name, flow_name, filter_func]
        ):
            raise AIPException(
                "Finding latest argo workflow with no specified filters risks picking up unexpected templates. "
                "Please set at least one of project_name, branch_name, template_prefix, flow_name or filter_func."
            )

        # TODO:
        # - Add filter by project_id instead of project name - project_id is not added as a label yet.

        templates = self._client.list_workflow_template()["items"]

        templates = [
            template
            for template in templates
            if (
                not project_name
                or template["metadata"]["labels"].get("metaflow.org/tag_project-name")
                == project_name
            )
            and (
                not branch_name
                or template["metadata"]["labels"].get("gitlab.zgtools.net/branch-name")
                == branch_name
            )
            and (
                not flow_name
                or template["metadata"]["labels"].get("metaflow.org/flow_name")
                == flow_name
            )
            and (
                not template_prefix
                or template["metadata"]["name"].startswith(template_prefix)
            )
            and (not filter_func or filter_func(template))
        ]
        if not templates:
            raise AIPException(
                f"No workflow template found with constraints "
                f"{project_name=}, {branch_name=}, {template_prefix=}, {filter_func=}"
            )

        # Sort by creation timestamp to get the latest template.
        templates.sort(
            key=lambda template: template["metadata"]["creationTimestamp"], reverse=True
        )
        latest_template = templates[0]
        latest_template_name = latest_template["metadata"]["name"]
        logger.info(
            f"Found {len(templates)} WorkflowTemplates. Using latest workflow template: {latest_template_name}"
        )

        if name_only:
            return latest_template_name
        else:
            return latest_template

    def template_delete(
        self,
        template_name: str,
    ):
        try:
            self._client.delete_workflow_template(template_name)
        except Exception as e:
            raise AIPException(str(e))

    def watch(
        self,
        run_id: str,
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

        Args:
            run_id: Argo workflow run id.
            wait_timeout: Wait timeout in seconds or datetime.timedelta.
            min_check_delay: Minimum check delay in seconds.
            max_check_delay: Maximum check delay in seconds.
            assert_success: Whether to throw exception if run is not successful.
        """

        def get_delay(secs_since_start, min_delay, max_delay):
            """
            This sigmoid function reaches
            - 0.1 after 11 minutes
            - 0.5 after 15 minutes
            - 1.0 after 23 minutes
            in other words, the delay is close to min_delay during the first 10 minutes
            """
            sigmoid = 1.0 / (1.0 + math.exp(-0.01 * secs_since_start + 9.0))
            return min_delay + sigmoid * (max_delay - min_delay)

        run_status: str = self._client.get_workflow_run_status(run_id)

        if not self._is_finished_run(run_status) and wait_timeout:
            if isinstance(wait_timeout, datetime.timedelta):
                wait_timeout = wait_timeout.total_seconds()

            # A mimic of aip.Client.wait_for_run_completion with customized logging
            logger.info(
                f"Waiting for workflow {run_id} to finish. Timeout: {wait_timeout} second(s)"
            )
            start_time = datetime.datetime.now()
            while not self._is_finished_run(run_status):
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
                        elapsed_time,
                        min_delay=min_check_delay,
                        max_delay=max_check_delay,
                    )
                )
                run_status = self._client.get_workflow_run_status(run_id)

            if assert_success:
                self._assert_run_success(run_id, run_status)

        return run_status

    @staticmethod
    def _is_finished_run(run_status: Optional[str]):
        return run_status and run_status.lower() in [
            "succeeded",
            "failed",
            "skipped",
            "error",
        ]

    @staticmethod
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


def get_metaflow_run_id(argo_run_uid: str):
    """Return metaflow run id useful for querying metadata, datastore, log etc."""
    return (
        f"argo-{argo_run_uid}" if not argo_run_uid.startswith("argo-") else argo_run_uid
    )


def get_argo_url(
    argo_run_id: str,
    kubernetes_namespace: str,
    argo_workflow_uid: str,
):
    argo_ui_url = f"{ARGO_RUN_URL_PREFIX}/argo-ui/workflows/{kubernetes_namespace}/{argo_run_id}?uid={argo_workflow_uid}"
    return argo_ui_url


def get_metaflow_url(flow_name: str, argo_run_id: str):
    metaflow_ui_url = (
        f"{METAFLOW_RUN_URL_PREFIX}/{flow_name}/{get_metaflow_run_id(argo_run_id)}"
    )
    return metaflow_ui_url
