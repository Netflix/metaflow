from typing import Any, ClassVar, Dict, Optional, TYPE_CHECKING, Type

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    import metaflow.plugins.argo.argo_workflows_deployer_objects


class ArgoWorkflowsDeployer(DeployerImpl):
    """
    Deployer implementation for Argo Workflows.

    Parameters
    ----------
    name : str, optional, default None
        Argo workflow name. The flow name is used instead if this option is not specified.
    """

    TYPE: ClassVar[Optional[str]] = "argo-workflows"

    def __init__(self, deployer_kwargs: Dict[str, str], **kwargs):
        """
        Initialize the ArgoWorkflowsDeployer.

        Parameters
        ----------
        deployer_kwargs : Dict[str, str]
            The deployer-specific keyword arguments.
        **kwargs : Any
            Additional arguments to pass to the superclass constructor.
        """
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> Dict[str, Any]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> (
        Type[
            "metaflow.plugins.argo.argo_workflows_deployer_objects.ArgoWorkflowsDeployedFlow"
        ]
    ):
        from .argo_workflows_deployer_objects import ArgoWorkflowsDeployedFlow

        return ArgoWorkflowsDeployedFlow

    def create(
        self, **kwargs
    ) -> "metaflow.plugins.argo.argo_workflows_deployer_objects.ArgoWorkflowsDeployedFlow":
        """
        Create a new ArgoWorkflow deployment.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize using this production token. Required when re-deploying an existing flow
            for the first time. The token is cached in METAFLOW_HOME.
        generate_new_token : bool, optional, default False
            Generate a new production token for this flow. Moves the production flow to a new namespace.
        given_token : str, optional, default None
            Use the given production token for this flow. Moves the production flow to the given namespace.
        tags : List[str], optional, default None
            Annotate all objects produced by Argo Workflows runs with these tags.
        user_namespace : str, optional, default None
            Change the namespace from the default (production token) to the given tag.
        only_json : bool, optional, default False
            Only print out JSON sent to Argo Workflows without deploying anything.
        max_workers : int, optional, default 100
            Maximum number of parallel processes.
        workflow_timeout : int, optional, default None
            Workflow timeout in seconds.
        workflow_priority : int, optional, default None
            Workflow priority as an integer. Higher priority workflows are processed first
            if Argo Workflows controller is configured to process limited parallel workflows.
        auto_emit_argo_events : bool, optional, default True
            Auto emits Argo Events when the run completes successfully.
        notify_on_error : bool, optional, default False
            Notify if the workflow fails.
        notify_on_success : bool, optional, default False
            Notify if the workflow succeeds.
        notify_slack_webhook_url : str, optional, default ''
            Slack incoming webhook url for workflow success/failure notifications.
        notify_pager_duty_integration_key : str, optional, default ''
            PagerDuty Events API V2 Integration key for workflow success/failure notifications.
        enable_heartbeat_daemon : bool, optional, default False
            Use a daemon container to broadcast heartbeats.
        deployer_attribute_file : str, optional, default None
            Write the workflow name to the specified file. Used internally for Metaflow's Deployer API.
        enable_error_msg_capture : bool, optional, default True
            Capture stack trace of first failed task in exit hook.

        Returns
        -------
        ArgoWorkflowsDeployedFlow
            The Flow deployed to Argo Workflows.
        """

        # Prevent circular import
        from .argo_workflows_deployer_objects import ArgoWorkflowsDeployedFlow

        return self._create(ArgoWorkflowsDeployedFlow, **kwargs)


_addl_stubgen_modules = ["metaflow.plugins.argo.argo_workflows_deployer_objects"]
