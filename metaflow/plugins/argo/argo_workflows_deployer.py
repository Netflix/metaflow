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
        Argo workflow name. The name of the flow is used if not specified.

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

        TODO: Complete all parameters. I'd also update the signature with the
        arguments instead of the generic `**kwargs`.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize using this production token

        Returns
        -------
        ArgoWorkflowsDeployedFlow
            The Flow deployed to Argo Workflows.
        """

        # Prevent circular import
        from .argo_workflows_deployer_objects import ArgoWorkflowsDeployedFlow

        return self._create(ArgoWorkflowsDeployedFlow, **kwargs)


_addl_stubgen_modules = ["metaflow.plugins.argo.argo_workflows_deployer_objects"]
