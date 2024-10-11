from typing import Any, ClassVar, Dict, Optional, TYPE_CHECKING

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    from .argo_workflows_deployer_objects import ArgoWorkflowsDeployedFlow


class ArgoWorkflowsDeployer(DeployerImpl):
    """
    Deployer implementation for Argo Workflows.
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

    @property
    def deployed_flow_type(self) -> "ArgoWorkflowsDeployedFlow":
        from .argo_workflows_deployer_objects import ArgoWorkflowsDeployedFlow

        return ArgoWorkflowsDeployedFlow

    def create(self, **kwargs) -> "ArgoWorkflowsDeployedFlow":
        # Prevent circular import
        from .argo_workflows_deployer_objects import ArgoWorkflowsDeployedFlow

        return self._create(ArgoWorkflowsDeployedFlow, **kwargs)
