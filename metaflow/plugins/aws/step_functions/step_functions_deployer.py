from typing import Any, ClassVar, Dict, Optional, TYPE_CHECKING, Type

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    import metaflow.plugins.aws.step_functions.step_functions_deployer_objects


class StepFunctionsDeployer(DeployerImpl):
    """
    Deployer implementation for AWS Step Functions.
    """

    TYPE: ClassVar[Optional[str]] = "step-functions"

    def __init__(self, deployer_kwargs, **kwargs):
        """
        Initialize the StepFunctionsDeployer.

        Parameters
        ----------
        deployer_kwargs : dict
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
            "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctionsDeployedFlow"
        ]
    ):
        from .step_functions_deployer_objects import StepFunctionsDeployedFlow

        return StepFunctionsDeployedFlow

    def create(
        self, **kwargs
    ) -> "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctionsDeployedFlow":
        from .step_functions_deployer_objects import StepFunctionsDeployedFlow

        return self._create(StepFunctionsDeployedFlow, **kwargs)


_addl_stubgen_modules = [
    "metaflow.plugins.aws.step_functions.step_functions_deployer_objects"
]
