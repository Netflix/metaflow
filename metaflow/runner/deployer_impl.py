import importlib
import json
import os
import sys
import tempfile

from typing import Any, ClassVar, Dict, Optional, TYPE_CHECKING, Type

from .subprocess_manager import SubprocessManager
from .utils import get_lower_level_group, handle_timeout

if TYPE_CHECKING:
    import metaflow.runner.deployer

# NOTE: This file is separate from the deployer.py file to prevent circular imports.
# This file is needed in any of the DeployerImpl implementations
# (like argo_workflows_deployer.py) which is in turn needed to create the Deployer
# class (ie: it uses ArgoWorkflowsDeployer to create the Deployer class).


class DeployerImpl(object):
    """
    Base class for deployer implementations. Each implementation should define a TYPE
    class variable that matches the name of the CLI group.

    Parameters
    ----------
    flow_file : str
        Path to the flow file to deploy.
    show_output : bool, default True
        Show the 'stdout' and 'stderr' to the console by default.
    profile : Optional[str], default None
        Metaflow profile to use for the deployment. If not specified, the default
        profile is used.
    env : Optional[Dict], default None
        Additional environment variables to set for the deployment.
    cwd : Optional[str], default None
        The directory to run the subprocess in; if not specified, the current
        directory is used.
    file_read_timeout : int, default 3600
        The timeout until which we try to read the deployer attribute file.
    **kwargs : Any
        Additional arguments that you would pass to `python myflow.py` before
        the deployment command.
    """

    TYPE: ClassVar[Optional[str]] = None

    def __init__(
        self,
        flow_file: str,
        show_output: bool = True,
        profile: Optional[str] = None,
        env: Optional[Dict] = None,
        cwd: Optional[str] = None,
        file_read_timeout: int = 3600,
        **kwargs
    ):
        if self.TYPE is None:
            raise ValueError(
                "DeployerImpl doesn't have a 'TYPE' to target. Please use a sub-class "
                "of DeployerImpl."
            )

        if "metaflow.cli" in sys.modules:
            importlib.reload(sys.modules["metaflow.cli"])
        from metaflow.cli import start
        from metaflow.runner.click_api import MetaflowAPI

        self.flow_file = flow_file
        self.show_output = show_output
        self.profile = profile
        self.env = env
        self.cwd = cwd
        self.file_read_timeout = file_read_timeout

        self.env_vars = os.environ.copy()
        self.env_vars.update(self.env or {})
        if self.profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.spm = SubprocessManager()
        self.top_level_kwargs = kwargs
        self.api = MetaflowAPI.from_cli(self.flow_file, start)

    @property
    def deployer_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    def deployed_flow_type() -> Type["metaflow.runner.deployer.DeployedFlow"]:
        raise NotImplementedError

    def __enter__(self) -> "DeployerImpl":
        return self

    def create(self, **kwargs) -> "metaflow.runner.deployer.DeployedFlow":
        """
        Create a sub-class of a `DeployedFlow` depending on the deployer implementation.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments to pass to `create` corresponding to the
            command line arguments of `create`

        Returns
        -------
        DeployedFlow
            DeployedFlow object representing the deployed flow.

        Raises
        ------
        Exception
            If there is an error during deployment.
        """
        # Sub-classes should implement this by simply calling _create and pass the
        # proper class as the DeployedFlow to return.
        raise NotImplementedError

    def _create(
        self, create_class: Type["metaflow.runner.deployer.DeployedFlow"], **kwargs
    ) -> "metaflow.runner.deployer.DeployedFlow":
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )
            # every subclass needs to have `self.deployer_kwargs`
            command = get_lower_level_group(
                self.api, self.top_level_kwargs, self.TYPE, self.deployer_kwargs
            ).create(deployer_attribute_file=tfp_runner_attribute.name, **kwargs)

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )

            command_obj = self.spm.get(pid)
            content = handle_timeout(
                tfp_runner_attribute, command_obj, self.file_read_timeout
            )
            content = json.loads(content)
            self.name = content.get("name")
            self.flow_name = content.get("flow_name")
            self.metadata = content.get("metadata")
            # Additional info is used to pass additional deployer specific information.
            # It is used in non-OSS deployers (extensions).
            self.additional_info = content.get("additional_info", {})

            if command_obj.process.returncode == 0:
                return create_class(deployer=self)

        raise RuntimeError("Error deploying %s to %s" % (self.flow_file, self.TYPE))

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Cleanup resources on exit.
        """
        self.cleanup()

    def cleanup(self):
        """
        Cleanup resources.
        """
        self.spm.cleanup()
