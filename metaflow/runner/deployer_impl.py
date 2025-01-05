import importlib
import json
import os
import sys
import time
import logging
from typing import Any, ClassVar, Dict, Optional, TYPE_CHECKING, Type

from .subprocess_manager import SubprocessManager
from .utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    import metaflow.runner.deployer

  # NOTE: This file is separate from the deployer.py file to prevent circular imports.
# This file is needed in any of the DeployerImpl implementations
# (like argo_workflows_deployer.py) which is in turn needed to create the Deployer
# class (ie: it uses ArgoWorkflowsDeployer to create the Deployer class).

# Logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
        Timeout to read deployer attribute file (in seconds).
    max_retries : int, default 3
        Maximum number of retries for deployment in case of failure.
    retry_delay : int, default 2
        Initial delay between retries, with exponential backoff.
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
        max_retries: int = 3,
        retry_delay: int = 2,
        **kwargs
    ):
        if self.TYPE is None:
            raise ValueError(
                "DeployerImpl doesn't have a 'TYPE' to target. Please use a sub-class "
                "of DeployerImpl."
            )

        self.flow_file = flow_file
        self.show_output = show_output
        self.profile = profile
        self.env = env
        self.cwd = cwd
        self.file_read_timeout = file_read_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.env_vars = os.environ.copy()
        self.env_vars.update(self.env or {})
        if self.profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.spm = SubprocessManager()
        self.top_level_kwargs = kwargs

        logger.info("Initialized DeployerImpl for flow file: %s", flow_file)

    @property
    def deployer_kwargs(self) -> Dict[str, Any]:
        return {}

    def _retry(self, func, *args, **kwargs):
        """
        Retry a function with exponential backoff.

        Parameters
        ----------
        func : Callable
            The function to retry.
        *args : Any
            Positional arguments for the function.
        **kwargs : Any
            Keyword arguments for the function.

        Returns
        -------
        Any
            The result of the function.

        Raises
        ------
        Exception
            If all retries fail.
        """
        delay = self.retry_delay
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        "Retry %d/%d failed with error: %s. Retrying in %d seconds...",
                        attempt + 1,
                        self.max_retries,
                        str(e),
                        delay,
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error("All retries failed. Raising exception.")
                    raise e

    def _create(
        self, create_class: Type["metaflow.runner.deployer.DeployedFlow"], **kwargs
    ) -> "metaflow.runner.deployer.DeployedFlow":
        """
        Create a deployed flow instance with retry and subprocess timeout.

        Parameters
        ----------
        create_class : Type[DeployedFlow]
            Class to instantiate for the deployed flow.
        **kwargs : Any
            Additional arguments for the create process.

        Returns
        -------
        DeployedFlow
            Instance of the deployed flow.

        Raises
        ------
        RuntimeError
            If deployment fails after retries.
        """
        def deploy_with_fifo():
            with temporary_fifo() as (attribute_file_path, attribute_file_fd):
                command = get_lower_level_group(
                    self.api, self.top_level_kwargs, self.TYPE, self.deployer_kwargs
                ).create(deployer_attribute_file=attribute_file_path, **kwargs)

                pid = self.spm.run_command(
                    [sys.executable, *command],
                    env=self.env_vars,
                    cwd=self.cwd,
                    show_output=self.show_output,
                )

                command_obj = self.spm.get(pid)
                content = handle_timeout(
                    attribute_file_fd, command_obj, self.file_read_timeout
                )
                content = json.loads(content)
                self.name = content.get("name")
                self.flow_name = content.get("flow_name")
                self.metadata = content.get("metadata")
                self.additional_info = content.get("additional_info", {})

                command_obj.sync_wait()
                if command_obj.process.returncode == 0:
                    return create_class(deployer=self)

            raise RuntimeError("Error deploying %s to %s" % (self.flow_file, self.TYPE))

        return self._retry(deploy_with_fifo)

    def cleanup(self):
        logger.info("Cleaning up resources...")
        self.spm.cleanup()
