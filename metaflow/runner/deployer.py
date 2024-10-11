import json
import time

from typing import Optional, Dict, TYPE_CHECKING

from metaflow.exception import MetaflowNotFound
from metaflow.metaflow_config import DEFAULT_FROM_DEPLOYMENT_IMPL

if TYPE_CHECKING:
    import metaflow
    from .deployer_impl import DeployerImpl


class DeployerMeta(type):
    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)

        from metaflow.plugins import DEPLOYER_IMPL_PROVIDERS

        def _injected_method(deployer_class):
            def f(self, **deployer_kwargs):
                return deployer_class(
                    deployer_kwargs=deployer_kwargs,
                    flow_file=self.flow_file,
                    show_output=self.show_output,
                    profile=self.profile,
                    env=self.env,
                    cwd=self.cwd,
                    file_read_timeout=self.file_read_timeout,
                    **self.top_level_kwargs
                )

            return f

        for provider_class in DEPLOYER_IMPL_PROVIDERS:
            # TYPE is the name of the CLI groups i.e.
            # `argo-workflows` instead of `argo_workflows`
            # The injected method names replace '-' by '_' though.
            method_name = provider_class.TYPE.replace("-", "_")
            setattr(cls, method_name, _injected_method(provider_class))

        return cls


class Deployer(metaclass=DeployerMeta):
    """
    Use the `Deployer` class to configure and access one of the production
    orchestrators supported by Metaflow.

    Parameters
    ----------
    flow_file : str
        Path to the flow file to deploy.
    show_output : bool, default True
        Show the 'stdout' and 'stderr' to the console by default.
    profile : Optional[str], default None
        Metaflow profile to use for the deployment. If not specified, the default
        profile is used.
    env : Optional[Dict[str, str]], default None
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
        self.flow_file = flow_file
        self.show_output = show_output
        self.profile = profile
        self.env = env
        self.cwd = cwd
        self.file_read_timeout = file_read_timeout
        self.top_level_kwargs = kwargs


class TriggeredRun(object):
    """
    TriggeredRun class represents a run that has been triggered on a
    production orchestrator.
    """

    def __init__(
        self,
        deployer: "DeployerImpl",
        content: str,
    ):
        self.deployer = deployer
        content_json = json.loads(content)
        self.metadata_for_flow = content_json.get("metadata")
        self.pathspec = content_json.get("pathspec")
        self.name = content_json.get("name")

    def wait_for_run(self, timeout: Optional[int] = None):
        """
        Wait for the `run` property to become available.

        The `run` property becomes available only after the `start` task of the triggered
        flow starts running.

        Parameters
        ----------
        timeout : int, optional, default None
            Maximum time to wait for the `run` to become available, in seconds. If
            None, wait indefinitely.

        Raises
        ------
        TimeoutError
            If the `run` is not available within the specified timeout.
        """
        start_time = time.time()
        check_interval = 5
        while True:
            if self.run is not None:
                return self.run

            if timeout is not None and (time.time() - start_time) > timeout:
                raise TimeoutError(
                    "Timed out waiting for the run object to become available."
                )

            time.sleep(check_interval)

    @property
    def run(self) -> Optional["metaflow.Run"]:
        """
        Retrieve the `Run` object for the triggered run.

        Note that Metaflow `Run` becomes available only when the `start` task
        has started executing.

        Returns
        -------
        Run, optional
            Metaflow Run object if the `start` step has started executing, otherwise None.
        """
        from metaflow import Run

        try:
            return Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None


class DeployedFlowMeta(type):
    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)

        from metaflow.plugins import DEPLOYER_IMPL_PROVIDERS

        allowed_providers = dict(
            {
                provider.TYPE.replace("-", "_"): provider
                for provider in DEPLOYER_IMPL_PROVIDERS
            }
        )

        def _default_injected_method():
            def f(
                identifier: str,
                metadata: Optional[str] = None,
                impl: str = DEFAULT_FROM_DEPLOYMENT_IMPL.replace("-", "_"),
            ):
                if impl in allowed_providers:
                    return allowed_providers[impl].deployed_flow_type.from_deployment(
                        identifier, metadata, impl
                    )

            return f

        setattr(cls, "from_deployment", _default_injected_method())

        return cls


class DeployedFlow(metaclass=DeployedFlowMeta):
    """
    DeployedFlow class represents a flow that has been deployed.

    This class is not meant to be instantiated directly. Instead, it is returned from
    methods of `Deployer`.
    """

    def __init__(self, deployer: "DeployerImpl"):
        self.deployer = deployer
        self.name = self.deployer.name
        self.flow_name = self.deployer.flow_name
        self.metadata = self.deployer.metadata
