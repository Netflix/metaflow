import os
import sys
import json
import importlib
import functools
import tempfile
from typing import Optional, Dict, ClassVar

from metaflow.exception import MetaflowNotFound
from metaflow.runner.subprocess_manager import CommandManager, SubprocessManager
from metaflow.runner.utils import read_from_file_when_ready


def handle_timeout(tfp_runner_attribute, command_obj: CommandManager):
    """
    Handle the timeout for a running subprocess command that reads a file
    and raises an error with appropriate logs if a TimeoutError occurs.

    Parameters
    ----------
    tfp_runner_attribute : NamedTemporaryFile
        Temporary file that stores runner attribute data.
    command_obj : CommandManager
        Command manager object that encapsulates the running command details.

    Returns
    -------
    str
        Content read from the temporary file.

    Raises
    ------
    RuntimeError
        If a TimeoutError occurs, it raises a RuntimeError with the command's
        stdout and stderr logs.
    """
    try:
        content = read_from_file_when_ready(tfp_runner_attribute.name, timeout=10)
        return content
    except TimeoutError as e:
        stdout_log = open(command_obj.log_files["stdout"]).read()
        stderr_log = open(command_obj.log_files["stderr"]).read()
        command = " ".join(command_obj.command)
        error_message = "Error executing: '%s':\n" % command
        if stdout_log.strip():
            error_message += "\nStdout:\n%s\n" % stdout_log
        if stderr_log.strip():
            error_message += "\nStderr:\n%s\n" % stderr_log
        raise RuntimeError(error_message) from e


def get_lower_level_group(
    api, top_level_kwargs: Dict, _type: Optional[str], deployer_kwargs: Dict
):
    """
    Retrieve a lower-level group from the API based on the type and provided arguments.

    Parameters
    ----------
    api : MetaflowAPI
        Metaflow API instance.
    top_level_kwargs : Dict
        Top-level keyword arguments to pass to the API.
    _type : str
        Type of the deployer implementation to target.
    deployer_kwargs : Dict
        Keyword arguments specific to the deployer.

    Returns
    -------
    Any
        The lower-level group object retrieved from the API.

    Raises
    ------
    ValueError
        If the `_type` is None.
    """
    if _type is None:
        raise ValueError(
            "DeployerImpl doesn't have a 'TYPE' to target. Please use a sub-class of DeployerImpl."
        )
    return getattr(api(**top_level_kwargs), _type)(**deployer_kwargs)


class Deployer(object):
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
        **kwargs
    ):
        self.flow_file = flow_file
        self.show_output = show_output
        self.profile = profile
        self.env = env
        self.cwd = cwd
        self.top_level_kwargs = kwargs

        from metaflow.plugins import DEPLOYER_IMPL_PROVIDERS

        for provider_class in DEPLOYER_IMPL_PROVIDERS:
            # TYPE is the name of the CLI groups i.e.
            # `argo-workflows` instead of `argo_workflows`
            # The injected method names replace '-' by '_' though.
            method_name = provider_class.TYPE.replace("-", "_")
            setattr(Deployer, method_name, self.__make_function(provider_class))

    def __make_function(self, deployer_class):
        """
        Create a function for the given deployer class.

        Parameters
        ----------
        deployer_class : Type[DeployerImpl]
            Deployer implementation class.

        Returns
        -------
        Callable
            Function that initializes and returns an instance of the deployer class.
        """

        def f(self, **deployer_kwargs):
            return deployer_class(
                deployer_kwargs=deployer_kwargs,
                flow_file=self.flow_file,
                show_output=self.show_output,
                profile=self.profile,
                env=self.env,
                cwd=self.cwd,
                **self.top_level_kwargs
            )

        return f


class TriggeredRun(object):
    """
    TriggeredRun class represents a run that has been triggered on a production orchestrator.

    Only when the `start` task starts running, the `run` object corresponding to the run
    becomes available.
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

    def _enrich_object(self, env):
        """
        Enrich the TriggeredRun object with additional properties and methods.

        Parameters
        ----------
        env : dict
            Environment dictionary containing properties and methods to add.
        """
        for k, v in env.items():
            if isinstance(v, property):
                setattr(self.__class__, k, v)
            elif callable(v):
                setattr(self, k, functools.partial(v, self))
            else:
                setattr(self.__class__, k, property(fget=lambda _, v=v: v))

    @property
    def run(self):
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


class DeployedFlow(object):
    """
    DeployedFlow class represents a flow that has been deployed.

    Parameters
    ----------
    deployer : DeployerImpl
        Instance of the deployer implementation.
    """

    def __init__(self, deployer: "DeployerImpl"):
        self.deployer = deployer

    def _enrich_object(self, env):
        """
        Enrich the DeployedFlow object with additional properties and methods.

        Parameters
        ----------
        env : dict
            Environment dictionary containing properties and methods to add.
        """
        for k, v in env.items():
            if isinstance(v, property):
                setattr(self.__class__, k, v)
            elif callable(v):
                setattr(self, k, functools.partial(v, self))
            else:
                setattr(self.__class__, k, property(fget=lambda _, v=v: v))


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
        **kwargs
    ):
        if self.TYPE is None:
            raise ValueError(
                "DeployerImpl doesn't have a 'TYPE' to target. Please use a sub-class of DeployerImpl."
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

        self.env_vars = os.environ.copy()
        self.env_vars.update(self.env or {})
        if self.profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.spm = SubprocessManager()
        self.top_level_kwargs = kwargs
        self.api = MetaflowAPI.from_cli(self.flow_file, start)

    def __enter__(self) -> "DeployerImpl":
        return self

    def create(self, **kwargs) -> DeployedFlow:
        """
        Create a deployed flow using the deployer implementation.

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
            content = handle_timeout(tfp_runner_attribute, command_obj)
            content = json.loads(content)
            self.name = content.get("name")
            self.flow_name = content.get("flow_name")
            self.metadata = content.get("metadata")

            if command_obj.process.returncode == 0:
                deployed_flow = DeployedFlow(deployer=self)
                self._enrich_deployed_flow(deployed_flow)
                return deployed_flow

        raise Exception("Error deploying %s to %s" % (self.flow_file, self.TYPE))

    def _enrich_deployed_flow(self, deployed_flow: DeployedFlow):
        """
        Enrich the DeployedFlow object with additional properties and methods.

        Parameters
        ----------
        deployed_flow : DeployedFlow
            The DeployedFlow object to enrich.
        """
        raise NotImplementedError

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
