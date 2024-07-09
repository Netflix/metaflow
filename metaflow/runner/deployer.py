import os
import sys
import json
import importlib
import functools
import tempfile
from typing import Optional, Dict, ClassVar, Any

from metaflow.exception import MetaflowNotFound
from metaflow.runner.subprocess_manager import CommandManager, SubprocessManager
from metaflow.runner.utils import read_from_file_when_ready


def handle_timeout(tfp_runner_attribute, command_obj: CommandManager):
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
    api, top_level_kwargs: Dict, _type: str, deployer_kwargs: Dict
):
    if _type is None:
        raise ValueError(
            "DeployerImpl doesn't have a 'TYPE' to target. Please use a sub-class of DeployerImpl."
        )
    return getattr(api(**top_level_kwargs), _type)(**deployer_kwargs)


class Deployer(object):
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

        from metaflow.plugins import CONCRETE_DEPLOYER_PROVIDERS

        for provider_class in CONCRETE_DEPLOYER_PROVIDERS:
            # TYPE is the name of the CLI groups i.e.
            # `argo-workflows` instead of `argo_workflows`
            # The injected method names replace '-' by '_' though.
            method_name = provider_class.TYPE.replace("-", "_")
            setattr(Deployer, method_name, self.make_function(provider_class))

    def make_function(self, deployer_class):
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
        for k, v in env.items():
            if isinstance(v, property):
                setattr(self.__class__, k, v)
            elif callable(v):
                setattr(self, k, functools.partial(v, self))
            else:
                setattr(self.__class__, k, property(fget=lambda _, v=v: v))

    @property
    def run(self):
        from metaflow import Run

        try:
            return Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None


class DeployedFlow(object):
    def __init__(self, deployer: "DeployerImpl"):
        self.deployer = deployer

    def _enrich_object(self, env):
        for k, v in env.items():
            if isinstance(v, property):
                setattr(self.__class__, k, v)
            elif callable(v):
                setattr(self, k, functools.partial(v, self))
            else:
                setattr(self.__class__, k, property(fget=lambda _, v=v: v))


class DeployerImpl(object):
    # TYPE needs to match the names of CLI groups i.e.
    # `argo-workflows` instead of `argo_workflows`
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

        self.old_env = os.environ.copy()
        self.env_vars = self.old_env.copy()
        self.env_vars.update(self.env or {})
        if self.profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.spm = SubprocessManager()
        self.top_level_kwargs = kwargs
        self.api = MetaflowAPI.from_cli(self.flow_file, start)

    def create(self, **kwargs) -> DeployedFlow:
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )
            # every subclass needs to have `self.deployer_kwargs`
            command = get_lower_level_group(
                self.api, self.top_level_kwargs, self.TYPE, self.deployer_kwargs
            ).create(runner_attribute_file=tfp_runner_attribute.name, **kwargs)

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )

            command_obj = self.spm.get(pid)
            content = handle_timeout(tfp_runner_attribute, command_obj)
            self.name = json.loads(content).get("name")

            if command_obj.process.returncode == 0:
                deployed_flow = DeployedFlow(deployer=self)
                self._enrich_deployed_flow(deployed_flow)
                return deployed_flow

        raise Exception("Error deploying %s to %s" % (self.flow_file, self.TYPE))

    def _enrich_deployed_flow(self, deployed_flow: DeployedFlow):
        raise NotImplementedError
