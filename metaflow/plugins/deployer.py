import os
import sys
import json
import tempfile
from typing import Optional, Dict, ClassVar

from metaflow import Run, metadata
from metaflow.exception import MetaflowNotFound
from metaflow.runner.utils import clear_and_set_os_environ
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


def get_lower_level_group(api, top_level_kwargs: Dict, _type: str, name: Optional[str]):
    if _type is None:
        raise ValueError(
            "Deployer doesn't have a 'type' to target. Please use a sub-class of Deployer."
        )
    if name is None:
        return getattr(api(**top_level_kwargs), _type)()
    return getattr(api(**top_level_kwargs), _type)(name=name)


class TriggeredRun(object):
    def __init__(
        self,
        deployer: "Deployer",
        content: str,
    ):
        self.deployer = deployer
        content_json = json.loads(content)
        self.metadata_for_flow = content_json.get("metadata")
        self.pathspec = content_json.get("pathspec")
        self.deployer.name = content_json.get("name")

    @property
    def name(self):
        return self.deployer.name

    @property
    def status(self):
        raise NotImplementedError

    @property
    def run(self):
        clear_and_set_os_environ(self.deployer.old_env)
        metadata(self.metadata_for_flow)
        try:
            return Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None

    def terminate(self, **kwargs):
        _, run_id = self.pathspec.split("/")
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.type,
            self.deployer.name,
        ).terminate(run_id=run_id, **kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        return command_obj.process.returncode == 0


class DeployedFlow(object):
    def __init__(
        self,
        deployer: "Deployer",
    ):
        self.deployer = deployer

    @property
    def production_token(self):
        raise NotImplementedError

    def trigger(self, **kwargs) -> "TriggeredRun":
        return self.deployer.trigger(**kwargs)


class Deployer(object):
    type: ClassVar[Optional[str]] = None

    def __init__(
        self,
        flow_file: str,
        name: Optional[str] = None,
        show_output: bool = False,
        profile: Optional[str] = None,
        env: Optional[Dict] = None,
        cwd: Optional[str] = None,
        **kwargs
    ):
        from metaflow.cli import start
        from metaflow.runner.click_api import MetaflowAPI

        self.flow_file = flow_file

        self.name = name
        self.show_output = show_output

        self.old_env = os.environ.copy()
        self.env_vars = self.old_env.copy()
        self.env_vars.update(env or {})
        if profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.cwd = cwd
        self.spm = SubprocessManager()
        self.top_level_kwargs = kwargs
        self.api = MetaflowAPI.from_cli(self.flow_file, start)

    def __enter__(self) -> "Deployer":
        return self

    def create(self, **kwargs) -> "DeployedFlow":
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )

            command = get_lower_level_group(
                self.api, self.top_level_kwargs, self.type, self.name
            ).create(runner_attribute_file=tfp_runner_attribute.name, **kwargs)

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )

            command_obj = self.spm.get(pid)

            content = read_from_file_when_ready(tfp_runner_attribute.name, timeout=10)
            self.name = json.loads(content).get("name")

            return command_obj

    def deploy(self, **kwargs) -> "DeployedFlow":
        return self.create(**kwargs)

    def trigger(self, **kwargs) -> "TriggeredRun":
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )

            command = get_lower_level_group(
                self.api, self.top_level_kwargs, self.type, self.name
            ).trigger(runner_attribute_file=tfp_runner_attribute.name, **kwargs)

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )

            command_obj = self.spm.get(pid)
            content = handle_timeout(tfp_runner_attribute, command_obj)

            return content, command_obj

    def __exit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    def cleanup(self):
        self.spm.cleanup()
