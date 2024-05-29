import os
import sys
import tempfile
from typing import Optional, Dict

from metaflow import Run, metadata
from metaflow.exception import MetaflowNotFound
from metaflow.runner.subprocess_manager import CommandManager, SubprocessManager
from metaflow.runner.utils import clear_and_set_os_environ, read_from_file_when_ready


def get_lower_level_sfn_group(api, top_level_kwargs: Dict, name: Optional[str]):
    if name is None:
        return getattr(api(**top_level_kwargs), "step-functions")()
    return getattr(api(**top_level_kwargs), "step-functions")(name=name)


class StepFunctionsExecutingRun(object):
    def __init__(
        self,
        workflows_template_obj: "StepFunctionsTemplate",
        runner_attribute_file_content: str,
    ):
        self.workflows_template_obj = workflows_template_obj
        self.runner = self.workflows_template_obj.runner
        self.metadata_for_flow, self.pathspec = runner_attribute_file_content.rsplit(
            ":", maxsplit=1
        )

    @property
    def run(self):
        clear_and_set_os_environ(self.runner.old_env)
        metadata(self.metadata_for_flow)

        try:
            return Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            raise MetaflowNotFound(
                "Run object not available yet, Please try again in a bit.."
            )

    @property
    def status(self):
        raise NotImplementedError

    def terminate(self, **kwargs):
        _, run_id = self.pathspec.split("/")
        command = get_lower_level_sfn_group(
            self.runner.api, self.runner.top_level_kwargs, self.runner.name
        ).terminate(run_id=run_id, **kwargs)

        pid = self.runner.spm.run_command(
            [sys.executable, *command],
            env=self.runner.env_vars,
            cwd=self.runner.cwd,
            show_output=self.runner.show_output,
        )

        command_obj = self.runner.spm.get(pid)
        return command_obj.process.returncode == 0


class StepFunctionsTemplate(object):
    def __init__(
        self,
        runner: "StepFunctionsRunner",
    ):
        self.runner = runner

    @staticmethod
    def from_deployment(name):
        # TODO: get the StepFunctionsTemplate object somehow from already deployed step-function, referenced by name
        raise NotImplementedError

    @property
    def production_token(self):
        # TODO: how to get this?
        raise NotImplementedError

    def __get_executing_sfn(self, tfp_runner_attribute, command_obj: CommandManager):
        try:
            content = read_from_file_when_ready(tfp_runner_attribute.name, timeout=10)
            return StepFunctionsExecutingRun(self, content)
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

    def trigger(self, **kwargs):
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )
            command = get_lower_level_sfn_group(
                self.runner.api, self.runner.top_level_kwargs, self.runner.name
            ).trigger(runner_attribute_file=tfp_runner_attribute.name, **kwargs)

            pid = self.runner.spm.run_command(
                [sys.executable, *command],
                env=self.runner.env_vars,
                cwd=self.runner.cwd,
                show_output=self.runner.show_output,
            )

            command_obj = self.runner.spm.get(pid)
            return self.__get_executing_sfn(tfp_runner_attribute, command_obj)


class StepFunctionsRunner(object):
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

        # TODO: if we don't supply it, it should default to flow name..
        # which it does internally behind the scenes in CLI, but that isn't reflected here.
        # This is so that we can use StepFunctionsTemplate.from_deployment(name=StepFunctionsRunner("../try.py").name)
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

    def __enter__(self) -> "StepFunctionsRunner":
        return self

    def create(self, **kwargs):
        command = get_lower_level_sfn_group(
            self.api, self.top_level_kwargs, self.name
        ).create(**kwargs)
        pid = self.spm.run_command(
            [sys.executable, *command],
            env=self.env_vars,
            cwd=self.cwd,
            show_output=self.show_output,
        )
        command_obj = self.spm.get(pid)
        if command_obj.process.returncode == 0:
            return StepFunctionsTemplate(runner=self)
        raise Exception("Error deploying %s to Step Functions" % self.flow_file)

    def __exit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    def cleanup(self):
        self.spm.cleanup()


if __name__ == "__main__":
    import time

    ar = StepFunctionsRunner("../try.py")
    print(ar.name)
    ar_obj = ar.create()
    result = ar_obj.trigger(alpha=300)
    # print("aaa", result.status)
    while True:
        try:
            print(result.run)
            break  # Exit the loop if the run object is found
        except MetaflowNotFound:
            print("didn't get the run object yet...")
            time.sleep(5)  # Wait for 5 seconds before retrying
    print(result.run.id)
    # print("bbb", result.status)
    time.sleep(120)
    print(result.terminate())
