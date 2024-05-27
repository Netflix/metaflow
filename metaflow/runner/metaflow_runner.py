import os
import sys
import tempfile
import time
from typing import Dict, Iterator, Optional, Tuple

from metaflow import Run, metadata

from .subprocess_manager import CommandManager, SubprocessManager


def clear_and_set_os_environ(env: Dict):
    os.environ.clear()
    os.environ.update(env)


def read_from_file_when_ready(file_path: str, timeout: float = 5):
    start_time = time.time()
    with open(file_path, "r", encoding="utf-8") as file_pointer:
        content = file_pointer.read()
        while not content:
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    "Timeout while waiting for file content from '%s'" % file_path
                )
            time.sleep(0.1)
            content = file_pointer.read()
        return content


class ExecutingRun(object):
    """
    An object that encapsulates both the:
        - CommandManager Object (that has ability to stream logs,
          kill the underlying process, etc.)
        - Metaflow's Run object

    This is a user facing object exposing methods and properties for:
        - waiting (with an optional timeout and optional streaming of logs)
        - current snapshot of stdout
        - current snapshot of stderr
        - ability to stream logs
    """

    def __init__(
        self, runner: "Runner", command_obj: CommandManager, run_obj: Run
    ) -> None:
        """
        Create a new ExecutingRun -- this should not be done by the user directly but
        instead user Runner.run()

        Parameters
        ----------
        runner : Runner
            Parent runner for this run.
        command_obj : CommandManager
            CommandManager containing the subprocess executing this run.
        run_obj : Run
            Run object corresponding to this run.
        """
        self.runner = runner
        self.command_obj = command_obj
        self.run = run_obj

    def __enter__(self) -> "ExecutingRun":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.runner.__exit__(exc_type, exc_value, traceback)

    async def wait(
        self, timeout: Optional[float] = None, stream: Optional[str] = None
    ) -> "ExecutingRun":
        """
        Wait for this run to finish, optionally with a timeout
        and optionally streaming its output.

        Parameters
        ----------
        timeout : Optional[float], default None
            The maximum time to wait for the run to finish.
            If the timeout is reached, the run is terminated
        stream : Optional[str], default None
            If specified, the specified stream is printed to stdout. `stream` can
            be one of `stdout` or `stderr`.

        Returns
        -------
        ExecutingRun
            This object
        """
        await self.command_obj.wait(timeout, stream)
        return self

    @property
    def returncode(self) -> Optional[int]:
        """
        Gets the returncode of the underlying subprocess that is responsible
        for executing the run.

        Returns
        -------
        Optional[int]
            The returncode for the subprocess that executes the run.
            (None if process is still running)
        """
        return self.command_obj.process.returncode

    @property
    def status(self) -> str:
        """
        Returns the status of the underlying subprocess that is responsible
        for executing the run.

        Returns
        -------
        str
            The current status of the run (one of 'running', 'failed', 'successful').
        """
        if self.command_obj.process.returncode is None:
            return "running"
        elif self.command_obj.process.returncode != 0:
            return "failed"
        else:
            return "successful"

    @property
    def stdout(self) -> str:
        """
        Returns the current stdout of the run. If the run is finished, this will
        contain the entire stdout output but otherwise, it will contain the
        stdout up until this point.

        Returns
        -------
        str
            The current snapshot of stdout.
        """
        with open(
            self.command_obj.log_files.get("stdout"), "r", encoding="utf-8"
        ) as fp:
            return fp.read()

    @property
    def stderr(self) -> str:
        """
        Returns the current stderr of the run. If the run is finished, this will
        contain the entire stderr output but otherwise, it will contain the
        stderr up until this point.

        Returns
        -------
        str
            The current snapshot of stderr.
        """
        with open(
            self.command_obj.log_files.get("stderr"), "r", encoding="utf-8"
        ) as fp:
            return fp.read()

    async def stream_log(
        self, stream: str, position: Optional[int] = None
    ) -> Iterator[Tuple[int, str]]:
        """
        Stream logs from the subprocess line by line.

        Parameters
        ----------
        stream : str
            The stream to stream logs from. Can be one of "stdout" or "stderr".
        position : Optional[int], default None
            The position in the log file to start streaming from. If None, it starts
            from the beginning of the log file. This allows resuming streaming from
            a previously known position

        Yields
        ------
        Tuple[int, str]
            A tuple containing the position in the log file and the line read. The
            position returned can be used to feed into another `stream_logs` call
            for example.
        """
        async for position, line in self.command_obj.stream_log(stream, position):
            yield position, line


class Runner(object):
    def __init__(
        self,
        flow_file: str,
        show_output: bool = True,
        profile: Optional[str] = None,
        env: Optional[Dict] = None,
        cwd: Optional[str] = None,
        **kwargs
    ):
        """
        Metaflow's Runner API that presents a programmatic interface
        to run flows either synchronously or asynchronously. The class expects
        a path to the flow file along with optional top level parameters such as:
        metadata, environment, datastore, etc.

        The run() method expects run-level parameters. The object returned by the
        `run()` method is an `ExecutingRun` and you can:
          - access the client's `Run` object using `.run`
          - stream logs, ...

        Example:
            with Runner('../try.py', metadata="local") as runner:
                result = runner.run(alpha=5, tags=["abc", "def"], max_workers=5)
                print(result.run.finished)

        Parameters
        ----------
        flow_file : str
            Path to the flow file to run
        show_output : bool, default True
            Show the 'stdout' and 'stderr' to the console by default,
            Only applicable for synchronous 'run' and 'resume' functions.
        profile : Optional[str], default None
            Metaflow profile to use to run this run. If not specified, the default
            profile is used (or the one already set using `METAFLOW_PROFILE`)
        env : Optional[Dict], default None
            Additional environment variables to set for the Run. This overrides the
            environment set for this process.
        cwd : Optional[str], default None
            The directory to run the subprocess in; if not specified, the current
            directory is used.
        **kwargs : Any
            Additional arguments that you would pass to `python ./myflow.py` before
            the `run` command.
        """

        # these imports are required here and not at the top
        # since they interfere with the user defined Parameters
        # in the flow file, this is related to the ability of
        # importing 'Runner' directly i.e.
        #    from metaflow import Runner
        # This ability is made possible by the statement:
        # 'from .metaflow_runner import Runner' in '__init__.py'
        from metaflow.cli import start
        from metaflow.runner.click_api import MetaflowAPI

        self.flow_file = flow_file
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

    def __enter__(self) -> "Runner":
        return self

    async def __aenter__(self) -> "Runner":
        return self

    def __get_executing_run(self, tfp_runner_attribute, command_obj):
        # When two 'Runner' executions are done sequentially i.e. one after the other
        # the 2nd run kinda uses the 1st run's previously set metadata and
        # environment variables.

        # It is thus necessary to set them to correct values before we return
        # the Run object.
        try:
            # Set the environment variables to what they were before the run executed.
            clear_and_set_os_environ(self.old_env)

            # Set the correct metadata from the runner_attribute file corresponding to this run.
            content = read_from_file_when_ready(tfp_runner_attribute.name, timeout=10)
            metadata_for_flow, pathspec = content.rsplit(":", maxsplit=1)
            metadata(metadata_for_flow)
            run_object = Run(pathspec, _namespace_check=False)
            return ExecutingRun(self, command_obj, run_object)
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

    def run(self, **kwargs) -> ExecutingRun:
        """
        Synchronous execution of the run. This method will *block* until
        the run has completed execution.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python ./myflow.py` after
            the `run` command.

        Returns
        -------
        ExecutingRun
            ExecutingRun object for this run.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )
            command = self.api(**self.top_level_kwargs).run(
                runner_attribute_file=tfp_runner_attribute.name, **kwargs
            )

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )
            command_obj = self.spm.get(pid)

            return self.__get_executing_run(tfp_runner_attribute, command_obj)

    def resume(self, **kwargs):
        """
        Synchronous resume execution of the run.
        This method will *block* until the resumed run has completed execution.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python ./myflow.py` after
            the `resume` command.

        Returns
        -------
        ExecutingRun
            ExecutingRun object for this resumed run.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )
            command = self.api(**self.top_level_kwargs).resume(
                runner_attribute_file=tfp_runner_attribute.name, **kwargs
            )

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )
            command_obj = self.spm.get(pid)

            return self.__get_executing_run(tfp_runner_attribute, command_obj)

    async def async_run(self, **kwargs) -> ExecutingRun:
        """
        Asynchronous execution of the run. This method will return as soon as the
        run has launched.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python ./myflow.py` after
            the `run` command.

        Returns
        -------
        ExecutingRun
            ExecutingRun object for this run.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )
            command = self.api(**self.top_level_kwargs).run(
                runner_attribute_file=tfp_runner_attribute.name, **kwargs
            )

            pid = await self.spm.async_run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
            )
            command_obj = self.spm.get(pid)

            return self.__get_executing_run(tfp_runner_attribute, command_obj)

    async def async_resume(self, **kwargs):
        """
        Asynchronous resume execution of the run.
        This method will return as soon as the resume has launched.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python ./myflow.py` after
            the `resume` command.

        Returns
        -------
        ExecutingRun
            ExecutingRun object for this resumed run.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )
            command = self.api(**self.top_level_kwargs).resume(
                runner_attribute_file=tfp_runner_attribute.name, **kwargs
            )

            pid = await self.spm.async_run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
            )
            command_obj = self.spm.get(pid)

            return self.__get_executing_run(tfp_runner_attribute, command_obj)

    def __exit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    def cleanup(self):
        self.spm.cleanup()
