import importlib
import os
import sys
import json
import tempfile

from typing import Dict, Iterator, Optional, Tuple

from metaflow import Run

from .utils import handle_timeout
from .subprocess_manager import CommandManager, SubprocessManager


class ExecutingRun(object):
    """
    This class contains a reference to a `metaflow.Run` object representing
    the currently executing or finished run, as well as metadata related
    to the process.

    `ExecutingRun` is returned by methods in `Runner` and `NBRunner`. It is not
    meant to be instantiated directly.

    This class works as a context manager, allowing you to use a pattern like
    ```python
    with Runner(...).run() as running:
        ...
    ```
    Note that you should use either this object as the context manager or
    `Runner`, not both in a nested manner.
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

        Note that this method is asynchronous and needs to be `await`ed.

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
            This object, allowing you to chain calls.
        """
        await self.command_obj.wait(timeout, stream)
        return self

    @property
    def returncode(self) -> Optional[int]:
        """
        Gets the return code of the underlying subprocess. A non-zero
        code indicates a failure, `None` a currently executing run.

        Returns
        -------
        Optional[int]
            The return code of the underlying subprocess.
        """
        return self.command_obj.process.returncode

    @property
    def status(self) -> str:
        """
        Returns the status of the underlying subprocess that is responsible
        for executing the run.

        The return value is one of the following strings:
        - `timeout` indicates that the run timed out.
        - `running` indicates a currently executing run.
        - `failed` indicates a failed run.
        - `successful` indicates a successful run.

        Returns
        -------
        str
            The current status of the run.
        """
        if self.command_obj.timeout:
            return "timeout"
        elif self.command_obj.process.returncode is None:
            return "running"
        elif self.command_obj.process.returncode != 0:
            return "failed"
        else:
            return "successful"

    @property
    def stdout(self) -> str:
        """
        Returns the current stdout of the run. If the run is finished, this will
        contain the entire stdout output. Otherwise, it will contain the
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
        contain the entire stderr output. Otherwise, it will contain the
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
        Asynchronous iterator to stream logs from the subprocess line by line.

        Note that this method is asynchronous and needs to be `await`ed.

        Parameters
        ----------
        stream : str
            The stream to stream logs from. Can be one of `stdout` or `stderr`.
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
    """
    Metaflow's Runner API that presents a programmatic interface
    to run flows and perform other operations either synchronously or asynchronously.
    The class expects a path to the flow file along with optional arguments
    that match top-level options on the command-line.

    This class works as a context manager, calling `cleanup()` to remove
    temporary files at exit.

    Example:
    ```python
    with Runner('slowflow.py', pylint=False) as runner:
        result = runner.run(alpha=5, tags=["abc", "def"], max_workers=5)
        print(result.run.finished)
    ```

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
    file_read_timeout : int, default 3600
        The timeout until which we try to read the runner attribute file.
    **kwargs : Any
        Additional arguments that you would pass to `python myflow.py` before
        the `run` command.
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
        # these imports are required here and not at the top
        # since they interfere with the user defined Parameters
        # in the flow file, this is related to the ability of
        # importing 'Runner' directly i.e.
        #    from metaflow import Runner
        # This ability is made possible by the statement:
        # 'from .metaflow_runner import Runner' in '__init__.py'

        if "metaflow.cli" in sys.modules:
            importlib.reload(sys.modules["metaflow.cli"])
        from metaflow.cli import start
        from metaflow.runner.click_api import MetaflowAPI

        self.flow_file = flow_file
        self.show_output = show_output

        self.env_vars = os.environ.copy()
        self.env_vars.update(env or {})
        if profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.cwd = cwd
        self.file_read_timeout = file_read_timeout
        self.spm = SubprocessManager()
        self.top_level_kwargs = kwargs
        self.api = MetaflowAPI.from_cli(self.flow_file, start)

    def __enter__(self) -> "Runner":
        return self

    async def __aenter__(self) -> "Runner":
        return self

    def __get_executing_run(self, tfp_runner_attribute, command_obj):
        content = handle_timeout(
            tfp_runner_attribute, command_obj, self.file_read_timeout
        )
        content = json.loads(content)
        pathspec = "%s/%s" % (content.get("flow_name"), content.get("run_id"))

        # Set the correct metadata from the runner_attribute file corresponding to this run.
        metadata_for_flow = content.get("metadata")

        run_object = Run(
            pathspec, _namespace_check=False, _current_metadata=metadata_for_flow
        )
        return ExecutingRun(self, command_obj, run_object)

    def run(self, **kwargs) -> ExecutingRun:
        """
        Blocking execution of the run. This method will wait until
        the run has completed execution.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python myflow.py` after
            the `run` command, in particular, any parameters accepted by the flow.

        Returns
        -------
        ExecutingRun
            ExecutingRun containing the results of the run.
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
        Blocking resume execution of the run.
        This method will wait until the resumed run has completed execution.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python ./myflow.py` after
            the `resume` command.

        Returns
        -------
        ExecutingRun
            ExecutingRun containing the results of the resumed run.
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
        Non-blocking execution of the run. This method will return as soon as the
        run has launched.

        Note that this method is asynchronous and needs to be `await`ed.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python myflow.py` after
            the `run` command, in particular, any parameters accepted by the flow.

        Returns
        -------
        ExecutingRun
            ExecutingRun representing the run that was started.
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
        Non-blocking resume execution of the run.
        This method will return as soon as the resume has launched.

        Note that this method is asynchronous and needs to be `await`ed.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python myflow.py` after
            the `resume` command.

        Returns
        -------
        ExecutingRun
            ExecutingRun representing the resumed run that was started.
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
        """
        Delete any temporary files created during execution.
        """
        self.spm.cleanup()
