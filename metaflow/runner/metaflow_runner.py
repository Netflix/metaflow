import importlib
import inspect
import os
import sys
import json

from typing import Dict, Iterator, Optional, Tuple

from metaflow import Run, Task

from metaflow.metaflow_config import CLICK_API_PROCESS_CONFIG

from metaflow.plugins import get_runner_cli

from .utils import (
    temporary_fifo,
    handle_timeout,
    async_handle_timeout,
    with_dir,
)
from .subprocess_manager import CommandManager, SubprocessManager


class ExecutingProcess(object):
    """
    This is a base class for `ExecutingRun` and `ExecutingTask` classes.
    The `ExecutingRun` and `ExecutingTask` classes are returned by methods
    in `Runner` and `NBRunner`, and they are subclasses of this class.

    The `ExecutingRun` class for instance contains a reference to a `metaflow.Run`
    object representing the currently executing or finished run, as well as the metadata
    related to the process.

    Similarly, the `ExecutingTask` class contains a reference to a `metaflow.Task`
    object representing the currently executing or finished task, as well as the metadata
    related to the process.

    This class or its subclasses are not meant to be instantiated directly. The class
    works as a context manager, allowing you to use a pattern like:

    ```python
    with Runner(...).run() as running:
        ...
    ```

    Note that you should use either this object as the context manager or `Runner`, not both
    in a nested manner.
    """

    def __init__(self, runner: "Runner", command_obj: CommandManager) -> None:
        """
        Create a new ExecutingRun -- this should not be done by the user directly but
        instead use Runner.run()

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

    def __enter__(self) -> "ExecutingProcess":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.runner.__exit__(exc_type, exc_value, traceback)

    async def wait(
        self, timeout: Optional[float] = None, stream: Optional[str] = None
    ) -> "ExecutingProcess":
        """
        Wait for this run to finish, optionally with a timeout
        and optionally streaming its output.

        Note that this method is asynchronous and needs to be `await`ed.

        Parameters
        ----------
        timeout : float, optional, default None
            The maximum time, in seconds, to wait for the run to finish.
            If the timeout is reached, the run is terminated. If not specified, wait
            forever.
        stream : str, optional, default None
            If specified, the specified stream is printed to stdout. `stream` can
            be one of `stdout` or `stderr`.

        Returns
        -------
        ExecutingProcess
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
        position : int, optional, default None
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


class ExecutingTask(ExecutingProcess):
    """
    This class contains a reference to a `metaflow.Task` object representing
    the currently executing or finished task, as well as metadata related
    to the process.
    `ExecutingTask` is returned by methods in `Runner` and `NBRunner`. It is not
    meant to be instantiated directly.
    This class works as a context manager, allowing you to use a pattern like
    ```python
    with Runner(...).spin() as running:
        ...
    ```
    Note that you should use either this object as the context manager or
    `Runner`, not both in a nested manner.
    """

    def __init__(
        self, runner: "Runner", command_obj: CommandManager, task_obj: Task
    ) -> None:
        """
        Create a new ExecutingTask -- this should not be done by the user directly but
        instead use Runner.spin()
        Parameters
        ----------
        runner : Runner
            Parent runner for this task.
        command_obj : CommandManager
            CommandManager containing the subprocess executing this task.
        task_obj : Task
            Task object corresponding to this task.
        """
        super().__init__(runner, command_obj)
        self.task = task_obj


class ExecutingRun(ExecutingProcess):
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
        instead use Runner.run()
        Parameters
        ----------
        runner : Runner
            Parent runner for this run.
        command_obj : CommandManager
            CommandManager containing the subprocess executing this run.
        run_obj : Run
            Run object corresponding to this run.
        """
        super().__init__(runner, command_obj)
        self.run = run_obj


class RunnerMeta(type):
    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)

        def _injected_method(subcommand_name, runner_subcommand):
            def f(self, *args, **kwargs):
                return runner_subcommand(self, *args, **kwargs)

            f.__doc__ = runner_subcommand.__init__.__doc__ or ""
            f.__name__ = subcommand_name
            sig = inspect.signature(runner_subcommand)
            # We take all the same parameters except replace the first with
            # simple "self"
            new_parameters = {}
            for name, param in sig.parameters.items():
                if new_parameters:
                    new_parameters[name] = param
                else:
                    new_parameters["self"] = inspect.Parameter(
                        "self", inspect.Parameter.POSITIONAL_OR_KEYWORD
                    )
            f.__signature__ = inspect.Signature(
                list(new_parameters.values()), return_annotation=runner_subcommand
            )

            return f

        for runner_subcommand in get_runner_cli():
            method_name = runner_subcommand.name.replace("-", "_")
            setattr(cls, method_name, _injected_method(method_name, runner_subcommand))

        return cls


class Runner(metaclass=RunnerMeta):
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
        Path to the flow file to run, relative to current directory.
    show_output : bool, default True
        Show the 'stdout' and 'stderr' to the console by default,
        Only applicable for synchronous 'run' and 'resume' functions.
    profile : str, optional, default None
        Metaflow profile to use to run this run. If not specified, the default
        profile is used (or the one already set using `METAFLOW_PROFILE`)
    env : Dict[str, str], optional, default None
        Additional environment variables to set for the Run. This overrides the
        environment set for this process.
    cwd : str, optional, default None
        The directory to run the subprocess in; if not specified, the current
        directory is used.
    file_read_timeout : int, default 3600
        The timeout until which we try to read the runner attribute file (in seconds).
    **kwargs : Any
        Additional arguments that you would pass to `python myflow.py` before
        the `run` command.
    """

    def __init__(
        self,
        flow_file: str,
        show_output: bool = True,
        profile: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        file_read_timeout: int = 3600,
        **kwargs,
    ):
        # these imports are required here and not at the top
        # since they interfere with the user defined Parameters
        # in the flow file, this is related to the ability of
        # importing 'Runner' directly i.e.
        #    from metaflow import Runner
        # This ability is made possible by the statement:
        # 'from .metaflow_runner import Runner' in '__init__.py'

        from metaflow.parameters import flow_context

        # Reload the CLI with an "empty" flow -- this will remove any configuration
        # and parameter options. They are re-added in from_cli (called below).
        to_reload = [
            "metaflow.cli",
            "metaflow.cli_components.run_cmds",
            "metaflow.cli_components.init_cmd",
        ]
        with flow_context(None):
            [
                importlib.reload(sys.modules[module])
                for module in to_reload
                if module in sys.modules
            ]

        from metaflow.cli import start
        from metaflow.runner.click_api import MetaflowAPI

        # Convert flow_file to absolute path if it's relative
        if not os.path.isabs(flow_file):
            self.flow_file = os.path.abspath(flow_file)
        else:
            self.flow_file = flow_file

        self.show_output = show_output

        self.env_vars = os.environ.copy()
        self.env_vars.update(env or {})
        if profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.cwd = cwd or os.getcwd()
        self.file_read_timeout = file_read_timeout
        self.spm = SubprocessManager()
        self.top_level_kwargs = kwargs
        self.api = MetaflowAPI.from_cli(self.flow_file, start)

    def __enter__(self) -> "Runner":
        return self

    async def __aenter__(self) -> "Runner":
        return self

    def __get_executing_run(self, attribute_file_fd, command_obj):
        content = handle_timeout(attribute_file_fd, command_obj, self.file_read_timeout)

        command_obj.sync_wait()

        content = json.loads(content)
        pathspec = "%s/%s" % (content.get("flow_name"), content.get("run_id"))

        # Set the correct metadata from the runner_attribute file corresponding to this run.
        metadata_for_flow = content.get("metadata")

        run_object = Run(
            pathspec, _namespace_check=False, _current_metadata=metadata_for_flow
        )
        return ExecutingRun(self, command_obj, run_object)

    async def __async_get_executing_run(self, attribute_file_fd, command_obj):
        content = await async_handle_timeout(
            attribute_file_fd, command_obj, self.file_read_timeout
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
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            if CLICK_API_PROCESS_CONFIG:
                with with_dir(self.cwd):
                    command = self.api(**self.top_level_kwargs).run(
                        runner_attribute_file=attribute_file_path, **kwargs
                    )
            else:
                command = self.api(**self.top_level_kwargs).run(
                    runner_attribute_file=attribute_file_path, **kwargs
                )

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )
            command_obj = self.spm.get(pid)

            return self.__get_executing_run(attribute_file_fd, command_obj)

    def __get_executing_task(self, attribute_file_fd, command_obj):
        content = handle_timeout(attribute_file_fd, command_obj, self.file_read_timeout)

        command_obj.sync_wait()

        content = json.loads(content)
        pathspec = f"{content.get('flow_name')}/{content.get('run_id')}/{content.get('step_name')}/{content.get('task_id')}"

        # Set the correct metadata from the runner_attribute file corresponding to this run.
        metadata_for_flow = content.get("metadata")

        task_object = Task(
            pathspec, _namespace_check=False, _current_metadata=metadata_for_flow
        )
        return ExecutingTask(self, command_obj, task_object)

    async def __async_get_executing_task(self, attribute_file_fd, command_obj):
        content = await async_handle_timeout(
            attribute_file_fd, command_obj, self.file_read_timeout
        )
        content = json.loads(content)
        pathspec = f"{content.get('flow_name')}/{content.get('run_id')}/{content.get('step_name')}/{content.get('task_id')}"

        # Set the correct metadata from the runner_attribute file corresponding to this run.
        metadata_for_flow = content.get("metadata")

        task_object = Task(
            pathspec, _namespace_check=False, _current_metadata=metadata_for_flow
        )
        return ExecutingTask(self, command_obj, task_object)

    def spin(self, pathspec, **kwargs) -> ExecutingTask:
        """
        Blocking spin execution of the run.
        This method will wait until the spun run has completed execution.
        Parameters
        ----------
        pathspec : str
            The pathspec of the step/task to spin.
        **kwargs : Any
            Additional arguments that you would pass to `python ./myflow.py` after
            the `spin` command.
        Returns
        -------
        ExecutingTask
            ExecutingTask containing the results of the spun task.
        """
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            if CLICK_API_PROCESS_CONFIG:
                with with_dir(self.cwd):
                    command = self.api(**self.top_level_kwargs).spin(
                        pathspec=pathspec,
                        runner_attribute_file=attribute_file_path,
                        **kwargs,
                    )
            else:
                command = self.api(**self.top_level_kwargs).spin(
                    pathspec=pathspec,
                    runner_attribute_file=attribute_file_path,
                    **kwargs,
                )

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )
            command_obj = self.spm.get(pid)

            return self.__get_executing_task(attribute_file_fd, command_obj)

    def resume(self, **kwargs) -> ExecutingRun:
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
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            if CLICK_API_PROCESS_CONFIG:
                with with_dir(self.cwd):
                    command = self.api(**self.top_level_kwargs).resume(
                        runner_attribute_file=attribute_file_path, **kwargs
                    )
            else:
                command = self.api(**self.top_level_kwargs).resume(
                    runner_attribute_file=attribute_file_path, **kwargs
                )

            pid = self.spm.run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
                show_output=self.show_output,
            )
            command_obj = self.spm.get(pid)

            return self.__get_executing_run(attribute_file_fd, command_obj)

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
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            if CLICK_API_PROCESS_CONFIG:
                with with_dir(self.cwd):
                    command = self.api(**self.top_level_kwargs).run(
                        runner_attribute_file=attribute_file_path, **kwargs
                    )
            else:
                command = self.api(**self.top_level_kwargs).run(
                    runner_attribute_file=attribute_file_path, **kwargs
                )

            pid = await self.spm.async_run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
            )
            command_obj = self.spm.get(pid)

            return await self.__async_get_executing_run(attribute_file_fd, command_obj)

    async def async_resume(self, **kwargs) -> ExecutingRun:
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
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            if CLICK_API_PROCESS_CONFIG:
                with with_dir(self.cwd):
                    command = self.api(**self.top_level_kwargs).resume(
                        runner_attribute_file=attribute_file_path, **kwargs
                    )
            else:
                command = self.api(**self.top_level_kwargs).resume(
                    runner_attribute_file=attribute_file_path, **kwargs
                )

            pid = await self.spm.async_run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
            )
            command_obj = self.spm.get(pid)

            return await self.__async_get_executing_run(attribute_file_fd, command_obj)

    async def async_spin(self, pathspec, **kwargs) -> ExecutingTask:
        """
        Non-blocking spin execution of the run.
        This method will return as soon as the spun task has launched.

        Note that this method is asynchronous and needs to be `await`ed.

        Parameters
        ----------
        pathspec : str
            The pathspec of the step/task to spin.
        **kwargs : Any
            Additional arguments that you would pass to `python ./myflow.py` after
            the `spin` command.

        Returns
        -------
        ExecutingTask
            ExecutingTask representing the spun task that was started.
        """
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            if CLICK_API_PROCESS_CONFIG:
                with with_dir(self.cwd):
                    command = self.api(**self.top_level_kwargs).spin(
                        pathspec=pathspec,
                        runner_attribute_file=attribute_file_path,
                        **kwargs,
                    )
            else:
                command = self.api(**self.top_level_kwargs).spin(
                    pathspec=pathspec,
                    runner_attribute_file=attribute_file_path,
                    **kwargs,
                )

            pid = await self.spm.async_run_command(
                [sys.executable, *command],
                env=self.env_vars,
                cwd=self.cwd,
            )
            command_obj = self.spm.get(pid)

            return await self.__async_get_executing_task(attribute_file_fd, command_obj)

    def __exit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    def cleanup(self):
        """
        Delete any temporary files created during execution.
        """
        self.spm.cleanup()
