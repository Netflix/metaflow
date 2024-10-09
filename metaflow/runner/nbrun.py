import os
import tempfile
from typing import Dict, Optional

from metaflow import Runner
from metaflow.runner.utils import get_current_cell, format_flowfile


class NBRunnerInitializationError(Exception):
    """Custom exception for errors during NBRunner initialization."""

    pass


class NBRunner(object):
    """
    A  wrapper over `Runner` for executing flows defined in a Jupyter
    notebook cell.

    Instantiate this class on the last line of a notebook cell where
    a `flow` is defined. In contrast to `Runner`, this class is not
    meant to be used in a context manager. Instead, use a blocking helper
    function like `nbrun` (which calls `cleanup()` internally) or call
    `cleanup()` explictly when using non-blocking APIs.

    ```python
    run = NBRunner(FlowName).nbrun()
    ```

    Parameters
    ----------
    flow : FlowSpec
        Flow defined in the same cell
    show_output : bool, default True
        Show the 'stdout' and 'stderr' to the console by default,
        Only applicable for synchronous 'run' and 'resume' functions.
    profile : Optional[str], default None
        Metaflow profile to use to run this run. If not specified, the default
        profile is used (or the one already set using `METAFLOW_PROFILE`)
    env : Optional[Dict], default None
        Additional environment variables to set for the Run. This overrides the
        environment set for this process.
    base_dir : Optional[str], default None
        The directory to run the subprocess in; if not specified, the current
        working directory is used.
    file_read_timeout : int, default 3600
        The timeout until which we try to read the runner attribute file.
    **kwargs : Any
        Additional arguments that you would pass to `python myflow.py` before
        the `run` command.

    """

    def __init__(
        self,
        flow,
        show_output: bool = True,
        profile: Optional[str] = None,
        env: Optional[Dict] = None,
        base_dir: Optional[str] = None,
        file_read_timeout: int = 3600,
        **kwargs,
    ):
        try:
            from IPython import get_ipython

            ipython = get_ipython()
        except ModuleNotFoundError:
            raise NBRunnerInitializationError(
                "'NBRunner' requires an interactive Python environment (such as Jupyter)"
            )

        self.cell = get_current_cell(ipython)
        self.flow = flow
        self.show_output = show_output

        self.env_vars = os.environ.copy()
        self.env_vars.update(env or {})
        # clears the Jupyter parent process ID environment variable
        # prevents server from interfering with Metaflow
        self.env_vars.update({"JPY_PARENT_PID": ""})
        if profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.base_dir = base_dir if base_dir is not None else os.getcwd()
        self.file_read_timeout = file_read_timeout

        if not self.cell:
            raise ValueError("Couldn't find a cell.")

        self.tmp_flow_file = tempfile.NamedTemporaryFile(
            prefix=self.flow.__name__,
            suffix=".py",
            mode="w",
            dir=self.base_dir,
            delete=False,
        )

        self.tmp_flow_file.write(format_flowfile(self.cell))
        self.tmp_flow_file.flush()
        self.tmp_flow_file.close()

        self.runner = Runner(
            flow_file=self.tmp_flow_file.name,
            show_output=self.show_output,
            profile=profile,
            env=self.env_vars,
            cwd=self.base_dir,
            file_read_timeout=self.file_read_timeout,
            **kwargs,
        )

    def nbrun(self, **kwargs):
        """
        Blocking execution of the run. This method will wait until
        the run has completed execution.

        Note that in contrast to `run`, this method returns a
        `metaflow.Run` object directly and calls `cleanup()` internally
        to support a common notebook pattern of executing a flow and
        retrieving its results immediately.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python myflow.py` after
            the `run` command, in particular, any parameters accepted by the flow.

        Returns
        -------
        Run
            A `metaflow.Run` object representing the finished run.
        """
        result = self.runner.run(**kwargs)
        self.cleanup()
        return result.run

    def nbresume(self, **kwargs):
        """
        Blocking resuming of a run. This method will wait until
        the resumed run has completed execution.

        Note that in contrast to `resume`, this method returns a
        `metaflow.Run` object directly and calls `cleanup()` internally
        to support a common notebook pattern of executing a flow and
        retrieving its results immediately.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments that you would pass to `python myflow.py` after
            the `resume` command.

        Returns
        -------
        Run
            A `metaflow.Run` object representing the resumed run.
        """

        result = self.runner.resume(**kwargs)
        self.cleanup()
        return result.run

    def run(self, **kwargs):
        """
        Runs the flow.
        """
        return self.runner.run(**kwargs)

    def resume(self, **kwargs):
        """
        Resumes the flow.
        """
        return self.runner.resume(**kwargs)

    async def async_run(self, **kwargs):
        """
        Non-blocking execution of the run. This method will return as soon as the
        run has launched. This method is equivalent to `Runner.async_run`.

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
        return await self.runner.async_run(**kwargs)

    async def async_resume(self, **kwargs):
        """
        Non-blocking execution of the run. This method will return as soon as the
        run has launched. This method is equivalent to `Runner.async_resume`.

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
        return await self.runner.async_resume(**kwargs)

    def cleanup(self):
        """
        Delete any temporary files created during execution.

        Call this method after using `async_run` or `async_resume`. You don't
        have to call this after `nbrun` or `nbresume`.
        """
        os.remove(self.tmp_flow_file.name)
        self.runner.cleanup()
