import os
import tempfile
from typing import Dict, Optional

from metaflow import Deployer
from metaflow.runner.utils import get_current_cell, format_flowfile


class NBDeployerInitializationError(Exception):
    """Custom exception for errors during NBDeployer initialization."""

    pass


class NBDeployer(object):
    """
    A  wrapper over `Deployer` for deploying flows defined in a Jupyter
    notebook cell.

    Instantiate this class on the last line of a notebook cell where
    a `flow` is defined. In contrast to `Deployer`, this class is not
    meant to be used in a context manager.

    ```python
    deployer = NBDeployer(FlowName)
    ar = deployer.argo_workflows(name="madhur")
    ar_obj = ar.create()
    result = ar_obj.trigger(alpha=300)
    print(result.status)
    print(result.run)
    result.terminate()
    ```

    Parameters
    ----------
    flow : FlowSpec
        Flow defined in the same cell
    show_output : bool, default True
        Show the 'stdout' and 'stderr' to the console by default,
    profile : str, optional, default None
        Metaflow profile to use to deploy this run. If not specified, the default
        profile is used (or the one already set using `METAFLOW_PROFILE`)
    env : Dict[str, str], optional, default None
        Additional environment variables to set. This overrides the
        environment set for this process.
    base_dir : str, optional, default None
        The directory to run the subprocess in; if not specified, the current
        working directory is used.
    file_read_timeout : int, default 3600
        The timeout until which we try to read the deployer attribute file (in seconds).
    **kwargs : Any
        Additional arguments that you would pass to `python myflow.py` i.e. options
        listed in `python myflow.py --help`

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
        except ModuleNotFoundError as e:
            raise NBDeployerInitializationError(
                "'NBDeployer' requires an interactive Python environment "
                "(such as Jupyter)"
            ) from e

        self.cell = get_current_cell(ipython)
        self.flow = flow
        self.show_output = show_output
        self.profile = profile
        self.env = env
        self.cwd = base_dir if base_dir is not None else os.getcwd()
        self.file_read_timeout = file_read_timeout
        self.top_level_kwargs = kwargs

        self.env_vars = os.environ.copy()
        self.env_vars.update(env or {})
        # clears the Jupyter parent process ID environment variable
        # prevents server from interfering with Metaflow
        self.env_vars.update({"JPY_PARENT_PID": ""})

        if self.profile:
            self.env_vars["METAFLOW_PROFILE"] = self.profile

        if not self.cell:
            raise ValueError("Couldn't find a cell.")

        self.tmp_flow_file = tempfile.NamedTemporaryFile(
            prefix=self.flow.__name__,
            suffix=".py",
            mode="w",
            dir=self.cwd,
            delete=False,
        )

        self.tmp_flow_file.write(format_flowfile(self.cell))
        self.tmp_flow_file.flush()
        self.tmp_flow_file.close()

        self.flow_file = self.tmp_flow_file.name

        self.deployer = Deployer(
            flow_file=self.flow_file,
            show_output=self.show_output,
            profile=self.profile,
            env=self.env_vars,
            cwd=self.cwd,
            file_read_timeout=self.file_read_timeout,
            **kwargs,
        )

    def __getattr__(self, name):
        """
        Forward all attribute access to the underlying `Deployer` instance.
        """
        return getattr(self.deployer, name)

    def cleanup(self):
        """
        Delete any temporary files created during execution.
        """
        os.remove(self.flow_file)
