import os
import tempfile
from typing import Dict, Optional

from metaflow import Deployer
from metaflow.runner.utils import get_current_cell, format_flowfile

DEFAULT_DIR = tempfile.gettempdir()


class NBDeployerInitializationError(Exception):
    """Custom exception for errors during NBDeployer initialization."""

    pass


class NBDeployer(object):
    def __init__(
        self,
        flow,
        show_output: bool = True,
        profile: Optional[str] = None,
        env: Optional[Dict] = None,
        base_dir: str = DEFAULT_DIR,
        **kwargs,
    ):
        try:
            from IPython import get_ipython

            ipython = get_ipython()
        except ModuleNotFoundError:
            raise NBDeployerInitializationError(
                "'NBDeployer' requires an interactive Python environment (such as Jupyter)"
            )

        self.cell = get_current_cell(ipython)
        self.flow = flow
        self.show_output = show_output
        self.profile = profile
        self.env = env
        self.cwd = base_dir
        self.top_level_kwargs = kwargs

        self.env_vars = os.environ.copy()
        self.env_vars.update(env or {})
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
            **kwargs,
        )

        from metaflow.plugins import DEPLOYER_IMPL_PROVIDERS

        for provider_class in DEPLOYER_IMPL_PROVIDERS:
            method_name = provider_class.TYPE.replace("-", "_")
            setattr(
                NBDeployer, method_name, self.deployer.make_function(provider_class)
            )
