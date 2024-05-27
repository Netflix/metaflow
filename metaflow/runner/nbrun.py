import ast
import os
import tempfile
from typing import Dict, Optional

from metaflow import Runner

DEFAULT_DIR = tempfile.gettempdir()


class NBRunnerInitializationError(Exception):
    """Custom exception for errors during NBRunner initialization."""

    pass


def get_current_cell(ipython):
    if ipython:
        return ipython.history_manager.input_hist_raw[-1]
    return None


def format_flowfile(cell):
    """
    Formats the given cell content to create a valid Python script that can be executed as a Metaflow flow.
    """
    flowspec = [
        x
        for x in ast.parse(cell).body
        if isinstance(x, ast.ClassDef) and any(b.id == "FlowSpec" for b in x.bases)
    ]

    if not flowspec:
        raise ModuleNotFoundError(
            "The cell doesn't contain any class that inherits from 'FlowSpec'"
        )

    lines = cell.splitlines()[: flowspec[0].end_lineno]
    lines += ["if __name__ == '__main__':", f"    {flowspec[0].name}()"]
    return "\n".join(lines)


class NBRunner(object):
    """
    A class to run Metaflow flows from Jupyter notebook cells.
    """

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
            raise NBRunnerInitializationError(
                "'NBRunner' requires an interactive Python environment (such as Jupyter)"
            )

        self.cell = get_current_cell(ipython)
        self.flow = flow
        self.show_output = show_output

        self.env_vars = os.environ.copy()
        self.env_vars.update(env or {})
        self.env_vars.update({"JPY_PARENT_PID": ""})
        if profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.base_dir = base_dir

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
            **kwargs,
        )

    def nbrun(self, **kwargs):
        result = self.runner.run(**kwargs)
        self.cleanup()
        return result.run

    def nbresume(self, **kwargs):
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
        Asynchronously runs the flow.
        """
        return await self.runner.async_run(**kwargs)

    async def async_resume(self, **kwargs):
        """
        Asynchronously resumes the flow.
        """
        return await self.runner.async_resume(**kwargs)

    def cleanup(self):
        os.remove(self.tmp_flow_file.name)
        self.runner.cleanup()
