import ast
import os
import shutil
import tempfile
from typing import Dict, Optional

from metaflow import Runner

try:
    from IPython import get_ipython

    ipython = get_ipython()
except ModuleNotFoundError:
    print("'nbrun' requires an interactive python environment (such as Jupyter)")


def get_current_cell():
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
        profile: Optional[str] = None,
        env: Optional[Dict] = None,
        base_dir: Optional[str] = None,
        **kwargs,
    ):
        self.cell = get_current_cell()
        self.flow = flow

        self.env_vars = os.environ.copy()
        self.env_vars.update(env or {})
        self.env_vars.update({"JPY_PARENT_PID": ""})
        if profile:
            self.env_vars["METAFLOW_PROFILE"] = profile

        self.base_dir = base_dir

        if not self.cell:
            raise ValueError("Couldn't find a cell.")

        if self.base_dir is None:
            # for some reason, using this is much faster
            self.tempdir = tempfile.mkdtemp()
        else:
            self.tempdir = self.base_dir

        self.tmp_flow_file = tempfile.NamedTemporaryFile(
            prefix=self.flow.__name__,
            suffix=".py",
            mode="w",
            dir=self.tempdir,
            delete=False,
        )

        self.tmp_flow_file.write(format_flowfile(self.cell))
        self.tmp_flow_file.flush()
        self.tmp_flow_file.close()

        self.runner = Runner(
            flow_file=self.tmp_flow_file.name,
            profile=profile,
            env=self.env_vars,
            **kwargs,
        )

    def nbrun(self, **kwargs):
        result = self.runner.run(show_output=True, **kwargs)
        self.runner.spm.cleanup()
        return result.run

    def nbresume(self, **kwargs):
        result = self.runner.resume(show_output=True, **kwargs)
        self.runner.spm.cleanup()
        return result.run

    def cleanup(self):
        """Cleans up the temporary directory used to store the flow script"""
        if self.base_dir is None:
            shutil.rmtree(self.tempdir, ignore_errors=True)

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
