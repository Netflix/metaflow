import ast
import shutil
import tempfile
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

    def __init__(self, flow):
        self.cell = get_current_cell()
        self.flow = flow
        self.temp_dir = tempfile.mkdtemp()

        if not self.cell:
            raise ValueError("Couldn't find the cell which contains 'FlowSpec'")

        self.tmp_flow_file = tempfile.NamedTemporaryFile(
            prefix=self.flow.__name__,
            suffix=".py",
            mode="w",
            dir=self.temp_dir,
            delete=False,
        )

        self.tmp_flow_file.write(format_flowfile(self.cell))
        self.tmp_flow_file.flush()
        self.tmp_flow_file.close()

        self.runner = Runner(self.tmp_flow_file.name, env={"JPY_PARENT_PID": ""})

    def cleanup(self):
        """
        Cleans up the temporary directory used to store the flow script.
        """
        shutil.rmtree(self.temp_dir, ignore_errors=True)

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
