import inspect
import logging
import os
import subprocess
import sys

from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

logger = logging.getLogger(__name__)


class RuffException(MetaflowException):
    headline = "Ruff is not happy"


class RuffFlowDecorator(FlowDecorator):

    name = "ruff"

    defaults = {"options": None}

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        ruff_options = self.attributes.get("options")

        echo(
            "Running ruff...",
            fg="magenta",
            highlight="green",
        )

        error = False

        fname = inspect.getfile(flow.__class__)
        stdout, stderr = Ruff().run(fname, ruff_options)

        if stderr:
            echo(stderr, fg="green", bold=True, indent=True)

        if stdout:
            echo(
                stdout,
                fg="red",
                bold=True,
                indent=True,
            )
            error = True

        if error:
            raise RuffException(
                "Fix ruff warnings listed above or remove the @ruff Flow decorator."
                " If you specified the `--fix` option,"
                " this may have been solved automatically"
            )
        else:
            echo("Ruff is happy!", fg="green", bold=True, indent=True)


class Ruff(object):
    def __init__(self, fname):
        self._fname = fname

    def run(self, options=None):
        if options is None:
            options = []

        ruff = [sys.executable, "-m", "ruff"]
        args = ["-v", *options, self.fname]
        cmd = [*ruff, *args]

        p = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        stdout, stderr = p.communicate()

        return stdout, stderr


if __name__ == "__main__":
    if len(sys.argv) < 2:
        fname = __file__
    else:
        fname = sys.argv[1]

    print(fname)
    print(os.getcwd())
    for type, line in Ruff(fname).run():
        print(type, line)
