import os
import subprocess

import metaflow
from metaflow.cmd.configure_cmd import echo
from metaflow.exception import MetaflowException
from metaflow.linters.linter_interface import LinterWrapperInterface

try:
    from ruff.__main__ import find_ruff_bin
except ImportError:
    echo("Package `ruff` is not installed. Please install it before proceeding")
except Exception as e:
    raise MetaflowException(e)


from metaflow.cmd.configure_cmd import echo
from metaflow.linters.lint import LintWarn, FlowLinter

try:
    from StringIO import StringIO
except ImportError:
    echo("unable to import StringIO, possibly not installed. Resorting to `io`", fg="yellow")
    from io import StringIO

from metaflow.exception import MetaflowException
from metaflow.extension_support import get_aliased_modules


class RuffLintWarn(LintWarn):
    headline = "Ruff is not happy"


class RuffWrapper(LinterWrapperInterface):
    def __init__(self):
        super().__init__()
        try:
            self._binary_path = os.fsdecode(find_ruff_bin())
        except Exception as e:
            raise MetaflowException(e)

    def lint(self, file_path: str, warnings=None, lint_config=None, logger=None) -> None:
        ruff_toml_path = os.path.join('/'.join(metaflow.__file__.split("/")[:-1]), "linters/ruff/ruff.toml")
        cmd = ['ruff', 'format', file_path, '--config', ruff_toml_path]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        while True:
            output = process.stdout.readline()
            if output == "" and process.poll() is not None:
                break
            if output:
                logger(output, fg='magenta')
        return_code = process.poll()
        if return_code != 0:
            # Print the stderr in red
            for err in process.stderr:
                logger(err, fg='bright_red')
