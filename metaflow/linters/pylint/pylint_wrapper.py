import re
import sys

from metaflow.cmd.configure_cmd import echo, echo_always
from metaflow.exception import MetaflowException
from metaflow.linters.lint import LintWarn
from metaflow.linters.linter_interface import LinterWrapperInterface

try:
    from StringIO import StringIO
except ImportError:
    echo("unable to import StringIO, possibly not installed. Resorting to `io`", fg="yellow")
    from io import StringIO
except Exception as e:
    raise MetaflowException(e)

from metaflow.extension_support import get_aliased_modules


class PyLintWarn(LintWarn):
    headline = "Pylint is not happy"


class PyLintWrapper(LinterWrapperInterface):
    def __init__(self):
        super().__init__()
        try:
            from pylint.lint import Run

            self._run = Run
        except ImportError:
            self._run = None
        except Exception as e:
            raise MetaflowException(e)

    def has_linter(self):
        return self._run is not None

    def run(self, logger=None, warnings=False, fname=None, pylint_config=None):
        if pylint_config is None:
            pylint_config = []
        args = [fname]
        if not warnings:
            args.append("--errors-only")
        if pylint_config:
            args.extend(pylint_config)
        stdout = sys.stdout
        stderr = sys.stderr
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            pylint_is_happy = True
            pylint_exception_msg = ""
            self._run(args, None, False)
        except Exception as e:
            pylint_is_happy = False
            pylint_exception_msg = repr(e)
        output = sys.stdout.getvalue()
        sys.stdout = stdout
        sys.stderr = stderr

        warnings = False
        for line in self._filter_lines(output):
            logger(line, indent=True)
            warnings = True

        if warnings:
            raise PyLintWarn("*Fix Pylint warnings listed above or say --no-pylint.*")

        return pylint_is_happy, pylint_exception_msg

    def lint(self, file_path: str, warnings=None, lint_config=None, logger=None):
        if self.has_linter():
            pylint_is_happy, pylint_exception_msg = self.run(
                warnings=warnings,
                pylint_config=lint_config,
                logger=echo_always,
            )
            if pylint_is_happy:
                echo("Pylint is happy!", fg="green", bold=True, indent=True)
            else:
                echo(
                    "Pylint couldn't analyze your code.\n\tPylint exception: %s"
                    % pylint_exception_msg,
                    fg="red",
                    bold=True,
                    indent=True,
                )
                echo("Skipping Pylint checks.", fg="red", bold=True, indent=True)
        else:
            echo(
                "Pylint not found, so extra checks are disabled.",
                fg="green",
            )

    def _filter_lines(self, output):
        ext_aliases = get_aliased_modules()
        import_error_line = re.compile(r"Unable to import '([^']+)'")
        for line in output.splitlines():
            # Ignore headers
            if "***" in line:
                continue
            # Ignore complaints about decorators missing in the metaflow module.
            # Automatic generation of decorators confuses Pylint.
            if "(no-name-in-module)" in line:
                continue
            # Ignore things related to module aliasing in EXT_PKG
            if "E0401" in line:
                m = import_error_line.search(line)
                if m and any([m.group(1).startswith(alias) for alias in ext_aliases]):
                    continue
            # Ignore complaints related to dynamic and JSON-types parameters
            if "Instance of 'Parameter' has no" in line:
                continue
            # Ditto for IncludeFile
            if "Instance of 'IncludeFile' has no" in line:
                continue
            # Ditto for dynamically added properties in 'current'
            if "Instance of 'Current' has no" in line:
                continue
            # Ignore complaints of self.next not callable
            if "self.next is not callable" in line:
                continue
            yield line
