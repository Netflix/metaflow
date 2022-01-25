# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import contextlib
import sys
import traceback
from datetime import datetime
from pathlib import Path

from metaflow._vendor.pylint.config import PYLINT_HOME
from metaflow._vendor.pylint.lint.expand_modules import get_python_path


class ArgumentPreprocessingError(Exception):
    """Raised if an error occurs during argument preprocessing."""


def prepare_crash_report(ex: Exception, filepath: str, crash_file_path: str) -> Path:
    issue_template_path = (
        Path(PYLINT_HOME) / datetime.now().strftime(str(crash_file_path))
    ).resolve()
    with open(filepath, encoding="utf8") as f:
        file_content = f.read()
    template = ""
    if not issue_template_path.exists():
        template = """\
First, please verify that the bug is not already filled:
https://github.com/PyCQA/pylint/issues/

Then create a new crash issue:
https://github.com/PyCQA/pylint/issues/new?assignees=&labels=crash%2Cneeds+triage&template=BUG-REPORT.yml

"""
    template += f"""\

Issue title:
Crash ``{ex}`` (if possible, be more specific about what made pylint crash)
Content:
When parsing the following file:

<!--
 If sharing the code is not an option, please state so,
 but providing only the stacktrace would still be helpful.
 -->

```python
{file_content}
```

pylint crashed with a ``{ex.__class__.__name__}`` and with the following stacktrace:
```
"""
    try:
        with open(issue_template_path, "a", encoding="utf8") as f:
            f.write(template)
            traceback.print_exc(file=f)
            f.write("```\n")
    except FileNotFoundError:
        print(f"Can't write the issue template for the crash in {issue_template_path}.")
    return issue_template_path


def get_fatal_error_message(filepath: str, issue_template_path: Path) -> str:
    return (
        f"Fatal error while checking '{filepath}'. "
        f"Please open an issue in our bug tracker so we address this. "
        f"There is a pre-filled template that you can use in '{issue_template_path}'."
    )


def preprocess_options(args, search_for):
    """look for some options (keys of <search_for>) which have to be processed
    before others

    values of <search_for> are callback functions to call when the option is
    found
    """
    i = 0
    while i < len(args):
        arg = args[i]
        if not arg.startswith("--"):
            i += 1
        else:
            try:
                option, val = arg[2:].split("=", 1)
            except ValueError:
                option, val = arg[2:], None
            try:
                cb, takearg = search_for[option]
            except KeyError:
                i += 1
            else:
                del args[i]
                if takearg and val is None:
                    if i >= len(args) or args[i].startswith("-"):
                        msg = f"Option {option} expects a value"
                        raise ArgumentPreprocessingError(msg)
                    val = args[i]
                    del args[i]
                elif not takearg and val is not None:
                    msg = f"Option {option} doesn't expects a value"
                    raise ArgumentPreprocessingError(msg)
                cb(option, val)


def _patch_sys_path(args):
    original = list(sys.path)
    changes = []
    seen = set()
    for arg in args:
        path = get_python_path(arg)
        if path not in seen:
            changes.append(path)
            seen.add(path)

    sys.path[:] = changes + sys.path
    return original


@contextlib.contextmanager
def fix_import_path(args):
    """Prepare sys.path for running the linter checks.

    Within this context, each of the given arguments is importable.
    Paths are added to sys.path in corresponding order to the arguments.
    We avoid adding duplicate directories to sys.path.
    `sys.path` is reset to its original value upon exiting this context.
    """
    original = _patch_sys_path(args)
    try:
        yield
    finally:
        sys.path[:] = original
