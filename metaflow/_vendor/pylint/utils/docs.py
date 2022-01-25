# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE
"""Various helper functions to create the docs of a linter object"""

import sys
from typing import TYPE_CHECKING, Dict, TextIO

from metaflow._vendor.pylint.constants import MAIN_CHECKER_NAME
from metaflow._vendor.pylint.utils.utils import get_rst_section, get_rst_title

if TYPE_CHECKING:
    from metaflow._vendor.pylint.lint.pylinter import PyLinter


def _get_checkers_infos(linter: "PyLinter") -> Dict[str, Dict]:
    """Get info from a checker and handle KeyError"""
    by_checker: Dict[str, Dict] = {}
    for checker in linter.get_checkers():
        name = checker.name
        if name != "master":
            try:
                by_checker[name]["checker"] = checker
                by_checker[name]["options"] += checker.options_and_values()
                by_checker[name]["msgs"].update(checker.msgs)
                by_checker[name]["reports"] += checker.reports
            except KeyError:
                by_checker[name] = {
                    "checker": checker,
                    "options": list(checker.options_and_values()),
                    "msgs": dict(checker.msgs),
                    "reports": list(checker.reports),
                }
    return by_checker


def _get_checkers_documentation(linter: "PyLinter") -> str:
    """Get documentation for individual checkers"""
    result = get_rst_title("Pylint global options and switches", "-")
    result += """
Pylint provides global options and switches.

"""
    for checker in linter.get_checkers():
        name = checker.name
        if name == MAIN_CHECKER_NAME:
            if checker.options:
                for section, options in checker.options_by_section():
                    if section is None:
                        title = "General options"
                    else:
                        title = f"{section.capitalize()} options"
                    result += get_rst_title(title, "~")
                    result += f"{get_rst_section(None, options)}\n"
    result += get_rst_title("Pylint checkers' options and switches", "-")
    result += """\

Pylint checkers can provide three set of features:

* options that control their execution,
* messages that they can raise,
* reports that they can generate.

Below is a list of all checkers and their features.

"""
    by_checker = _get_checkers_infos(linter)
    for checker in sorted(by_checker):
        information = by_checker[checker]
        checker = information["checker"]
        del information["checker"]
        result += checker.get_full_documentation(**information)
    return result


def print_full_documentation(linter: "PyLinter", stream: TextIO = sys.stdout) -> None:
    """Output a full documentation in ReST format"""
    print(_get_checkers_documentation(linter)[:-1], file=stream)
