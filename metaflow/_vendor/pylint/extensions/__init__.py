# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

from typing import TYPE_CHECKING

from metaflow._vendor.pylint.utils import register_plugins

if TYPE_CHECKING:
    from metaflow._vendor.pylint.lint import PyLinter


def initialize(linter: "PyLinter") -> None:
    """Initialize linter with checkers in the extensions directory"""
    register_plugins(linter, __path__[0])  # type: ignore[name-defined] # Fixed in https://github.com/python/mypy/pull/9454


__all__ = ["initialize"]
