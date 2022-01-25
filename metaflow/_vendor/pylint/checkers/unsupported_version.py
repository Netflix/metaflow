# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Mark Byrne <31762852+mbyrnepr2@users.noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Checker for features used that are not supported by all python versions
indicated by the py-version setting.
"""


from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker
from metaflow._vendor.pylint.checkers.utils import (
    check_messages,
    safe_infer,
    uninferable_final_decorators,
)
from metaflow._vendor.pylint.interfaces import IAstroidChecker
from metaflow._vendor.pylint.lint import PyLinter
from metaflow._vendor.pylint.utils import get_global_option


class UnsupportedVersionChecker(BaseChecker):
    """Checker for features that are not supported by all python versions
    indicated by the py-version setting.
    """

    __implements__ = (IAstroidChecker,)
    name = "unsupported_version"
    msgs = {
        "W1601": (
            "F-strings are not supported by all versions included in the py-version setting",
            "using-f-string-in-unsupported-version",
            "Used when the py-version set by the user is lower than 3.6 and pylint encounters "
            "a f-string.",
        ),
        "W1602": (
            "typing.final is not supported by all versions included in the py-version setting",
            "using-final-decorator-in-unsupported-version",
            "Used when the py-version set by the user is lower than 3.8 and pylint encounters "
            "a ``typing.final`` decorator.",
        ),
    }

    def open(self) -> None:
        """Initialize visit variables and statistics."""
        py_version = get_global_option(self, "py-version")
        self._py36_plus = py_version >= (3, 6)
        self._py38_plus = py_version >= (3, 8)

    @check_messages("using-f-string-in-unsupported-version")
    def visit_joinedstr(self, node: nodes.JoinedStr) -> None:
        """Check f-strings"""
        if not self._py36_plus:
            self.add_message("using-f-string-in-unsupported-version", node=node)

    @check_messages("using-final-decorator-in-unsupported-version")
    def visit_decorators(self, node: nodes.Decorators) -> None:
        """Check decorators"""
        self._check_typing_final(node)

    def _check_typing_final(self, node: nodes.Decorators) -> None:
        """Add a message when the `typing.final` decorator is used and the
        py-version is lower than 3.8"""
        if self._py38_plus:
            return

        decorators = []
        for decorator in node.get_children():
            inferred = safe_infer(decorator)
            if inferred and inferred.qname() == "typing.final":
                decorators.append(decorator)

        for decorator in decorators or uninferable_final_decorators(node):
            self.add_message(
                "using-final-decorator-in-unsupported-version", node=decorator
            )


def register(linter: PyLinter) -> None:
    """Required method to auto register this checker"""
    linter.register_checker(UnsupportedVersionChecker(linter))
