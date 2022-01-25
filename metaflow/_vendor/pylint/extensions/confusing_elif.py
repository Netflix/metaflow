# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Andreas Finkler <andi.finkler@gmail.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker
from metaflow._vendor.pylint.checkers.utils import check_messages
from metaflow._vendor.pylint.interfaces import IAstroidChecker
from metaflow._vendor.pylint.lint import PyLinter


class ConfusingConsecutiveElifChecker(BaseChecker):
    """Checks if "elif" is used right after an indented block that finishes with "if" or "elif" itself."""

    __implements__ = IAstroidChecker

    name = "confusing_elif"
    priority = -1
    msgs = {
        "R5601": (
            "Consecutive elif with differing indentation level, consider creating a function to separate the inner elif",
            "confusing-consecutive-elif",
            "Used when an elif statement follows right after an indented block which itself ends with if or elif. "
            "It may not be ovious if the elif statement was willingly or mistakenly unindented. "
            "Extracting the indented if statement into a separate function might avoid confusion and prevent errors.",
        )
    }

    @check_messages("confusing-consecutive-elif")
    def visit_if(self, node: nodes.If) -> None:
        body_ends_with_if = isinstance(
            node.body[-1], nodes.If
        ) and self._has_no_else_clause(node.body[-1])
        if node.has_elif_block() and body_ends_with_if:
            self.add_message("confusing-consecutive-elif", node=node.orelse[0])

    @staticmethod
    def _has_no_else_clause(node: nodes.If):
        orelse = node.orelse
        while orelse and isinstance(orelse[0], nodes.If):
            orelse = orelse[0].orelse
        if not orelse or isinstance(orelse[0], nodes.If):
            return True
        return False


def register(linter: PyLinter):
    """This required method auto registers the checker.

    :param linter: The linter to register the checker to.
    :type linter: pylint.lint.PyLinter
    """
    linter.register_checker(ConfusingConsecutiveElifChecker(linter))
