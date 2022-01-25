"""Check for use of for loops that only check for a condition."""
from typing import TYPE_CHECKING

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker
from metaflow._vendor.pylint.checkers.utils import check_messages, returns_bool
from metaflow._vendor.pylint.interfaces import IAstroidChecker

if TYPE_CHECKING:
    from metaflow._vendor.pylint.lint.pylinter import PyLinter


class ConsiderUsingAnyOrAllChecker(BaseChecker):

    __implements__ = (IAstroidChecker,)
    name = "consider-using-any-or-all"
    msgs = {
        "C0501": (
            "`for` loop could be `%s`",
            "consider-using-any-or-all",
            "A for loop that checks for a condition and return a bool can be replaced with any or all.",
        )
    }

    @check_messages("consider-using-any-or-all")
    def visit_for(self, node: nodes.For) -> None:
        if len(node.body) != 1:  # Only If node with no Else
            return
        if not isinstance(node.body[0], nodes.If):
            return

        if_children = list(node.body[0].get_children())
        if not len(if_children) == 2:  # The If node has only a comparison and return
            return
        if not returns_bool(if_children[1]):
            return

        # Check for terminating boolean return right after the loop
        node_after_loop = node.next_sibling()
        if returns_bool(node_after_loop):
            final_return_bool = node_after_loop.value.value
            suggested_string = self._build_suggested_string(node, final_return_bool)
            self.add_message(
                "consider-using-any-or-all", node=node, args=suggested_string
            )

    @staticmethod
    def _build_suggested_string(node: nodes.For, final_return_bool: bool) -> str:
        """When a nodes.For node can be rewritten as an any/all statement, return a suggestion for that statement
        final_return_bool is the boolean literal returned after the for loop if all conditions fail
        """
        loop_var = node.target.as_string()
        loop_iter = node.iter.as_string()
        test_node = next(node.body[0].get_children())

        if isinstance(test_node, nodes.UnaryOp) and test_node.op == "not":
            # The condition is negated. Advance the node to the operand and modify the suggestion
            test_node = test_node.operand
            suggested_function = "all" if final_return_bool else "not all"
        else:
            suggested_function = "not any" if final_return_bool else "any"

        test = test_node.as_string()
        return f"{suggested_function}({test} for {loop_var} in {loop_iter})"


def register(linter: "PyLinter") -> None:
    """Required method to auto register this checker.

    :param linter: Main interface object for Pylint plugins
    """
    linter.register_checker(ConsiderUsingAnyOrAllChecker(linter))
