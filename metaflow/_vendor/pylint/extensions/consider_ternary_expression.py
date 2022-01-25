"""Check for if / assign blocks that can be rewritten with if-expressions."""

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker
from metaflow._vendor.pylint.interfaces import IAstroidChecker


class ConsiderTernaryExpressionChecker(BaseChecker):

    __implements__ = (IAstroidChecker,)
    name = "consider_ternary_expression"
    msgs = {
        "W0160": (
            "Consider rewriting as a ternary expression",
            "consider-ternary-expression",
            "Multiple assign statements spread across if/else blocks can be "
            "rewritten with a single assignment and ternary expression",
        )
    }

    def visit_if(self, node: nodes.If) -> None:
        if isinstance(node.parent, nodes.If):
            return

        if len(node.body) != 1 or len(node.orelse) != 1:
            return

        bst = node.body[0]
        ost = node.orelse[0]

        if not isinstance(bst, nodes.Assign) or not isinstance(ost, nodes.Assign):
            return

        for (bname, oname) in zip(bst.targets, ost.targets):
            if not isinstance(bname, nodes.AssignName) or not isinstance(
                oname, nodes.AssignName
            ):
                return

            if bname.name != oname.name:
                return

        self.add_message("consider-ternary-expression", node=node)


def register(linter):
    """Required method to auto register this checker.

    :param linter: Main interface object for Pylint plugins
    :type linter: Pylint object
    """
    linter.register_checker(ConsiderTernaryExpressionChecker(linter))
