# Copyright (c) 2019-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2019-2020 Tyler Thieding <tyler@thieding.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Looks for try/except statements with too much code in the try clause."""

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint import checkers, interfaces


class BroadTryClauseChecker(checkers.BaseChecker):
    """Checks for try clauses with too many lines.

    According to PEP 8, ``try`` clauses shall contain the absolute minimum
    amount of code. This checker enforces a maximum number of statements within
    ``try`` clauses.

    """

    __implements__ = interfaces.IAstroidChecker

    # configuration section name
    name = "broad_try_clause"
    msgs = {
        "W0717": (
            "%s",
            "too-many-try-statements",
            "Try clause contains too many statements.",
        )
    }

    priority = -2
    options = (
        (
            "max-try-statements",
            {
                "default": 1,
                "type": "int",
                "metavar": "<int>",
                "help": "Maximum number of statements allowed in a try clause",
            },
        ),
    )

    def _count_statements(self, try_node):
        statement_count = len(try_node.body)

        for body_node in try_node.body:
            if isinstance(body_node, (nodes.For, nodes.If, nodes.While, nodes.With)):
                statement_count += self._count_statements(body_node)

        return statement_count

    def visit_tryexcept(self, node: nodes.TryExcept) -> None:
        try_clause_statements = self._count_statements(node)
        if try_clause_statements > self.config.max_try_statements:
            msg = f"try clause contains {try_clause_statements} statements, expected at most {self.config.max_try_statements}"
            self.add_message(
                "too-many-try-statements", node.lineno, node=node, args=msg
            )

    def visit_tryfinally(self, node: nodes.TryFinally) -> None:
        self.visit_tryexcept(node)


def register(linter):
    """Required method to auto register this checker."""
    linter.register_checker(BroadTryClauseChecker(linter))
