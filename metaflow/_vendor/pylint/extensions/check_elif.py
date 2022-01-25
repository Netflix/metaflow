# Copyright (c) 2015 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2016-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2016 Glenn Matthews <glmatthe@cisco.com>
# Copyright (c) 2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 bot <bot@noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseTokenChecker
from metaflow._vendor.pylint.checkers.utils import check_messages
from metaflow._vendor.pylint.interfaces import HIGH, IAstroidChecker, ITokenChecker


class ElseifUsedChecker(BaseTokenChecker):
    """Checks for use of "else if" when an "elif" could be used"""

    __implements__ = (ITokenChecker, IAstroidChecker)
    name = "else_if_used"
    msgs = {
        "R5501": (
            'Consider using "elif" instead of "else if"',
            "else-if-used",
            "Used when an else statement is immediately followed by "
            "an if statement and does not contain statements that "
            "would be unrelated to it.",
        )
    }

    def __init__(self, linter=None):
        super().__init__(linter)
        self._init()

    def _init(self):
        self._elifs = {}

    def process_tokens(self, tokens):
        """Process tokens and look for 'if' or 'elif'"""
        self._elifs = {
            begin: token for _, token, begin, _, _ in tokens if token in {"elif", "if"}
        }

    def leave_module(self, _: nodes.Module) -> None:
        self._init()

    @check_messages("else-if-used")
    def visit_if(self, node: nodes.If) -> None:
        """Current if node must directly follow an 'else'"""
        if (
            isinstance(node.parent, nodes.If)
            and node.parent.orelse == [node]
            and (node.lineno, node.col_offset) in self._elifs
            and self._elifs[(node.lineno, node.col_offset)] == "if"
        ):
            self.add_message("else-if-used", node=node, confidence=HIGH)


def register(linter):
    """Required method to auto register this checker.

    :param linter: Main interface object for Pylint plugins
    :type linter: Pylint object
    """
    linter.register_checker(ElseifUsedChecker(linter))
