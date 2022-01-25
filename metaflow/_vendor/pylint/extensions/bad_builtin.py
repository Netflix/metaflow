# Copyright (c) 2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2019, 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020 Peter Kolbus <peter.kolbus@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Checker for deprecated builtins."""
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker
from metaflow._vendor.pylint.checkers.utils import check_messages
from metaflow._vendor.pylint.interfaces import IAstroidChecker

BAD_FUNCTIONS = ["map", "filter"]
# Some hints regarding the use of bad builtins.
BUILTIN_HINTS = {"map": "Using a list comprehension can be clearer."}
BUILTIN_HINTS["filter"] = BUILTIN_HINTS["map"]


class BadBuiltinChecker(BaseChecker):

    __implements__ = (IAstroidChecker,)
    name = "deprecated_builtins"
    msgs = {
        "W0141": (
            "Used builtin function %s",
            "bad-builtin",
            "Used when a disallowed builtin function is used (see the "
            "bad-function option). Usual disallowed functions are the ones "
            "like map, or filter , where Python offers now some cleaner "
            "alternative like list comprehension.",
        )
    }

    options = (
        (
            "bad-functions",
            {
                "default": BAD_FUNCTIONS,
                "type": "csv",
                "metavar": "<builtin function names>",
                "help": "List of builtins function names that should not be "
                "used, separated by a comma",
            },
        ),
    )

    @check_messages("bad-builtin")
    def visit_call(self, node: nodes.Call) -> None:
        if isinstance(node.func, nodes.Name):
            name = node.func.name
            # ignore the name if it's not a builtin (i.e. not defined in the
            # locals nor globals scope)
            if not (name in node.frame() or name in node.root()):
                if name in self.config.bad_functions:
                    hint = BUILTIN_HINTS.get(name)
                    args = f"{name!r}. {hint}" if hint else repr(name)
                    self.add_message("bad-builtin", node=node, args=args)


def register(linter):
    """Required method to auto register this checker.

    :param linter: Main interface object for Pylint plugins
    :type linter: Pylint object
    """
    linter.register_checker(BadBuiltinChecker(linter))
