# Copyright (c) 2015-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2017 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2019, 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Checker for anything related to the async protocol (PEP 492)."""

import sys

from metaflow._vendor import astroid
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint import checkers, interfaces, utils
from metaflow._vendor.pylint.checkers import utils as checker_utils
from metaflow._vendor.pylint.checkers.utils import decorated_with


class AsyncChecker(checkers.BaseChecker):
    __implements__ = interfaces.IAstroidChecker
    name = "async"
    msgs = {
        "E1700": (
            "Yield inside async function",
            "yield-inside-async-function",
            "Used when an `yield` or `yield from` statement is "
            "found inside an async function.",
            {"minversion": (3, 5)},
        ),
        "E1701": (
            "Async context manager '%s' doesn't implement __aenter__ and __aexit__.",
            "not-async-context-manager",
            "Used when an async context manager is used with an object "
            "that does not implement the async context management protocol.",
            {"minversion": (3, 5)},
        ),
    }

    def open(self):
        self._ignore_mixin_members = utils.get_global_option(
            self, "ignore-mixin-members"
        )
        self._mixin_class_rgx = utils.get_global_option(self, "mixin-class-rgx")
        self._async_generators = ["contextlib.asynccontextmanager"]

    @checker_utils.check_messages("yield-inside-async-function")
    def visit_asyncfunctiondef(self, node: nodes.AsyncFunctionDef) -> None:
        for child in node.nodes_of_class(nodes.Yield):
            if child.scope() is node and (
                sys.version_info[:2] == (3, 5) or isinstance(child, nodes.YieldFrom)
            ):
                self.add_message("yield-inside-async-function", node=child)

    @checker_utils.check_messages("not-async-context-manager")
    def visit_asyncwith(self, node: nodes.AsyncWith) -> None:
        for ctx_mgr, _ in node.items:
            inferred = checker_utils.safe_infer(ctx_mgr)
            if inferred is None or inferred is astroid.Uninferable:
                continue

            if isinstance(inferred, nodes.AsyncFunctionDef):
                # Check if we are dealing with a function decorated
                # with contextlib.asynccontextmanager.
                if decorated_with(inferred, self._async_generators):
                    continue
            elif isinstance(inferred, astroid.bases.AsyncGenerator):
                # Check if we are dealing with a function decorated
                # with contextlib.asynccontextmanager.
                if decorated_with(inferred.parent, self._async_generators):
                    continue
            else:
                try:
                    inferred.getattr("__aenter__")
                    inferred.getattr("__aexit__")
                except astroid.exceptions.NotFoundError:
                    if isinstance(inferred, astroid.Instance):
                        # If we do not know the bases of this class,
                        # just skip it.
                        if not checker_utils.has_known_bases(inferred):
                            continue
                        # Ignore mixin classes if they match the rgx option.
                        if self._ignore_mixin_members and self._mixin_class_rgx.match(
                            inferred.name
                        ):
                            continue
                else:
                    continue
            self.add_message(
                "not-async-context-manager", node=node, args=(inferred.name,)
            )


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(AsyncChecker(linter))
