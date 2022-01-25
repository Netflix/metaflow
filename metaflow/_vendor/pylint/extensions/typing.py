from typing import Dict, List, NamedTuple, Set, Union

import astroid.bases
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker
from metaflow._vendor.pylint.checkers.utils import (
    check_messages,
    is_node_in_type_annotation_context,
    safe_infer,
)
from metaflow._vendor.pylint.interfaces import IAstroidChecker
from metaflow._vendor.pylint.lint import PyLinter
from metaflow._vendor.pylint.utils.utils import get_global_option


class TypingAlias(NamedTuple):
    name: str
    name_collision: bool


DEPRECATED_TYPING_ALIASES: Dict[str, TypingAlias] = {
    "typing.Tuple": TypingAlias("tuple", False),
    "typing.List": TypingAlias("list", False),
    "typing.Dict": TypingAlias("dict", False),
    "typing.Set": TypingAlias("set", False),
    "typing.FrozenSet": TypingAlias("frozenset", False),
    "typing.Type": TypingAlias("type", False),
    "typing.Deque": TypingAlias("collections.deque", True),
    "typing.DefaultDict": TypingAlias("collections.defaultdict", True),
    "typing.OrderedDict": TypingAlias("collections.OrderedDict", True),
    "typing.Counter": TypingAlias("collections.Counter", True),
    "typing.ChainMap": TypingAlias("collections.ChainMap", True),
    "typing.Awaitable": TypingAlias("collections.abc.Awaitable", True),
    "typing.Coroutine": TypingAlias("collections.abc.Coroutine", True),
    "typing.AsyncIterable": TypingAlias("collections.abc.AsyncIterable", True),
    "typing.AsyncIterator": TypingAlias("collections.abc.AsyncIterator", True),
    "typing.AsyncGenerator": TypingAlias("collections.abc.AsyncGenerator", True),
    "typing.Iterable": TypingAlias("collections.abc.Iterable", True),
    "typing.Iterator": TypingAlias("collections.abc.Iterator", True),
    "typing.Generator": TypingAlias("collections.abc.Generator", True),
    "typing.Reversible": TypingAlias("collections.abc.Reversible", True),
    "typing.Container": TypingAlias("collections.abc.Container", True),
    "typing.Collection": TypingAlias("collections.abc.Collection", True),
    "typing.Callable": TypingAlias("collections.abc.Callable", True),
    "typing.AbstractSet": TypingAlias("collections.abc.Set", False),
    "typing.MutableSet": TypingAlias("collections.abc.MutableSet", True),
    "typing.Mapping": TypingAlias("collections.abc.Mapping", True),
    "typing.MutableMapping": TypingAlias("collections.abc.MutableMapping", True),
    "typing.Sequence": TypingAlias("collections.abc.Sequence", True),
    "typing.MutableSequence": TypingAlias("collections.abc.MutableSequence", True),
    "typing.ByteString": TypingAlias("collections.abc.ByteString", True),
    "typing.MappingView": TypingAlias("collections.abc.MappingView", True),
    "typing.KeysView": TypingAlias("collections.abc.KeysView", True),
    "typing.ItemsView": TypingAlias("collections.abc.ItemsView", True),
    "typing.ValuesView": TypingAlias("collections.abc.ValuesView", True),
    "typing.ContextManager": TypingAlias("contextlib.AbstractContextManager", False),
    "typing.AsyncContextManager": TypingAlias(
        "contextlib.AbstractAsyncContextManager", False
    ),
    "typing.Pattern": TypingAlias("re.Pattern", True),
    "typing.Match": TypingAlias("re.Match", True),
    "typing.Hashable": TypingAlias("collections.abc.Hashable", True),
    "typing.Sized": TypingAlias("collections.abc.Sized", True),
}

ALIAS_NAMES = frozenset(key.split(".")[1] for key in DEPRECATED_TYPING_ALIASES)
UNION_NAMES = ("Optional", "Union")


class DeprecatedTypingAliasMsg(NamedTuple):
    node: Union[nodes.Name, nodes.Attribute]
    qname: str
    alias: str
    parent_subscript: bool


class TypingChecker(BaseChecker):
    """Find issue specifically related to type annotations."""

    __implements__ = (IAstroidChecker,)

    name = "typing"
    priority = -1
    msgs = {
        "W6001": (
            "'%s' is deprecated, use '%s' instead",
            "deprecated-typing-alias",
            "Emitted when a deprecated typing alias is used.",
        ),
        "R6002": (
            "'%s' will be deprecated with PY39, consider using '%s' instead%s",
            "consider-using-alias",
            "Only emitted if 'runtime-typing=no' and a deprecated "
            "typing alias is used in a type annotation context in "
            "Python 3.7 or 3.8.",
        ),
        "R6003": (
            "Consider using alternative Union syntax instead of '%s'%s",
            "consider-alternative-union-syntax",
            "Emitted when 'typing.Union' or 'typing.Optional' is used "
            "instead of the alternative Union syntax 'int | None'.",
        ),
    }
    options = (
        (
            "runtime-typing",
            {
                "default": True,
                "type": "yn",
                "metavar": "<y or n>",
                "help": (
                    "Set to ``no`` if the app / library does **NOT** need to "
                    "support runtime introspection of type annotations. "
                    "If you use type annotations **exclusively** for type checking "
                    "of an application, you're probably fine. For libraries, "
                    "evaluate if some users what to access the type hints "
                    "at runtime first, e.g., through ``typing.get_type_hints``. "
                    "Applies to Python versions 3.7 - 3.9"
                ),
            },
        ),
    )

    _should_check_typing_alias: bool
    """The use of type aliases (PEP 585) requires Python 3.9
    or Python 3.7+ with postponed evaluation.
    """

    _should_check_alternative_union_syntax: bool
    """The use of alternative union syntax (PEP 604) requires Python 3.10
    or Python 3.7+ with postponed evaluation.
    """

    def __init__(self, linter: PyLinter) -> None:
        """Initialize checker instance."""
        super().__init__(linter=linter)
        self._alias_name_collisions: Set[str] = set()
        self._consider_using_alias_msgs: List[DeprecatedTypingAliasMsg] = []

    def open(self) -> None:
        py_version = get_global_option(self, "py-version")
        self._py37_plus = py_version >= (3, 7)
        self._py39_plus = py_version >= (3, 9)
        self._py310_plus = py_version >= (3, 10)

        self._should_check_typing_alias = self._py39_plus or (
            self._py37_plus and self.config.runtime_typing is False
        )
        self._should_check_alternative_union_syntax = self._py310_plus or (
            self._py37_plus and self.config.runtime_typing is False
        )

    def _msg_postponed_eval_hint(self, node) -> str:
        """Message hint if postponed evaluation isn't enabled."""
        if self._py310_plus or "annotations" in node.root().future_imports:
            return ""
        return ". Add 'from __future__ import annotations' as well"

    @check_messages(
        "deprecated-typing-alias",
        "consider-using-alias",
        "consider-alternative-union-syntax",
    )
    def visit_name(self, node: nodes.Name) -> None:
        if self._should_check_typing_alias and node.name in ALIAS_NAMES:
            self._check_for_typing_alias(node)
        if self._should_check_alternative_union_syntax and node.name in UNION_NAMES:
            self._check_for_alternative_union_syntax(node, node.name)

    @check_messages(
        "deprecated-typing-alias",
        "consider-using-alias",
        "consider-alternative-union-syntax",
    )
    def visit_attribute(self, node: nodes.Attribute) -> None:
        if self._should_check_typing_alias and node.attrname in ALIAS_NAMES:
            self._check_for_typing_alias(node)
        if self._should_check_alternative_union_syntax and node.attrname in UNION_NAMES:
            self._check_for_alternative_union_syntax(node, node.attrname)

    def _check_for_alternative_union_syntax(
        self,
        node: Union[nodes.Name, nodes.Attribute],
        name: str,
    ) -> None:
        """Check if alternative union syntax could be used.

        Requires
        - Python 3.10
        - OR: Python 3.7+ with postponed evaluation in
              a type annotation context
        """
        inferred = safe_infer(node)
        if not (
            isinstance(inferred, nodes.FunctionDef)
            and inferred.qname() in {"typing.Optional", "typing.Union"}
            or isinstance(inferred, astroid.bases.Instance)
            and inferred.qname() == "typing._SpecialForm"
        ):
            return
        if not (self._py310_plus or is_node_in_type_annotation_context(node)):
            return
        self.add_message(
            "consider-alternative-union-syntax",
            node=node,
            args=(name, self._msg_postponed_eval_hint(node)),
        )

    def _check_for_typing_alias(
        self,
        node: Union[nodes.Name, nodes.Attribute],
    ) -> None:
        """Check if typing alias is deprecated or could be replaced.

        Requires
        - Python 3.9
        - OR: Python 3.7+ with postponed evaluation in
              a type annotation context

        For Python 3.7+: Only emitt message if change doesn't create
            any name collisions, only ever used in a type annotation
            context, and can safely be replaced.
        """
        inferred = safe_infer(node)
        if not isinstance(inferred, nodes.ClassDef):
            return
        alias = DEPRECATED_TYPING_ALIASES.get(inferred.qname(), None)
        if alias is None:
            return

        if self._py39_plus:
            self.add_message(
                "deprecated-typing-alias",
                node=node,
                args=(inferred.qname(), alias.name),
            )
            return

        # For PY37+, check for type annotation context first
        if not is_node_in_type_annotation_context(node) and isinstance(
            node.parent, nodes.Subscript
        ):
            if alias.name_collision is True:
                self._alias_name_collisions.add(inferred.qname())
            return
        self._consider_using_alias_msgs.append(
            DeprecatedTypingAliasMsg(
                node,
                inferred.qname(),
                alias.name,
                isinstance(node.parent, nodes.Subscript),
            )
        )

    @check_messages("consider-using-alias")
    def leave_module(self, node: nodes.Module) -> None:
        """After parsing of module is complete, add messages for
        'consider-using-alias' check. Make sure results are safe
        to recommend / collision free.
        """
        if self._py37_plus and not self._py39_plus:
            msg_future_import = self._msg_postponed_eval_hint(node)
            for msg in self._consider_using_alias_msgs:
                if msg.qname in self._alias_name_collisions:
                    continue
                self.add_message(
                    "consider-using-alias",
                    node=msg.node,
                    args=(
                        msg.qname,
                        msg.alias,
                        msg_future_import if msg.parent_subscript else "",
                    ),
                )
        # Clear all module cache variables
        self._alias_name_collisions.clear()
        self._consider_using_alias_msgs.clear()


def register(linter: PyLinter) -> None:
    linter.register_checker(TypingChecker(linter))
