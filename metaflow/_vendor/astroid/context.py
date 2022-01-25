# Copyright (c) 2015-2016, 2018-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2018 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2019-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Bryce Guinta <bryce.guinta@protonmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Kian Meng, Ang <kianmeng.ang@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 David Liu <david@cs.toronto.edu>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Andrew Haigh <hello@nelf.in>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Various context related utilities, including inference and call contexts."""
import contextlib
import pprint
from typing import TYPE_CHECKING, List, MutableMapping, Optional, Sequence, Tuple

if TYPE_CHECKING:
    from metaflow._vendor.astroid.nodes.node_classes import Keyword, NodeNG


_INFERENCE_CACHE = {}


def _invalidate_cache():
    _INFERENCE_CACHE.clear()


class InferenceContext:
    """Provide context for inference

    Store already inferred nodes to save time
    Account for already visited nodes to stop infinite recursion
    """

    __slots__ = (
        "path",
        "lookupname",
        "callcontext",
        "boundnode",
        "extra_context",
        "_nodes_inferred",
    )

    max_inferred = 100

    def __init__(self, path=None, nodes_inferred=None):
        if nodes_inferred is None:
            self._nodes_inferred = [0]
        else:
            self._nodes_inferred = nodes_inferred
        self.path = path or set()
        """
        :type: set(tuple(NodeNG, optional(str)))

        Path of visited nodes and their lookupname

        Currently this key is ``(node, context.lookupname)``
        """
        self.lookupname = None
        """
        :type: optional[str]

        The original name of the node

        e.g.
        foo = 1
        The inference of 'foo' is nodes.Const(1) but the lookup name is 'foo'
        """
        self.callcontext = None
        """
        :type: optional[CallContext]

        The call arguments and keywords for the given context
        """
        self.boundnode = None
        """
        :type: optional[NodeNG]

        The bound node of the given context

        e.g. the bound node of object.__new__(cls) is the object node
        """
        self.extra_context = {}
        """
        :type: dict(NodeNG, Context)

        Context that needs to be passed down through call stacks
        for call arguments
        """

    @property
    def nodes_inferred(self):
        """
        Number of nodes inferred in this context and all its clones/descendents

        Wrap inner value in a mutable cell to allow for mutating a class
        variable in the presence of __slots__
        """
        return self._nodes_inferred[0]

    @nodes_inferred.setter
    def nodes_inferred(self, value):
        self._nodes_inferred[0] = value

    @property
    def inferred(
        self,
    ) -> MutableMapping[
        Tuple["NodeNG", Optional[str], Optional[str], Optional[str]], Sequence["NodeNG"]
    ]:
        """
        Inferred node contexts to their mapped results

        Currently the key is ``(node, lookupname, callcontext, boundnode)``
        and the value is tuple of the inferred results
        """
        return _INFERENCE_CACHE

    def push(self, node):
        """Push node into inference path

        :return: True if node is already in context path else False
        :rtype: bool

        Allows one to see if the given node has already
        been looked at for this inference context"""
        name = self.lookupname
        if (node, name) in self.path:
            return True

        self.path.add((node, name))
        return False

    def clone(self):
        """Clone inference path

        For example, each side of a binary operation (BinOp)
        starts with the same context but diverge as each side is inferred
        so the InferenceContext will need be cloned"""
        # XXX copy lookupname/callcontext ?
        clone = InferenceContext(self.path.copy(), nodes_inferred=self._nodes_inferred)
        clone.callcontext = self.callcontext
        clone.boundnode = self.boundnode
        clone.extra_context = self.extra_context
        return clone

    @contextlib.contextmanager
    def restore_path(self):
        path = set(self.path)
        yield
        self.path = path

    def __str__(self):
        state = (
            f"{field}={pprint.pformat(getattr(self, field), width=80 - len(field))}"
            for field in self.__slots__
        )
        return "{}({})".format(type(self).__name__, ",\n    ".join(state))


class CallContext:
    """Holds information for a call site."""

    __slots__ = ("args", "keywords", "callee")

    def __init__(
        self,
        args: List["NodeNG"],
        keywords: Optional[List["Keyword"]] = None,
        callee: Optional["NodeNG"] = None,
    ):
        self.args = args  # Call positional arguments
        if keywords:
            keywords = [(arg.arg, arg.value) for arg in keywords]
        else:
            keywords = []
        self.keywords = keywords  # Call keyword arguments
        self.callee = callee  # Function being called


def copy_context(context: Optional[InferenceContext]) -> InferenceContext:
    """Clone a context if given, or return a fresh contexxt"""
    if context is not None:
        return context.clone()

    return InferenceContext()


def bind_context_to_node(context, node):
    """Give a context a boundnode
    to retrieve the correct function name or attribute value
    with from further inference.

    Do not use an existing context since the boundnode could then
    be incorrectly propagated higher up in the call stack.

    :param context: Context to use
    :type context: Optional(context)

    :param node: Node to do name lookups from
    :type node NodeNG:

    :returns: A new context
    :rtype: InferenceContext
    """
    context = copy_context(context)
    context.boundnode = node
    return context
