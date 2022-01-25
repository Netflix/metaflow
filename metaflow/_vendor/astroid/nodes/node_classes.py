# Copyright (c) 2009-2011, 2013-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2010 Daniel Harding <dharding@gmail.com>
# Copyright (c) 2012 FELD Boris <lothiraldan@gmail.com>
# Copyright (c) 2013-2014 Google, Inc.
# Copyright (c) 2014-2021 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Eevee (Alex Munroe) <amunroe@yelp.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2016-2017 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2016 Jared Garst <jgarst@users.noreply.github.com>
# Copyright (c) 2016 Jakub Wilk <jwilk@jwilk.net>
# Copyright (c) 2016 Dave Baum <dbaum@google.com>
# Copyright (c) 2017-2020 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2017, 2019 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2017 rr- <rr-@sakuya.pl>
# Copyright (c) 2018, 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2018 brendanator <brendan.maginnis@gmail.com>
# Copyright (c) 2018 HoverHell <hoverhell@gmail.com>
# Copyright (c) 2019 kavins14 <kavin.singh@mail.utoronto.ca>
# Copyright (c) 2019 kavins14 <kavinsingh@hotmail.com>
# Copyright (c) 2020 Raphael Gaschignard <raphael@rtpg.co>
# Copyright (c) 2020 Bryce Guinta <bryce.guinta@protonmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Tushar Sadhwani <86737547+tushar-deepsource@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Kian Meng, Ang <kianmeng.ang@gmail.com>
# Copyright (c) 2021 David Liu <david@cs.toronto.edu>
# Copyright (c) 2021 Alphadelta14 <alpha@alphaservcomputing.solutions>
# Copyright (c) 2021 Andrew Haigh <hello@nelf.in>
# Copyright (c) 2021 Federico Bond <federicobond@gmail.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Module for some node classes. More nodes in scoped_nodes.py"""

import abc
import itertools
import sys
import typing
import warnings
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Callable, Generator, Optional, TypeVar, Union

from metaflow._vendor.astroid import decorators, mixins, util
from metaflow._vendor.astroid.bases import Instance, _infer_stmts
from metaflow._vendor.astroid.const import Context
from metaflow._vendor.astroid.context import InferenceContext
from metaflow._vendor.astroid.exceptions import (
    AstroidIndexError,
    AstroidTypeError,
    InferenceError,
    NoDefault,
    ParentMissingError,
)
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes.const import OP_PRECEDENCE
from metaflow._vendor.astroid.nodes.node_ng import NodeNG

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from metaflow._vendor.typing_extensions import Literal

if TYPE_CHECKING:
    from metaflow._vendor.astroid import nodes
    from metaflow._vendor.astroid.nodes import LocalsDictNodeNG


def _is_const(value):
    return isinstance(value, tuple(CONST_CLS))


T_Nodes = TypeVar("T_Nodes", bound=NodeNG)

AssignedStmtsPossibleNode = Union["List", "Tuple", "AssignName", "AssignAttr", None]
AssignedStmtsCall = Callable[
    [
        T_Nodes,
        AssignedStmtsPossibleNode,
        Optional[InferenceContext],
        Optional[typing.List[int]],
    ],
    Any,
]


@decorators.raise_if_nothing_inferred
def unpack_infer(stmt, context=None):
    """recursively generate nodes inferred by the given statement.
    If the inferred value is a list or a tuple, recurse on the elements
    """
    if isinstance(stmt, (List, Tuple)):
        for elt in stmt.elts:
            if elt is util.Uninferable:
                yield elt
                continue
            yield from unpack_infer(elt, context)
        return dict(node=stmt, context=context)
    # if inferred is a final node, return it and stop
    inferred = next(stmt.infer(context), util.Uninferable)
    if inferred is stmt:
        yield inferred
        return dict(node=stmt, context=context)
    # else, infer recursively, except Uninferable object that should be returned as is
    for inferred in stmt.infer(context):
        if inferred is util.Uninferable:
            yield inferred
        else:
            yield from unpack_infer(inferred, context)

    return dict(node=stmt, context=context)


def are_exclusive(stmt1, stmt2, exceptions: Optional[typing.List[str]] = None) -> bool:
    """return true if the two given statements are mutually exclusive

    `exceptions` may be a list of exception names. If specified, discard If
    branches and check one of the statement is in an exception handler catching
    one of the given exceptions.

    algorithm :
     1) index stmt1's parents
     2) climb among stmt2's parents until we find a common parent
     3) if the common parent is a If or TryExcept statement, look if nodes are
        in exclusive branches
    """
    # index stmt1's parents
    stmt1_parents = {}
    children = {}
    previous = stmt1
    for node in stmt1.node_ancestors():
        stmt1_parents[node] = 1
        children[node] = previous
        previous = node
    # climb among stmt2's parents until we find a common parent
    previous = stmt2
    for node in stmt2.node_ancestors():
        if node in stmt1_parents:
            # if the common parent is a If or TryExcept statement, look if
            # nodes are in exclusive branches
            if isinstance(node, If) and exceptions is None:
                if (
                    node.locate_child(previous)[1]
                    is not node.locate_child(children[node])[1]
                ):
                    return True
            elif isinstance(node, TryExcept):
                c2attr, c2node = node.locate_child(previous)
                c1attr, c1node = node.locate_child(children[node])
                if c1node is not c2node:
                    first_in_body_caught_by_handlers = (
                        c2attr == "handlers"
                        and c1attr == "body"
                        and previous.catch(exceptions)
                    )
                    second_in_body_caught_by_handlers = (
                        c2attr == "body"
                        and c1attr == "handlers"
                        and children[node].catch(exceptions)
                    )
                    first_in_else_other_in_handlers = (
                        c2attr == "handlers" and c1attr == "orelse"
                    )
                    second_in_else_other_in_handlers = (
                        c2attr == "orelse" and c1attr == "handlers"
                    )
                    if any(
                        (
                            first_in_body_caught_by_handlers,
                            second_in_body_caught_by_handlers,
                            first_in_else_other_in_handlers,
                            second_in_else_other_in_handlers,
                        )
                    ):
                        return True
                elif c2attr == "handlers" and c1attr == "handlers":
                    return previous is not children[node]
            return False
        previous = node
    return False


# getitem() helpers.

_SLICE_SENTINEL = object()


def _slice_value(index, context=None):
    """Get the value of the given slice index."""

    if isinstance(index, Const):
        if isinstance(index.value, (int, type(None))):
            return index.value
    elif index is None:
        return None
    else:
        # Try to infer what the index actually is.
        # Since we can't return all the possible values,
        # we'll stop at the first possible value.
        try:
            inferred = next(index.infer(context=context))
        except (InferenceError, StopIteration):
            pass
        else:
            if isinstance(inferred, Const):
                if isinstance(inferred.value, (int, type(None))):
                    return inferred.value

    # Use a sentinel, because None can be a valid
    # value that this function can return,
    # as it is the case for unspecified bounds.
    return _SLICE_SENTINEL


def _infer_slice(node, context=None):
    lower = _slice_value(node.lower, context)
    upper = _slice_value(node.upper, context)
    step = _slice_value(node.step, context)
    if all(elem is not _SLICE_SENTINEL for elem in (lower, upper, step)):
        return slice(lower, upper, step)

    raise AstroidTypeError(
        message="Could not infer slice used in subscript",
        node=node,
        index=node.parent,
        context=context,
    )


def _container_getitem(instance, elts, index, context=None):
    """Get a slice or an item, using the given *index*, for the given sequence."""
    try:
        if isinstance(index, Slice):
            index_slice = _infer_slice(index, context=context)
            new_cls = instance.__class__()
            new_cls.elts = elts[index_slice]
            new_cls.parent = instance.parent
            return new_cls
        if isinstance(index, Const):
            return elts[index.value]
    except IndexError as exc:
        raise AstroidIndexError(
            message="Index {index!s} out of range",
            node=instance,
            index=index,
            context=context,
        ) from exc
    except TypeError as exc:
        raise AstroidTypeError(
            message="Type error {error!r}", node=instance, index=index, context=context
        ) from exc

    raise AstroidTypeError(f"Could not use {index} as subscript index")


class Statement(NodeNG):
    """Statement node adding a few attributes"""

    is_statement = True
    """Whether this node indicates a statement."""

    def next_sibling(self):
        """The next sibling statement node.

        :returns: The next sibling statement node.
        :rtype: NodeNG or None
        """
        stmts = self.parent.child_sequence(self)
        index = stmts.index(self)
        try:
            return stmts[index + 1]
        except IndexError:
            return None

    def previous_sibling(self):
        """The previous sibling statement.

        :returns: The previous sibling statement node.
        :rtype: NodeNG or None
        """
        stmts = self.parent.child_sequence(self)
        index = stmts.index(self)
        if index >= 1:
            return stmts[index - 1]
        return None


class BaseContainer(
    mixins.ParentAssignTypeMixin, NodeNG, Instance, metaclass=abc.ABCMeta
):
    """Base class for Set, FrozenSet, Tuple and List."""

    _astroid_fields = ("elts",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.elts: typing.List[NodeNG] = []
        """The elements in the node."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, elts: typing.List[NodeNG]) -> None:
        """Do some setup after initialisation.

        :param elts: The list of elements the that node contains.
        """
        self.elts = elts

    @classmethod
    def from_elements(cls, elts=None):
        """Create a node of this type from the given list of elements.

        :param elts: The list of elements that the node should contain.
        :type elts: list(NodeNG)

        :returns: A new node containing the given elements.
        :rtype: NodeNG
        """
        node = cls()
        if elts is None:
            node.elts = []
        else:
            node.elts = [const_factory(e) if _is_const(e) else e for e in elts]
        return node

    def itered(self):
        """An iterator over the elements this node contains.

        :returns: The contents of this node.
        :rtype: iterable(NodeNG)
        """
        return self.elts

    def bool_value(self, context=None):
        """Determine the boolean value of this node.

        :returns: The boolean value of this node.
        :rtype: bool or Uninferable
        """
        return bool(self.elts)

    @abc.abstractmethod
    def pytype(self):
        """Get the name of the type that this node represents.

        :returns: The name of the type.
        :rtype: str
        """

    def get_children(self):
        yield from self.elts


class LookupMixIn:
    """Mixin to look up a name in the right scope."""

    @lru_cache(maxsize=None)
    def lookup(self, name):
        """Lookup where the given variable is assigned.

        The lookup starts from self's scope. If self is not a frame itself
        and the name is found in the inner frame locals, statements will be
        filtered to remove ignorable statements according to self's location.

        :param name: The name of the variable to find assignments for.
        :type name: str

        :returns: The scope node and the list of assignments associated to the
            given name according to the scope where it has been found (locals,
            globals or builtin).
        :rtype: tuple(str, list(NodeNG))
        """
        return self.scope().scope_lookup(self, name)

    def ilookup(self, name):
        """Lookup the inferred values of the given variable.

        :param name: The variable name to find values for.
        :type name: str

        :returns: The inferred values of the statements returned from
            :meth:`lookup`.
        :rtype: iterable
        """
        frame, stmts = self.lookup(name)
        context = InferenceContext()
        return _infer_stmts(stmts, context, frame)


# Name classes


class AssignName(
    mixins.NoChildrenMixin, LookupMixIn, mixins.ParentAssignTypeMixin, NodeNG
):
    """Variation of :class:`ast.Assign` representing assignment to a name.

    An :class:`AssignName` is the name of something that is assigned to.
    This includes variables defined in a function signature or in a loop.

    >>> import astroid
    >>> node = astroid.extract_node('variable = range(10)')
    >>> node
    <Assign l.1 at 0x7effe1db8550>
    >>> list(node.get_children())
    [<AssignName.variable l.1 at 0x7effe1db8748>, <Call l.1 at 0x7effe1db8630>]
    >>> list(node.get_children())[0].as_string()
    'variable'
    """

    _other_fields = ("name",)

    @decorators.deprecate_default_argument_values(name="str")
    def __init__(
        self,
        name: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param name: The name that is assigned to.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.name: Optional[str] = name
        """The name that is assigned to."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    assigned_stmts: AssignedStmtsCall["AssignName"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """


class DelName(
    mixins.NoChildrenMixin, LookupMixIn, mixins.ParentAssignTypeMixin, NodeNG
):
    """Variation of :class:`ast.Delete` representing deletion of a name.

    A :class:`DelName` is the name of something that is deleted.

    >>> import astroid
    >>> node = astroid.extract_node("del variable #@")
    >>> list(node.get_children())
    [<DelName.variable l.1 at 0x7effe1da4d30>]
    >>> list(node.get_children())[0].as_string()
    'variable'
    """

    _other_fields = ("name",)

    @decorators.deprecate_default_argument_values(name="str")
    def __init__(
        self,
        name: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param name: The name that is being deleted.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.name: Optional[str] = name
        """The name that is being deleted."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )


class Name(mixins.NoChildrenMixin, LookupMixIn, NodeNG):
    """Class representing an :class:`ast.Name` node.

    A :class:`Name` node is something that is named, but not covered by
    :class:`AssignName` or :class:`DelName`.

    >>> import astroid
    >>> node = astroid.extract_node('range(10)')
    >>> node
    <Call l.1 at 0x7effe1db8710>
    >>> list(node.get_children())
    [<Name.range l.1 at 0x7effe1db86a0>, <Const.int l.1 at 0x7effe1db8518>]
    >>> list(node.get_children())[0].as_string()
    'range'
    """

    _other_fields = ("name",)

    @decorators.deprecate_default_argument_values(name="str")
    def __init__(
        self,
        name: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param name: The name that this node refers to.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.name: Optional[str] = name
        """The name that this node refers to."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def _get_name_nodes(self):
        yield self

        for child_node in self.get_children():
            yield from child_node._get_name_nodes()


class Arguments(mixins.AssignTypeMixin, NodeNG):
    """Class representing an :class:`ast.arguments` node.

    An :class:`Arguments` node represents that arguments in a
    function definition.

    >>> import astroid
    >>> node = astroid.extract_node('def foo(bar): pass')
    >>> node
    <FunctionDef.foo l.1 at 0x7effe1db8198>
    >>> node.args
    <Arguments l.1 at 0x7effe1db82e8>
    """

    # Python 3.4+ uses a different approach regarding annotations,
    # each argument is a new class, _ast.arg, which exposes an
    # 'annotation' attribute. In astroid though, arguments are exposed
    # as is in the Arguments node and the only way to expose annotations
    # is by using something similar with Python 3.3:
    #  - we expose 'varargannotation' and 'kwargannotation' of annotations
    #    of varargs and kwargs.
    #  - we expose 'annotation', a list with annotations for
    #    for each normal argument. If an argument doesn't have an
    #    annotation, its value will be None.
    _astroid_fields = (
        "args",
        "defaults",
        "kwonlyargs",
        "posonlyargs",
        "posonlyargs_annotations",
        "kw_defaults",
        "annotations",
        "varargannotation",
        "kwargannotation",
        "kwonlyargs_annotations",
        "type_comment_args",
        "type_comment_kwonlyargs",
        "type_comment_posonlyargs",
    )

    _other_fields = ("vararg", "kwarg")

    lineno: None
    col_offset: None
    end_lineno: None
    end_col_offset: None

    def __init__(
        self,
        vararg: Optional[str] = None,
        kwarg: Optional[str] = None,
        parent: Optional[NodeNG] = None,
    ) -> None:
        """
        :param vararg: The name of the variable length arguments.

        :param kwarg: The name of the variable length keyword arguments.

        :param parent: The parent node in the syntax tree.
        """
        super().__init__(parent=parent)

        self.vararg: Optional[str] = vararg  # can be None
        """The name of the variable length arguments."""

        self.kwarg: Optional[str] = kwarg  # can be None
        """The name of the variable length keyword arguments."""

        self.args: typing.Optional[typing.List[AssignName]]
        """The names of the required arguments.

        Can be None if the associated function does not have a retrievable
        signature and the arguments are therefore unknown.
        This happens with builtin functions implemented in C.
        """

        self.defaults: typing.List[NodeNG]
        """The default values for arguments that can be passed positionally."""

        self.kwonlyargs: typing.List[AssignName]
        """The keyword arguments that cannot be passed positionally."""

        self.posonlyargs: typing.List[AssignName] = []
        """The arguments that can only be passed positionally."""

        self.kw_defaults: typing.List[Optional[NodeNG]]
        """The default values for keyword arguments that cannot be passed positionally."""

        self.annotations: typing.List[Optional[NodeNG]]
        """The type annotations of arguments that can be passed positionally."""

        self.posonlyargs_annotations: typing.List[Optional[NodeNG]] = []
        """The type annotations of arguments that can only be passed positionally."""

        self.kwonlyargs_annotations: typing.List[Optional[NodeNG]] = []
        """The type annotations of arguments that cannot be passed positionally."""

        self.type_comment_args: typing.List[Optional[NodeNG]] = []
        """The type annotation, passed by a type comment, of each argument.

        If an argument does not have a type comment,
        the value for that argument will be None.
        """

        self.type_comment_kwonlyargs: typing.List[Optional[NodeNG]] = []
        """The type annotation, passed by a type comment, of each keyword only argument.

        If an argument does not have a type comment,
        the value for that argument will be None.
        """

        self.type_comment_posonlyargs: typing.List[Optional[NodeNG]] = []
        """The type annotation, passed by a type comment, of each positional argument.

        If an argument does not have a type comment,
        the value for that argument will be None.
        """

        self.varargannotation: Optional[NodeNG] = None  # can be None
        """The type annotation for the variable length arguments."""

        self.kwargannotation: Optional[NodeNG] = None  # can be None
        """The type annotation for the variable length keyword arguments."""

    # pylint: disable=too-many-arguments
    def postinit(
        self,
        args: typing.List[AssignName],
        defaults: typing.List[NodeNG],
        kwonlyargs: typing.List[AssignName],
        kw_defaults: typing.List[Optional[NodeNG]],
        annotations: typing.List[Optional[NodeNG]],
        posonlyargs: Optional[typing.List[AssignName]] = None,
        kwonlyargs_annotations: Optional[typing.List[Optional[NodeNG]]] = None,
        posonlyargs_annotations: Optional[typing.List[Optional[NodeNG]]] = None,
        varargannotation: Optional[NodeNG] = None,
        kwargannotation: Optional[NodeNG] = None,
        type_comment_args: Optional[typing.List[Optional[NodeNG]]] = None,
        type_comment_kwonlyargs: Optional[typing.List[Optional[NodeNG]]] = None,
        type_comment_posonlyargs: Optional[typing.List[Optional[NodeNG]]] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param args: The names of the required arguments.

        :param defaults: The default values for arguments that can be passed
            positionally.

        :param kwonlyargs: The keyword arguments that cannot be passed
            positionally.

        :param posonlyargs: The arguments that can only be passed
            positionally.

        :param kw_defaults: The default values for keyword arguments that
            cannot be passed positionally.

        :param annotations: The type annotations of arguments that can be
            passed positionally.

        :param kwonlyargs_annotations: The type annotations of arguments that
            cannot be passed positionally. This should always be passed in
            Python 3.

        :param posonlyargs_annotations: The type annotations of arguments that
            can only be passed positionally. This should always be passed in
            Python 3.

        :param varargannotation: The type annotation for the variable length
            arguments.

        :param kwargannotation: The type annotation for the variable length
            keyword arguments.

        :param type_comment_args: The type annotation,
            passed by a type comment, of each argument.

        :param type_comment_args: The type annotation,
            passed by a type comment, of each keyword only argument.

        :param type_comment_args: The type annotation,
            passed by a type comment, of each positional argument.
        """
        self.args = args
        self.defaults = defaults
        self.kwonlyargs = kwonlyargs
        if posonlyargs is not None:
            self.posonlyargs = posonlyargs
        self.kw_defaults = kw_defaults
        self.annotations = annotations
        if kwonlyargs_annotations is not None:
            self.kwonlyargs_annotations = kwonlyargs_annotations
        if posonlyargs_annotations is not None:
            self.posonlyargs_annotations = posonlyargs_annotations
        self.varargannotation = varargannotation
        self.kwargannotation = kwargannotation
        if type_comment_args is not None:
            self.type_comment_args = type_comment_args
        if type_comment_kwonlyargs is not None:
            self.type_comment_kwonlyargs = type_comment_kwonlyargs
        if type_comment_posonlyargs is not None:
            self.type_comment_posonlyargs = type_comment_posonlyargs

    assigned_stmts: AssignedStmtsCall["Arguments"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def _infer_name(self, frame, name):
        if self.parent is frame:
            return name
        return None

    @decorators.cachedproperty
    def fromlineno(self):
        """The first line that this node appears on in the source code.

        :type: int or None
        """
        lineno = super().fromlineno
        return max(lineno, self.parent.fromlineno or 0)

    @decorators.cachedproperty
    def arguments(self):
        """Get all the arguments for this node, including positional only and positional and keyword"""
        return list(itertools.chain((self.posonlyargs or ()), self.args or ()))

    def format_args(self):
        """Get the arguments formatted as string.

        :returns: The formatted arguments.
        :rtype: str
        """
        result = []
        positional_only_defaults = []
        positional_or_keyword_defaults = self.defaults
        if self.defaults:
            args = self.args or []
            positional_or_keyword_defaults = self.defaults[-len(args) :]
            positional_only_defaults = self.defaults[: len(self.defaults) - len(args)]

        if self.posonlyargs:
            result.append(
                _format_args(
                    self.posonlyargs,
                    positional_only_defaults,
                    self.posonlyargs_annotations,
                )
            )
            result.append("/")
        if self.args:
            result.append(
                _format_args(
                    self.args,
                    positional_or_keyword_defaults,
                    getattr(self, "annotations", None),
                )
            )
        if self.vararg:
            result.append(f"*{self.vararg}")
        if self.kwonlyargs:
            if not self.vararg:
                result.append("*")
            result.append(
                _format_args(
                    self.kwonlyargs, self.kw_defaults, self.kwonlyargs_annotations
                )
            )
        if self.kwarg:
            result.append(f"**{self.kwarg}")
        return ", ".join(result)

    def default_value(self, argname):
        """Get the default value for an argument.

        :param argname: The name of the argument to get the default value for.
        :type argname: str

        :raises NoDefault: If there is no default value defined for the
            given argument.
        """
        args = self.arguments
        index = _find_arg(argname, args)[0]
        if index is not None:
            idx = index - (len(args) - len(self.defaults))
            if idx >= 0:
                return self.defaults[idx]
        index = _find_arg(argname, self.kwonlyargs)[0]
        if index is not None and self.kw_defaults[index] is not None:
            return self.kw_defaults[index]
        raise NoDefault(func=self.parent, name=argname)

    def is_argument(self, name):
        """Check if the given name is defined in the arguments.

        :param name: The name to check for.
        :type name: str

        :returns: True if the given name is defined in the arguments,
            False otherwise.
        :rtype: bool
        """
        if name == self.vararg:
            return True
        if name == self.kwarg:
            return True
        return (
            self.find_argname(name, rec=True)[1] is not None
            or self.kwonlyargs
            and _find_arg(name, self.kwonlyargs, rec=True)[1] is not None
        )

    def find_argname(self, argname, rec=False):
        """Get the index and :class:`AssignName` node for given name.

        :param argname: The name of the argument to search for.
        :type argname: str

        :param rec: Whether or not to include arguments in unpacked tuples
            in the search.
        :type rec: bool

        :returns: The index and node for the argument.
        :rtype: tuple(str or None, AssignName or None)
        """
        if self.arguments:
            return _find_arg(argname, self.arguments, rec)
        return None, None

    def get_children(self):
        yield from self.posonlyargs or ()

        for elt in self.posonlyargs_annotations:
            if elt is not None:
                yield elt

        yield from self.args or ()

        yield from self.defaults
        yield from self.kwonlyargs

        for elt in self.kw_defaults:
            if elt is not None:
                yield elt

        for elt in self.annotations:
            if elt is not None:
                yield elt

        if self.varargannotation is not None:
            yield self.varargannotation

        if self.kwargannotation is not None:
            yield self.kwargannotation

        for elt in self.kwonlyargs_annotations:
            if elt is not None:
                yield elt


def _find_arg(argname, args, rec=False):
    for i, arg in enumerate(args):
        if isinstance(arg, Tuple):
            if rec:
                found = _find_arg(argname, arg.elts)
                if found[0] is not None:
                    return found
        elif arg.name == argname:
            return i, arg
    return None, None


def _format_args(args, defaults=None, annotations=None):
    values = []
    if args is None:
        return ""
    if annotations is None:
        annotations = []
    if defaults is not None:
        default_offset = len(args) - len(defaults)
    packed = itertools.zip_longest(args, annotations)
    for i, (arg, annotation) in enumerate(packed):
        if isinstance(arg, Tuple):
            values.append(f"({_format_args(arg.elts)})")
        else:
            argname = arg.name
            default_sep = "="
            if annotation is not None:
                argname += ": " + annotation.as_string()
                default_sep = " = "
            values.append(argname)

            if defaults is not None and i >= default_offset:
                if defaults[i - default_offset] is not None:
                    values[-1] += default_sep + defaults[i - default_offset].as_string()
    return ", ".join(values)


class AssignAttr(mixins.ParentAssignTypeMixin, NodeNG):
    """Variation of :class:`ast.Assign` representing assignment to an attribute.

    >>> import astroid
    >>> node = astroid.extract_node('self.attribute = range(10)')
    >>> node
    <Assign l.1 at 0x7effe1d521d0>
    >>> list(node.get_children())
    [<AssignAttr.attribute l.1 at 0x7effe1d52320>, <Call l.1 at 0x7effe1d522e8>]
    >>> list(node.get_children())[0].as_string()
    'self.attribute'
    """

    _astroid_fields = ("expr",)
    _other_fields = ("attrname",)

    @decorators.deprecate_default_argument_values(attrname="str")
    def __init__(
        self,
        attrname: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param attrname: The name of the attribute being assigned to.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.expr: Optional[NodeNG] = None
        """What has the attribute that is being assigned to."""

        self.attrname: Optional[str] = attrname
        """The name of the attribute being assigned to."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, expr: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param expr: What has the attribute that is being assigned to.
        """
        self.expr = expr

    assigned_stmts: AssignedStmtsCall["AssignAttr"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def get_children(self):
        yield self.expr


class Assert(Statement):
    """Class representing an :class:`ast.Assert` node.

    An :class:`Assert` node represents an assert statement.

    >>> import astroid
    >>> node = astroid.extract_node('assert len(things) == 10, "Not enough things"')
    >>> node
    <Assert l.1 at 0x7effe1d527b8>
    """

    _astroid_fields = ("test", "fail")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.test: Optional[NodeNG] = None
        """The test that passes or fails the assertion."""

        self.fail: Optional[NodeNG] = None  # can be None
        """The message shown when the assertion fails."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self, test: Optional[NodeNG] = None, fail: Optional[NodeNG] = None
    ) -> None:
        """Do some setup after initialisation.

        :param test: The test that passes or fails the assertion.

        :param fail: The message shown when the assertion fails.
        """
        self.fail = fail
        self.test = test

    def get_children(self):
        yield self.test

        if self.fail is not None:
            yield self.fail


class Assign(mixins.AssignTypeMixin, Statement):
    """Class representing an :class:`ast.Assign` node.

    An :class:`Assign` is a statement where something is explicitly
    asssigned to.

    >>> import astroid
    >>> node = astroid.extract_node('variable = range(10)')
    >>> node
    <Assign l.1 at 0x7effe1db8550>
    """

    _astroid_fields = ("targets", "value")
    _other_other_fields = ("type_annotation",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.targets: typing.List[NodeNG] = []
        """What is being assigned to."""

        self.value: Optional[NodeNG] = None
        """The value being assigned to the variables."""

        self.type_annotation: Optional[NodeNG] = None  # can be None
        """If present, this will contain the type annotation passed by a type comment"""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        targets: Optional[typing.List[NodeNG]] = None,
        value: Optional[NodeNG] = None,
        type_annotation: Optional[NodeNG] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param targets: What is being assigned to.
        :param value: The value being assigned to the variables.
        :param type_annotation:
        """
        if targets is not None:
            self.targets = targets
        self.value = value
        self.type_annotation = type_annotation

    assigned_stmts: AssignedStmtsCall["Assign"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def get_children(self):
        yield from self.targets

        yield self.value

    @decorators.cached
    def _get_assign_nodes(self):
        return [self] + list(self.value._get_assign_nodes())

    def _get_yield_nodes_skip_lambdas(self):
        yield from self.value._get_yield_nodes_skip_lambdas()


class AnnAssign(mixins.AssignTypeMixin, Statement):
    """Class representing an :class:`ast.AnnAssign` node.

    An :class:`AnnAssign` is an assignment with a type annotation.

    >>> import astroid
    >>> node = astroid.extract_node('variable: List[int] = range(10)')
    >>> node
    <AnnAssign l.1 at 0x7effe1d4c630>
    """

    _astroid_fields = ("target", "annotation", "value")
    _other_fields = ("simple",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.target: Optional[NodeNG] = None
        """What is being assigned to."""

        self.annotation: Optional[NodeNG] = None
        """The type annotation of what is being assigned to."""

        self.value: Optional[NodeNG] = None  # can be None
        """The value being assigned to the variables."""

        self.simple: Optional[int] = None
        """Whether :attr:`target` is a pure name or a complex statement."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        target: NodeNG,
        annotation: NodeNG,
        simple: int,
        value: Optional[NodeNG] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param target: What is being assigned to.

        :param annotation: The type annotation of what is being assigned to.

        :param simple: Whether :attr:`target` is a pure name
            or a complex statement.

        :param value: The value being assigned to the variables.
        """
        self.target = target
        self.annotation = annotation
        self.value = value
        self.simple = simple

    assigned_stmts: AssignedStmtsCall["AnnAssign"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def get_children(self):
        yield self.target
        yield self.annotation

        if self.value is not None:
            yield self.value


class AugAssign(mixins.AssignTypeMixin, Statement):
    """Class representing an :class:`ast.AugAssign` node.

    An :class:`AugAssign` is an assignment paired with an operator.

    >>> import astroid
    >>> node = astroid.extract_node('variable += 1')
    >>> node
    <AugAssign l.1 at 0x7effe1db4d68>
    """

    _astroid_fields = ("target", "value")
    _other_fields = ("op",)

    @decorators.deprecate_default_argument_values(op="str")
    def __init__(
        self,
        op: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param op: The operator that is being combined with the assignment.
            This includes the equals sign.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.target: Optional[NodeNG] = None
        """What is being assigned to."""

        self.op: Optional[str] = op
        """The operator that is being combined with the assignment.

        This includes the equals sign.
        """

        self.value: Optional[NodeNG] = None
        """The value being assigned to the variable."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self, target: Optional[NodeNG] = None, value: Optional[NodeNG] = None
    ) -> None:
        """Do some setup after initialisation.

        :param target: What is being assigned to.

        :param value: The value being assigned to the variable.
        """
        self.target = target
        self.value = value

    assigned_stmts: AssignedStmtsCall["AugAssign"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    # This is set by inference.py
    def _infer_augassign(self, context=None):
        raise NotImplementedError

    def type_errors(self, context=None):
        """Get a list of type errors which can occur during inference.

        Each TypeError is represented by a :class:`BadBinaryOperationMessage` ,
        which holds the original exception.

        :returns: The list of possible type errors.
        :rtype: list(BadBinaryOperationMessage)
        """
        try:
            results = self._infer_augassign(context=context)
            return [
                result
                for result in results
                if isinstance(result, util.BadBinaryOperationMessage)
            ]
        except InferenceError:
            return []

    def get_children(self):
        yield self.target
        yield self.value

    def _get_yield_nodes_skip_lambdas(self):
        """An AugAssign node can contain a Yield node in the value"""
        yield from self.value._get_yield_nodes_skip_lambdas()
        yield from super()._get_yield_nodes_skip_lambdas()


class BinOp(NodeNG):
    """Class representing an :class:`ast.BinOp` node.

    A :class:`BinOp` node is an application of a binary operator.

    >>> import astroid
    >>> node = astroid.extract_node('a + b')
    >>> node
    <BinOp l.1 at 0x7f23b2e8cfd0>
    """

    _astroid_fields = ("left", "right")
    _other_fields = ("op",)

    @decorators.deprecate_default_argument_values(op="str")
    def __init__(
        self,
        op: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param op: The operator.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.left: Optional[NodeNG] = None
        """What is being applied to the operator on the left side."""

        self.op: Optional[str] = op
        """The operator."""

        self.right: Optional[NodeNG] = None
        """What is being applied to the operator on the right side."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self, left: Optional[NodeNG] = None, right: Optional[NodeNG] = None
    ) -> None:
        """Do some setup after initialisation.

        :param left: What is being applied to the operator on the left side.

        :param right: What is being applied to the operator on the right side.
        """
        self.left = left
        self.right = right

    # This is set by inference.py
    def _infer_binop(self, context=None):
        raise NotImplementedError

    def type_errors(self, context=None):
        """Get a list of type errors which can occur during inference.

        Each TypeError is represented by a :class:`BadBinaryOperationMessage`,
        which holds the original exception.

        :returns: The list of possible type errors.
        :rtype: list(BadBinaryOperationMessage)
        """
        try:
            results = self._infer_binop(context=context)
            return [
                result
                for result in results
                if isinstance(result, util.BadBinaryOperationMessage)
            ]
        except InferenceError:
            return []

    def get_children(self):
        yield self.left
        yield self.right

    def op_precedence(self):
        return OP_PRECEDENCE[self.op]

    def op_left_associative(self):
        # 2**3**4 == 2**(3**4)
        return self.op != "**"


class BoolOp(NodeNG):
    """Class representing an :class:`ast.BoolOp` node.

    A :class:`BoolOp` is an application of a boolean operator.

    >>> import astroid
    >>> node = astroid.extract_node('a and b')
    >>> node
    <BinOp l.1 at 0x7f23b2e71c50>
    """

    _astroid_fields = ("values",)
    _other_fields = ("op",)

    @decorators.deprecate_default_argument_values(op="str")
    def __init__(
        self,
        op: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param op: The operator.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.op: Optional[str] = op
        """The operator."""

        self.values: typing.List[NodeNG] = []
        """The values being applied to the operator."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, values: Optional[typing.List[NodeNG]] = None) -> None:
        """Do some setup after initialisation.

        :param values: The values being applied to the operator.
        """
        if values is not None:
            self.values = values

    def get_children(self):
        yield from self.values

    def op_precedence(self):
        return OP_PRECEDENCE[self.op]


class Break(mixins.NoChildrenMixin, Statement):
    """Class representing an :class:`ast.Break` node.

    >>> import astroid
    >>> node = astroid.extract_node('break')
    >>> node
    <Break l.1 at 0x7f23b2e9e5c0>
    """


class Call(NodeNG):
    """Class representing an :class:`ast.Call` node.

    A :class:`Call` node is a call to a function, method, etc.

    >>> import astroid
    >>> node = astroid.extract_node('function()')
    >>> node
    <Call l.1 at 0x7f23b2e71eb8>
    """

    _astroid_fields = ("func", "args", "keywords")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.func: Optional[NodeNG] = None
        """What is being called."""

        self.args: typing.List[NodeNG] = []
        """The positional arguments being given to the call."""

        self.keywords: typing.List["Keyword"] = []
        """The keyword arguments being given to the call."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        func: Optional[NodeNG] = None,
        args: Optional[typing.List[NodeNG]] = None,
        keywords: Optional[typing.List["Keyword"]] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param func: What is being called.

        :param args: The positional arguments being given to the call.

        :param keywords: The keyword arguments being given to the call.
        """
        self.func = func
        if args is not None:
            self.args = args
        if keywords is not None:
            self.keywords = keywords

    @property
    def starargs(self) -> typing.List["Starred"]:
        """The positional arguments that unpack something."""
        return [arg for arg in self.args if isinstance(arg, Starred)]

    @property
    def kwargs(self) -> typing.List["Keyword"]:
        """The keyword arguments that unpack something."""
        return [keyword for keyword in self.keywords if keyword.arg is None]

    def get_children(self):
        yield self.func

        yield from self.args

        yield from self.keywords


class Compare(NodeNG):
    """Class representing an :class:`ast.Compare` node.

    A :class:`Compare` node indicates a comparison.

    >>> import astroid
    >>> node = astroid.extract_node('a <= b <= c')
    >>> node
    <Compare l.1 at 0x7f23b2e9e6d8>
    >>> node.ops
    [('<=', <Name.b l.1 at 0x7f23b2e9e2b0>), ('<=', <Name.c l.1 at 0x7f23b2e9e390>)]
    """

    _astroid_fields = ("left", "ops")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.left: Optional[NodeNG] = None
        """The value at the left being applied to a comparison operator."""

        self.ops: typing.List[typing.Tuple[str, NodeNG]] = []
        """The remainder of the operators and their relevant right hand value."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        left: Optional[NodeNG] = None,
        ops: Optional[typing.List[typing.Tuple[str, NodeNG]]] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param left: The value at the left being applied to a comparison
            operator.

        :param ops: The remainder of the operators
            and their relevant right hand value.
        """
        self.left = left
        if ops is not None:
            self.ops = ops

    def get_children(self):
        """Get the child nodes below this node.

        Overridden to handle the tuple fields and skip returning the operator
        strings.

        :returns: The children.
        :rtype: iterable(NodeNG)
        """
        yield self.left
        for _, comparator in self.ops:
            yield comparator  # we don't want the 'op'

    def last_child(self):
        """An optimized version of list(get_children())[-1]

        :returns: The last child.
        :rtype: NodeNG
        """
        # XXX maybe if self.ops:
        return self.ops[-1][1]
        # return self.left


class Comprehension(NodeNG):
    """Class representing an :class:`ast.comprehension` node.

    A :class:`Comprehension` indicates the loop inside any type of
    comprehension including generator expressions.

    >>> import astroid
    >>> node = astroid.extract_node('[x for x in some_values]')
    >>> list(node.get_children())
    [<Name.x l.1 at 0x7f23b2e352b0>, <Comprehension l.1 at 0x7f23b2e35320>]
    >>> list(node.get_children())[1].as_string()
    'for x in some_values'
    """

    _astroid_fields = ("target", "iter", "ifs")
    _other_fields = ("is_async",)

    optional_assign = True
    """Whether this node optionally assigns a variable."""

    lineno: None
    col_offset: None
    end_lineno: None
    end_col_offset: None

    def __init__(self, parent: Optional[NodeNG] = None) -> None:
        """
        :param parent: The parent node in the syntax tree.
        """
        self.target: Optional[NodeNG] = None
        """What is assigned to by the comprehension."""

        self.iter: Optional[NodeNG] = None
        """What is iterated over by the comprehension."""

        self.ifs: typing.List[NodeNG] = []
        """The contents of any if statements that filter the comprehension."""

        self.is_async: Optional[bool] = None
        """Whether this is an asynchronous comprehension or not."""

        super().__init__(parent=parent)

    # pylint: disable=redefined-builtin; same name as builtin ast module.
    def postinit(
        self,
        target: Optional[NodeNG] = None,
        iter: Optional[NodeNG] = None,
        ifs: Optional[typing.List[NodeNG]] = None,
        is_async: Optional[bool] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param target: What is assigned to by the comprehension.

        :param iter: What is iterated over by the comprehension.

        :param ifs: The contents of any if statements that filter
            the comprehension.

        :param is_async: Whether this is an asynchronous comprehension or not.
        """
        self.target = target
        self.iter = iter
        if ifs is not None:
            self.ifs = ifs
        self.is_async = is_async

    assigned_stmts: AssignedStmtsCall["Comprehension"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def assign_type(self):
        """The type of assignment that this node performs.

        :returns: The assignment type.
        :rtype: NodeNG
        """
        return self

    def _get_filtered_stmts(
        self, lookup_node, node, stmts, mystmt: Optional[Statement]
    ):
        """method used in filter_stmts"""
        if self is mystmt:
            if isinstance(lookup_node, (Const, Name)):
                return [lookup_node], True

        elif self.statement(future=True) is mystmt:
            # original node's statement is the assignment, only keeps
            # current node (gen exp, list comp)

            return [node], True

        return stmts, False

    def get_children(self):
        yield self.target
        yield self.iter

        yield from self.ifs


class Const(mixins.NoChildrenMixin, NodeNG, Instance):
    """Class representing any constant including num, str, bool, None, bytes.

    >>> import astroid
    >>> node = astroid.extract_node('(5, "This is a string.", True, None, b"bytes")')
    >>> node
    <Tuple.tuple l.1 at 0x7f23b2e358d0>
    >>> list(node.get_children())
    [<Const.int l.1 at 0x7f23b2e35940>,
    <Const.str l.1 at 0x7f23b2e35978>,
    <Const.bool l.1 at 0x7f23b2e359b0>,
    <Const.NoneType l.1 at 0x7f23b2e359e8>,
    <Const.bytes l.1 at 0x7f23b2e35a20>]
    """

    _other_fields = ("value", "kind")

    def __init__(
        self,
        value: typing.Any,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        kind: Optional[str] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param value: The value that the constant represents.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param kind: The string prefix. "u" for u-prefixed strings and ``None`` otherwise. Python 3.8+ only.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.value: typing.Any = value
        """The value that the constant represents."""

        self.kind: Optional[str] = kind  # can be None
        """"The string prefix. "u" for u-prefixed strings and ``None`` otherwise. Python 3.8+ only."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def __getattr__(self, name):
        # This is needed because of Proxy's __getattr__ method.
        # Calling object.__new__ on this class without calling
        # __init__ would result in an infinite loop otherwise
        # since __getattr__ is called when an attribute doesn't
        # exist and self._proxied indirectly calls self.value
        # and Proxy __getattr__ calls self.value
        if name == "value":
            raise AttributeError
        return super().__getattr__(name)

    def getitem(self, index, context=None):
        """Get an item from this node if subscriptable.

        :param index: The node to use as a subscript index.
        :type index: Const or Slice

        :raises AstroidTypeError: When the given index cannot be used as a
            subscript index, or if this node is not subscriptable.
        """
        if isinstance(index, Const):
            index_value = index.value
        elif isinstance(index, Slice):
            index_value = _infer_slice(index, context=context)

        else:
            raise AstroidTypeError(
                f"Could not use type {type(index)} as subscript index"
            )

        try:
            if isinstance(self.value, (str, bytes)):
                return Const(self.value[index_value])
        except IndexError as exc:
            raise AstroidIndexError(
                message="Index {index!r} out of range",
                node=self,
                index=index,
                context=context,
            ) from exc
        except TypeError as exc:
            raise AstroidTypeError(
                message="Type error {error!r}", node=self, index=index, context=context
            ) from exc

        raise AstroidTypeError(f"{self!r} (value={self.value})")

    def has_dynamic_getattr(self):
        """Check if the node has a custom __getattr__ or __getattribute__.

        :returns: True if the class has a custom
            __getattr__ or __getattribute__, False otherwise.
            For a :class:`Const` this is always ``False``.
        :rtype: bool
        """
        return False

    def itered(self):
        """An iterator over the elements this node contains.

        :returns: The contents of this node.
        :rtype: iterable(Const)

        :raises TypeError: If this node does not represent something that is iterable.
        """
        if isinstance(self.value, str):
            return [const_factory(elem) for elem in self.value]
        raise TypeError(f"Cannot iterate over type {type(self.value)!r}")

    def pytype(self):
        """Get the name of the type that this node represents.

        :returns: The name of the type.
        :rtype: str
        """
        return self._proxied.qname()

    def bool_value(self, context=None):
        """Determine the boolean value of this node.

        :returns: The boolean value of this node.
        :rtype: bool
        """
        return bool(self.value)


class Continue(mixins.NoChildrenMixin, Statement):
    """Class representing an :class:`ast.Continue` node.

    >>> import astroid
    >>> node = astroid.extract_node('continue')
    >>> node
    <Continue l.1 at 0x7f23b2e35588>
    """


class Decorators(NodeNG):
    """A node representing a list of decorators.

    A :class:`Decorators` is the decorators that are applied to
    a method or function.

    >>> import astroid
    >>> node = astroid.extract_node('''
    @property
    def my_property(self):
        return 3
    ''')
    >>> node
    <FunctionDef.my_property l.2 at 0x7f23b2e35d30>
    >>> list(node.get_children())[0]
    <Decorators l.1 at 0x7f23b2e35d68>
    """

    _astroid_fields = ("nodes",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.nodes: typing.List[NodeNG]
        """The decorators that this node contains.

        :type: list(Name or Call) or None
        """

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, nodes: typing.List[NodeNG]) -> None:
        """Do some setup after initialisation.

        :param nodes: The decorators that this node contains.
        :type nodes: list(Name or Call)
        """
        self.nodes = nodes

    def scope(self) -> "LocalsDictNodeNG":
        """The first parent node defining a new scope.
        These can be Module, FunctionDef, ClassDef, Lambda, or GeneratorExp nodes.

        :returns: The first parent scope node.
        """
        # skip the function node to go directly to the upper level scope
        if not self.parent:
            raise ParentMissingError(target=self)
        if not self.parent.parent:
            raise ParentMissingError(target=self.parent)
        return self.parent.parent.scope()

    def get_children(self):
        yield from self.nodes


class DelAttr(mixins.ParentAssignTypeMixin, NodeNG):
    """Variation of :class:`ast.Delete` representing deletion of an attribute.

    >>> import astroid
    >>> node = astroid.extract_node('del self.attr')
    >>> node
    <Delete l.1 at 0x7f23b2e35f60>
    >>> list(node.get_children())[0]
    <DelAttr.attr l.1 at 0x7f23b2e411d0>
    """

    _astroid_fields = ("expr",)
    _other_fields = ("attrname",)

    @decorators.deprecate_default_argument_values(attrname="str")
    def __init__(
        self,
        attrname: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param attrname: The name of the attribute that is being deleted.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.expr: Optional[NodeNG] = None
        """The name that this node represents.

        :type: Name or None
        """

        self.attrname: Optional[str] = attrname
        """The name of the attribute that is being deleted."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, expr: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param expr: The name that this node represents.
        :type expr: Name or None
        """
        self.expr = expr

    def get_children(self):
        yield self.expr


class Delete(mixins.AssignTypeMixin, Statement):
    """Class representing an :class:`ast.Delete` node.

    A :class:`Delete` is a ``del`` statement this is deleting something.

    >>> import astroid
    >>> node = astroid.extract_node('del self.attr')
    >>> node
    <Delete l.1 at 0x7f23b2e35f60>
    """

    _astroid_fields = ("targets",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.targets: typing.List[NodeNG] = []
        """What is being deleted."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, targets: Optional[typing.List[NodeNG]] = None) -> None:
        """Do some setup after initialisation.

        :param targets: What is being deleted.
        """
        if targets is not None:
            self.targets = targets

    def get_children(self):
        yield from self.targets


class Dict(NodeNG, Instance):
    """Class representing an :class:`ast.Dict` node.

    A :class:`Dict` is a dictionary that is created with ``{}`` syntax.

    >>> import astroid
    >>> node = astroid.extract_node('{1: "1"}')
    >>> node
    <Dict.dict l.1 at 0x7f23b2e35cc0>
    """

    _astroid_fields = ("items",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.items: typing.List[typing.Tuple[NodeNG, NodeNG]] = []
        """The key-value pairs contained in the dictionary."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, items: typing.List[typing.Tuple[NodeNG, NodeNG]]) -> None:
        """Do some setup after initialisation.

        :param items: The key-value pairs contained in the dictionary.
        """
        self.items = items

    @classmethod
    def from_elements(cls, items=None):
        """Create a :class:`Dict` of constants from a live dictionary.

        :param items: The items to store in the node.
        :type items: dict

        :returns: The created dictionary node.
        :rtype: Dict
        """
        node = cls()
        if items is None:
            node.items = []
        else:
            node.items = [
                (const_factory(k), const_factory(v) if _is_const(v) else v)
                for k, v in items.items()
                # The keys need to be constants
                if _is_const(k)
            ]
        return node

    def pytype(self):
        """Get the name of the type that this node represents.

        :returns: The name of the type.
        :rtype: str
        """
        return "builtins.dict"

    def get_children(self):
        """Get the key and value nodes below this node.

        Children are returned in the order that they are defined in the source
        code, key first then the value.

        :returns: The children.
        :rtype: iterable(NodeNG)
        """
        for key, value in self.items:
            yield key
            yield value

    def last_child(self):
        """An optimized version of list(get_children())[-1]

        :returns: The last child, or None if no children exist.
        :rtype: NodeNG or None
        """
        if self.items:
            return self.items[-1][1]
        return None

    def itered(self):
        """An iterator over the keys this node contains.

        :returns: The keys of this node.
        :rtype: iterable(NodeNG)
        """
        return [key for (key, _) in self.items]

    def getitem(self, index, context=None):
        """Get an item from this node.

        :param index: The node to use as a subscript index.
        :type index: Const or Slice

        :raises AstroidTypeError: When the given index cannot be used as a
            subscript index, or if this node is not subscriptable.
        :raises AstroidIndexError: If the given index does not exist in the
            dictionary.
        """
        for key, value in self.items:
            # TODO(cpopa): no support for overriding yet, {1:2, **{1: 3}}.
            if isinstance(key, DictUnpack):
                try:
                    return value.getitem(index, context)
                except (AstroidTypeError, AstroidIndexError):
                    continue
            for inferredkey in key.infer(context):
                if inferredkey is util.Uninferable:
                    continue
                if isinstance(inferredkey, Const) and isinstance(index, Const):
                    if inferredkey.value == index.value:
                        return value

        raise AstroidIndexError(index)

    def bool_value(self, context=None):
        """Determine the boolean value of this node.

        :returns: The boolean value of this node.
        :rtype: bool
        """
        return bool(self.items)


class Expr(Statement):
    """Class representing an :class:`ast.Expr` node.

    An :class:`Expr` is any expression that does not have its value used or
    stored.

    >>> import astroid
    >>> node = astroid.extract_node('method()')
    >>> node
    <Call l.1 at 0x7f23b2e352b0>
    >>> node.parent
    <Expr l.1 at 0x7f23b2e35278>
    """

    _astroid_fields = ("value",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.value: Optional[NodeNG] = None
        """What the expression does."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, value: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param value: What the expression does.
        """
        self.value = value

    def get_children(self):
        yield self.value

    def _get_yield_nodes_skip_lambdas(self):
        if not self.value.is_lambda:
            yield from self.value._get_yield_nodes_skip_lambdas()


class Ellipsis(mixins.NoChildrenMixin, NodeNG):  # pylint: disable=redefined-builtin
    """Class representing an :class:`ast.Ellipsis` node.

    An :class:`Ellipsis` is the ``...`` syntax.

    Deprecated since v2.6.0 - Use :class:`Const` instead.
    Will be removed with the release v2.7.0
    """


class EmptyNode(mixins.NoChildrenMixin, NodeNG):
    """Holds an arbitrary object in the :attr:`LocalsDictNodeNG.locals`."""

    object = None


class ExceptHandler(mixins.MultiLineBlockMixin, mixins.AssignTypeMixin, Statement):
    """Class representing an :class:`ast.ExceptHandler`. node.

    An :class:`ExceptHandler` is an ``except`` block on a try-except.

    >>> import astroid
    >>> node = astroid.extract_node('''
        try:
            do_something()
        except Exception as error:
            print("Error!")
        ''')
    >>> node
    <TryExcept l.2 at 0x7f23b2e9d908>
    >>> node.handlers
    [<ExceptHandler l.4 at 0x7f23b2e9e860>]
    """

    _astroid_fields = ("type", "name", "body")
    _multi_line_block_fields = ("body",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.type: Optional[NodeNG] = None  # can be None
        """The types that the block handles.

        :type: Tuple or NodeNG or None
        """

        self.name: Optional[AssignName] = None  # can be None
        """The name that the caught exception is assigned to."""

        self.body: typing.List[NodeNG] = []
        """The contents of the block."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    assigned_stmts: AssignedStmtsCall["ExceptHandler"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def get_children(self):
        if self.type is not None:
            yield self.type

        if self.name is not None:
            yield self.name

        yield from self.body

    # pylint: disable=redefined-builtin; had to use the same name as builtin ast module.
    def postinit(
        self,
        type: Optional[NodeNG] = None,
        name: Optional[AssignName] = None,
        body: Optional[typing.List[NodeNG]] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param type: The types that the block handles.
        :type type: Tuple or NodeNG or None

        :param name: The name that the caught exception is assigned to.

        :param body:The contents of the block.
        """
        self.type = type
        self.name = name
        if body is not None:
            self.body = body

    @decorators.cachedproperty
    def blockstart_tolineno(self):
        """The line on which the beginning of this block ends.

        :type: int
        """
        if self.name:
            return self.name.tolineno
        if self.type:
            return self.type.tolineno
        return self.lineno

    def catch(self, exceptions: Optional[typing.List[str]]) -> bool:
        """Check if this node handles any of the given

        :param exceptions: The names of the exceptions to check for.
        """
        if self.type is None or exceptions is None:
            return True
        return any(node.name in exceptions for node in self.type._get_name_nodes())


class ExtSlice(NodeNG):
    """Class representing an :class:`ast.ExtSlice` node.

    An :class:`ExtSlice` is a complex slice expression.

    Deprecated since v2.6.0 - Now part of the :class:`Subscript` node.
    Will be removed with the release of v2.7.0
    """


class For(
    mixins.MultiLineBlockMixin,
    mixins.BlockRangeMixIn,
    mixins.AssignTypeMixin,
    Statement,
):
    """Class representing an :class:`ast.For` node.

    >>> import astroid
    >>> node = astroid.extract_node('for thing in things: print(thing)')
    >>> node
    <For l.1 at 0x7f23b2e8cf28>
    """

    _astroid_fields = ("target", "iter", "body", "orelse")
    _other_other_fields = ("type_annotation",)
    _multi_line_block_fields = ("body", "orelse")

    optional_assign = True
    """Whether this node optionally assigns a variable.

    This is always ``True`` for :class:`For` nodes.
    """

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.target: Optional[NodeNG] = None
        """What the loop assigns to."""

        self.iter: Optional[NodeNG] = None
        """What the loop iterates over."""

        self.body: typing.List[NodeNG] = []
        """The contents of the body of the loop."""

        self.orelse: typing.List[NodeNG] = []
        """The contents of the ``else`` block of the loop."""

        self.type_annotation: Optional[NodeNG] = None  # can be None
        """If present, this will contain the type annotation passed by a type comment"""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    # pylint: disable=redefined-builtin; had to use the same name as builtin ast module.
    def postinit(
        self,
        target: Optional[NodeNG] = None,
        iter: Optional[NodeNG] = None,
        body: Optional[typing.List[NodeNG]] = None,
        orelse: Optional[typing.List[NodeNG]] = None,
        type_annotation: Optional[NodeNG] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param target: What the loop assigns to.

        :param iter: What the loop iterates over.

        :param body: The contents of the body of the loop.

        :param orelse: The contents of the ``else`` block of the loop.
        """
        self.target = target
        self.iter = iter
        if body is not None:
            self.body = body
        if orelse is not None:
            self.orelse = orelse
        self.type_annotation = type_annotation

    assigned_stmts: AssignedStmtsCall["For"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    @decorators.cachedproperty
    def blockstart_tolineno(self):
        """The line on which the beginning of this block ends.

        :type: int
        """
        return self.iter.tolineno

    def get_children(self):
        yield self.target
        yield self.iter

        yield from self.body
        yield from self.orelse


class AsyncFor(For):
    """Class representing an :class:`ast.AsyncFor` node.

    An :class:`AsyncFor` is an asynchronous :class:`For` built with
    the ``async`` keyword.

    >>> import astroid
    >>> node = astroid.extract_node('''
    async def func(things):
        async for thing in things:
            print(thing)
    ''')
    >>> node
    <AsyncFunctionDef.func l.2 at 0x7f23b2e416d8>
    >>> node.body[0]
    <AsyncFor l.3 at 0x7f23b2e417b8>
    """


class Await(NodeNG):
    """Class representing an :class:`ast.Await` node.

    An :class:`Await` is the ``await`` keyword.

    >>> import astroid
    >>> node = astroid.extract_node('''
    async def func(things):
        await other_func()
    ''')
    >>> node
    <AsyncFunctionDef.func l.2 at 0x7f23b2e41748>
    >>> node.body[0]
    <Expr l.3 at 0x7f23b2e419e8>
    >>> list(node.body[0].get_children())[0]
    <Await l.3 at 0x7f23b2e41a20>
    """

    _astroid_fields = ("value",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.value: Optional[NodeNG] = None
        """What to wait for."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, value: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param value: What to wait for.
        """
        self.value = value

    def get_children(self):
        yield self.value


class ImportFrom(mixins.NoChildrenMixin, mixins.ImportFromMixin, Statement):
    """Class representing an :class:`ast.ImportFrom` node.

    >>> import astroid
    >>> node = astroid.extract_node('from my_package import my_module')
    >>> node
    <ImportFrom l.1 at 0x7f23b2e415c0>
    """

    _other_fields = ("modname", "names", "level")

    def __init__(
        self,
        fromname: Optional[str],
        names: typing.List[typing.Tuple[str, Optional[str]]],
        level: Optional[int] = 0,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param fromname: The module that is being imported from.

        :param names: What is being imported from the module.

        :param level: The level of relative import.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.modname: Optional[str] = fromname  # can be None
        """The module that is being imported from.

        This is ``None`` for relative imports.
        """

        self.names: typing.List[typing.Tuple[str, Optional[str]]] = names
        """What is being imported from the module.

        Each entry is a :class:`tuple` of the name being imported,
        and the alias that the name is assigned to (if any).
        """

        # TODO When is 'level' None?
        self.level: Optional[int] = level  # can be None
        """The level of relative import.

        Essentially this is the number of dots in the import.
        This is always 0 for absolute imports.
        """

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )


class Attribute(NodeNG):
    """Class representing an :class:`ast.Attribute` node."""

    _astroid_fields = ("expr",)
    _other_fields = ("attrname",)

    @decorators.deprecate_default_argument_values(attrname="str")
    def __init__(
        self,
        attrname: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param attrname: The name of the attribute.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.expr: Optional[NodeNG] = None
        """The name that this node represents.

        :type: Name or None
        """

        self.attrname: Optional[str] = attrname
        """The name of the attribute."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, expr: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param expr: The name that this node represents.
        :type expr: Name or None
        """
        self.expr = expr

    def get_children(self):
        yield self.expr


class Global(mixins.NoChildrenMixin, Statement):
    """Class representing an :class:`ast.Global` node.

    >>> import astroid
    >>> node = astroid.extract_node('global a_global')
    >>> node
    <Global l.1 at 0x7f23b2e9de10>
    """

    _other_fields = ("names",)

    def __init__(
        self,
        names: typing.List[str],
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param names: The names being declared as global.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.names: typing.List[str] = names
        """The names being declared as global."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def _infer_name(self, frame, name):
        return name


class If(mixins.MultiLineBlockMixin, mixins.BlockRangeMixIn, Statement):
    """Class representing an :class:`ast.If` node.

    >>> import astroid
    >>> node = astroid.extract_node('if condition: print(True)')
    >>> node
    <If l.1 at 0x7f23b2e9dd30>
    """

    _astroid_fields = ("test", "body", "orelse")
    _multi_line_block_fields = ("body", "orelse")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.test: Optional[NodeNG] = None
        """The condition that the statement tests."""

        self.body: typing.List[NodeNG] = []
        """The contents of the block."""

        self.orelse: typing.List[NodeNG] = []
        """The contents of the ``else`` block."""

        self.is_orelse: bool = False
        """Whether the if-statement is the orelse-block of another if statement."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        test: Optional[NodeNG] = None,
        body: Optional[typing.List[NodeNG]] = None,
        orelse: Optional[typing.List[NodeNG]] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param test: The condition that the statement tests.

        :param body: The contents of the block.

        :param orelse: The contents of the ``else`` block.
        """
        self.test = test
        if body is not None:
            self.body = body
        if orelse is not None:
            self.orelse = orelse
        if isinstance(self.parent, If) and self in self.parent.orelse:
            self.is_orelse = True

    @decorators.cachedproperty
    def blockstart_tolineno(self):
        """The line on which the beginning of this block ends.

        :type: int
        """
        return self.test.tolineno

    def block_range(self, lineno):
        """Get a range from the given line number to where this node ends.

        :param lineno: The line number to start the range at.
        :type lineno: int

        :returns: The range of line numbers that this node belongs to,
            starting at the given line number.
        :rtype: tuple(int, int)
        """
        if lineno == self.body[0].fromlineno:
            return lineno, lineno
        if lineno <= self.body[-1].tolineno:
            return lineno, self.body[-1].tolineno
        return self._elsed_block_range(lineno, self.orelse, self.body[0].fromlineno - 1)

    def get_children(self):
        yield self.test

        yield from self.body
        yield from self.orelse

    def has_elif_block(self):
        return len(self.orelse) == 1 and isinstance(self.orelse[0], If)

    def _get_yield_nodes_skip_lambdas(self):
        """An If node can contain a Yield node in the test"""
        yield from self.test._get_yield_nodes_skip_lambdas()
        yield from super()._get_yield_nodes_skip_lambdas()

    def is_sys_guard(self) -> bool:
        """Return True if IF stmt is a sys.version_info guard.

        >>> import astroid
        >>> node = astroid.extract_node('''
        import sys
        if sys.version_info > (3, 8):
            from typing import Literal
        else:
            from metaflow._vendor.typing_extensions import Literal
        ''')
        >>> node.is_sys_guard()
        True
        """
        warnings.warn(
            "The 'is_sys_guard' function is deprecated and will be removed in astroid 3.0.0 "
            "It has been moved to pylint and can be imported from 'pylint.checkers.utils' "
            "starting with pylint 2.12",
            DeprecationWarning,
        )
        if isinstance(self.test, Compare):
            value = self.test.left
            if isinstance(value, Subscript):
                value = value.value
            if isinstance(value, Attribute) and value.as_string() == "sys.version_info":
                return True

        return False

    def is_typing_guard(self) -> bool:
        """Return True if IF stmt is a typing guard.

        >>> import astroid
        >>> node = astroid.extract_node('''
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from xyz import a
        ''')
        >>> node.is_typing_guard()
        True
        """
        warnings.warn(
            "The 'is_typing_guard' function is deprecated and will be removed in astroid 3.0.0 "
            "It has been moved to pylint and can be imported from 'pylint.checkers.utils' "
            "starting with pylint 2.12",
            DeprecationWarning,
        )
        return isinstance(
            self.test, (Name, Attribute)
        ) and self.test.as_string().endswith("TYPE_CHECKING")


class IfExp(NodeNG):
    """Class representing an :class:`ast.IfExp` node.
    >>> import astroid
    >>> node = astroid.extract_node('value if condition else other')
    >>> node
    <IfExp l.1 at 0x7f23b2e9dbe0>
    """

    _astroid_fields = ("test", "body", "orelse")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.test: Optional[NodeNG] = None
        """The condition that the statement tests."""

        self.body: Optional[NodeNG] = None
        """The contents of the block."""

        self.orelse: Optional[NodeNG] = None
        """The contents of the ``else`` block."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        test: Optional[NodeNG] = None,
        body: Optional[NodeNG] = None,
        orelse: Optional[NodeNG] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param test: The condition that the statement tests.

        :param body: The contents of the block.

        :param orelse: The contents of the ``else`` block.
        """
        self.test = test
        self.body = body
        self.orelse = orelse

    def get_children(self):
        yield self.test
        yield self.body
        yield self.orelse

    def op_left_associative(self):
        # `1 if True else 2 if False else 3` is parsed as
        # `1 if True else (2 if False else 3)`
        return False


class Import(mixins.NoChildrenMixin, mixins.ImportFromMixin, Statement):
    """Class representing an :class:`ast.Import` node.
    >>> import astroid
    >>> node = astroid.extract_node('import astroid')
    >>> node
    <Import l.1 at 0x7f23b2e4e5c0>
    """

    _other_fields = ("names",)

    @decorators.deprecate_default_argument_values(names="list[tuple[str, str | None]]")
    def __init__(
        self,
        names: Optional[typing.List[typing.Tuple[str, Optional[str]]]] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param names: The names being imported.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.names: typing.List[typing.Tuple[str, Optional[str]]] = names or []
        """The names being imported.

        Each entry is a :class:`tuple` of the name being imported,
        and the alias that the name is assigned to (if any).
        """

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )


class Index(NodeNG):
    """Class representing an :class:`ast.Index` node.

    An :class:`Index` is a simple subscript.

    Deprecated since v2.6.0 - Now part of the :class:`Subscript` node.
    Will be removed with the release of v2.7.0
    """


class Keyword(NodeNG):
    """Class representing an :class:`ast.keyword` node.

    >>> import astroid
    >>> node = astroid.extract_node('function(a_kwarg=True)')
    >>> node
    <Call l.1 at 0x7f23b2e9e320>
    >>> node.keywords
    [<Keyword l.1 at 0x7f23b2e9e9b0>]
    """

    _astroid_fields = ("value",)
    _other_fields = ("arg",)

    def __init__(
        self,
        arg: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param arg: The argument being assigned to.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.arg: Optional[str] = arg  # can be None
        """The argument being assigned to."""

        self.value: Optional[NodeNG] = None
        """The value being assigned to the keyword argument."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, value: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param value: The value being assigned to the keyword argument.
        """
        self.value = value

    def get_children(self):
        yield self.value


class List(BaseContainer):
    """Class representing an :class:`ast.List` node.

    >>> import astroid
    >>> node = astroid.extract_node('[1, 2, 3]')
    >>> node
    <List.list l.1 at 0x7f23b2e9e128>
    """

    _other_fields = ("ctx",)

    def __init__(
        self,
        ctx: Optional[Context] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param ctx: Whether the list is assigned to or loaded from.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.ctx: Optional[Context] = ctx
        """Whether the list is assigned to or loaded from."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    assigned_stmts: AssignedStmtsCall["List"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def pytype(self):
        """Get the name of the type that this node represents.

        :returns: The name of the type.
        :rtype: str
        """
        return "builtins.list"

    def getitem(self, index, context=None):
        """Get an item from this node.

        :param index: The node to use as a subscript index.
        :type index: Const or Slice
        """
        return _container_getitem(self, self.elts, index, context=context)


class Nonlocal(mixins.NoChildrenMixin, Statement):
    """Class representing an :class:`ast.Nonlocal` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    def function():
        nonlocal var
    ''')
    >>> node
    <FunctionDef.function l.2 at 0x7f23b2e9e208>
    >>> node.body[0]
    <Nonlocal l.3 at 0x7f23b2e9e908>
    """

    _other_fields = ("names",)

    def __init__(
        self,
        names: typing.List[str],
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param names: The names being declared as not local.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.names: typing.List[str] = names
        """The names being declared as not local."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def _infer_name(self, frame, name):
        return name


class Pass(mixins.NoChildrenMixin, Statement):
    """Class representing an :class:`ast.Pass` node.

    >>> import astroid
    >>> node = astroid.extract_node('pass')
    >>> node
    <Pass l.1 at 0x7f23b2e9e748>
    """


class Raise(Statement):
    """Class representing an :class:`ast.Raise` node.

    >>> import astroid
    >>> node = astroid.extract_node('raise RuntimeError("Something bad happened!")')
    >>> node
    <Raise l.1 at 0x7f23b2e9e828>
    """

    _astroid_fields = ("exc", "cause")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.exc: Optional[NodeNG] = None  # can be None
        """What is being raised."""

        self.cause: Optional[NodeNG] = None  # can be None
        """The exception being used to raise this one."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        exc: Optional[NodeNG] = None,
        cause: Optional[NodeNG] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param exc: What is being raised.

        :param cause: The exception being used to raise this one.
        """
        self.exc = exc
        self.cause = cause

    def raises_not_implemented(self):
        """Check if this node raises a :class:`NotImplementedError`.

        :returns: True if this node raises a :class:`NotImplementedError`,
            False otherwise.
        :rtype: bool
        """
        if not self.exc:
            return False
        return any(
            name.name == "NotImplementedError" for name in self.exc._get_name_nodes()
        )

    def get_children(self):
        if self.exc is not None:
            yield self.exc

        if self.cause is not None:
            yield self.cause


class Return(Statement):
    """Class representing an :class:`ast.Return` node.

    >>> import astroid
    >>> node = astroid.extract_node('return True')
    >>> node
    <Return l.1 at 0x7f23b8211908>
    """

    _astroid_fields = ("value",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.value: Optional[NodeNG] = None  # can be None
        """The value being returned."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, value: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param value: The value being returned.
        """
        self.value = value

    def get_children(self):
        if self.value is not None:
            yield self.value

    def is_tuple_return(self):
        return isinstance(self.value, Tuple)

    def _get_return_nodes_skip_functions(self):
        yield self


class Set(BaseContainer):
    """Class representing an :class:`ast.Set` node.

    >>> import astroid
    >>> node = astroid.extract_node('{1, 2, 3}')
    >>> node
    <Set.set l.1 at 0x7f23b2e71d68>
    """

    def pytype(self):
        """Get the name of the type that this node represents.

        :returns: The name of the type.
        :rtype: str
        """
        return "builtins.set"


class Slice(NodeNG):
    """Class representing an :class:`ast.Slice` node.

    >>> import astroid
    >>> node = astroid.extract_node('things[1:3]')
    >>> node
    <Subscript l.1 at 0x7f23b2e71f60>
    >>> node.slice
    <Slice l.1 at 0x7f23b2e71e80>
    """

    _astroid_fields = ("lower", "upper", "step")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.lower: Optional[NodeNG] = None  # can be None
        """The lower index in the slice."""

        self.upper: Optional[NodeNG] = None  # can be None
        """The upper index in the slice."""

        self.step: Optional[NodeNG] = None  # can be None
        """The step to take between indexes."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        lower: Optional[NodeNG] = None,
        upper: Optional[NodeNG] = None,
        step: Optional[NodeNG] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param lower: The lower index in the slice.

        :param upper: The upper index in the slice.

        :param step: The step to take between index.
        """
        self.lower = lower
        self.upper = upper
        self.step = step

    def _wrap_attribute(self, attr):
        """Wrap the empty attributes of the Slice in a Const node."""
        if not attr:
            const = const_factory(attr)
            const.parent = self
            return const
        return attr

    @decorators.cachedproperty
    def _proxied(self):
        builtins = AstroidManager().builtins_module
        return builtins.getattr("slice")[0]

    def pytype(self):
        """Get the name of the type that this node represents.

        :returns: The name of the type.
        :rtype: str
        """
        return "builtins.slice"

    def igetattr(self, attrname, context=None):
        """Infer the possible values of the given attribute on the slice.

        :param attrname: The name of the attribute to infer.
        :type attrname: str

        :returns: The inferred possible values.
        :rtype: iterable(NodeNG)
        """
        if attrname == "start":
            yield self._wrap_attribute(self.lower)
        elif attrname == "stop":
            yield self._wrap_attribute(self.upper)
        elif attrname == "step":
            yield self._wrap_attribute(self.step)
        else:
            yield from self.getattr(attrname, context=context)

    def getattr(self, attrname, context=None):
        return self._proxied.getattr(attrname, context)

    def get_children(self):
        if self.lower is not None:
            yield self.lower

        if self.upper is not None:
            yield self.upper

        if self.step is not None:
            yield self.step


class Starred(mixins.ParentAssignTypeMixin, NodeNG):
    """Class representing an :class:`ast.Starred` node.

    >>> import astroid
    >>> node = astroid.extract_node('*args')
    >>> node
    <Starred l.1 at 0x7f23b2e41978>
    """

    _astroid_fields = ("value",)
    _other_fields = ("ctx",)

    def __init__(
        self,
        ctx: Optional[Context] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param ctx: Whether the list is assigned to or loaded from.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.value: Optional[NodeNG] = None
        """What is being unpacked."""

        self.ctx: Optional[Context] = ctx
        """Whether the starred item is assigned to or loaded from."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, value: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param value: What is being unpacked.
        """
        self.value = value

    assigned_stmts: AssignedStmtsCall["Starred"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def get_children(self):
        yield self.value


class Subscript(NodeNG):
    """Class representing an :class:`ast.Subscript` node.

    >>> import astroid
    >>> node = astroid.extract_node('things[1:3]')
    >>> node
    <Subscript l.1 at 0x7f23b2e71f60>
    """

    _astroid_fields = ("value", "slice")
    _other_fields = ("ctx",)

    def __init__(
        self,
        ctx: Optional[Context] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param ctx: Whether the subscripted item is assigned to or loaded from.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.value: Optional[NodeNG] = None
        """What is being indexed."""

        self.slice: Optional[NodeNG] = None
        """The slice being used to lookup."""

        self.ctx: Optional[Context] = ctx
        """Whether the subscripted item is assigned to or loaded from."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    # pylint: disable=redefined-builtin; had to use the same name as builtin ast module.
    def postinit(
        self, value: Optional[NodeNG] = None, slice: Optional[NodeNG] = None
    ) -> None:
        """Do some setup after initialisation.

        :param value: What is being indexed.

        :param slice: The slice being used to lookup.
        """
        self.value = value
        self.slice = slice

    def get_children(self):
        yield self.value
        yield self.slice


class TryExcept(mixins.MultiLineBlockMixin, mixins.BlockRangeMixIn, Statement):
    """Class representing an :class:`ast.TryExcept` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
        try:
            do_something()
        except Exception as error:
            print("Error!")
        ''')
    >>> node
    <TryExcept l.2 at 0x7f23b2e9d908>
    """

    _astroid_fields = ("body", "handlers", "orelse")
    _multi_line_block_fields = ("body", "handlers", "orelse")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.body: typing.List[NodeNG] = []
        """The contents of the block to catch exceptions from."""

        self.handlers: typing.List[ExceptHandler] = []
        """The exception handlers."""

        self.orelse: typing.List[NodeNG] = []
        """The contents of the ``else`` block."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        body: Optional[typing.List[NodeNG]] = None,
        handlers: Optional[typing.List[ExceptHandler]] = None,
        orelse: Optional[typing.List[NodeNG]] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param body: The contents of the block to catch exceptions from.

        :param handlers: The exception handlers.

        :param orelse: The contents of the ``else`` block.
        """
        if body is not None:
            self.body = body
        if handlers is not None:
            self.handlers = handlers
        if orelse is not None:
            self.orelse = orelse

    def _infer_name(self, frame, name):
        return name

    def block_range(self, lineno):
        """Get a range from the given line number to where this node ends.

        :param lineno: The line number to start the range at.
        :type lineno: int

        :returns: The range of line numbers that this node belongs to,
            starting at the given line number.
        :rtype: tuple(int, int)
        """
        last = None
        for exhandler in self.handlers:
            if exhandler.type and lineno == exhandler.type.fromlineno:
                return lineno, lineno
            if exhandler.body[0].fromlineno <= lineno <= exhandler.body[-1].tolineno:
                return lineno, exhandler.body[-1].tolineno
            if last is None:
                last = exhandler.body[0].fromlineno - 1
        return self._elsed_block_range(lineno, self.orelse, last)

    def get_children(self):
        yield from self.body

        yield from self.handlers or ()
        yield from self.orelse or ()


class TryFinally(mixins.MultiLineBlockMixin, mixins.BlockRangeMixIn, Statement):
    """Class representing an :class:`ast.TryFinally` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    try:
        do_something()
    except Exception as error:
        print("Error!")
    finally:
        print("Cleanup!")
    ''')
    >>> node
    <TryFinally l.2 at 0x7f23b2e41d68>
    """

    _astroid_fields = ("body", "finalbody")
    _multi_line_block_fields = ("body", "finalbody")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.body: typing.Union[typing.List[TryExcept], typing.List[NodeNG]] = []
        """The try-except that the finally is attached to."""

        self.finalbody: typing.List[NodeNG] = []
        """The contents of the ``finally`` block."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        body: typing.Union[typing.List[TryExcept], typing.List[NodeNG], None] = None,
        finalbody: Optional[typing.List[NodeNG]] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param body: The try-except that the finally is attached to.

        :param finalbody: The contents of the ``finally`` block.
        """
        if body is not None:
            self.body = body
        if finalbody is not None:
            self.finalbody = finalbody

    def block_range(self, lineno):
        """Get a range from the given line number to where this node ends.

        :param lineno: The line number to start the range at.
        :type lineno: int

        :returns: The range of line numbers that this node belongs to,
            starting at the given line number.
        :rtype: tuple(int, int)
        """
        child = self.body[0]
        # py2.5 try: except: finally:
        if (
            isinstance(child, TryExcept)
            and child.fromlineno == self.fromlineno
            and child.tolineno >= lineno > self.fromlineno
        ):
            return child.block_range(lineno)
        return self._elsed_block_range(lineno, self.finalbody)

    def get_children(self):
        yield from self.body
        yield from self.finalbody


class Tuple(BaseContainer):
    """Class representing an :class:`ast.Tuple` node.

    >>> import astroid
    >>> node = astroid.extract_node('(1, 2, 3)')
    >>> node
    <Tuple.tuple l.1 at 0x7f23b2e41780>
    """

    _other_fields = ("ctx",)

    def __init__(
        self,
        ctx: Optional[Context] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param ctx: Whether the tuple is assigned to or loaded from.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.ctx: Optional[Context] = ctx
        """Whether the tuple is assigned to or loaded from."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    assigned_stmts: AssignedStmtsCall["Tuple"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def pytype(self):
        """Get the name of the type that this node represents.

        :returns: The name of the type.
        :rtype: str
        """
        return "builtins.tuple"

    def getitem(self, index, context=None):
        """Get an item from this node.

        :param index: The node to use as a subscript index.
        :type index: Const or Slice
        """
        return _container_getitem(self, self.elts, index, context=context)


class UnaryOp(NodeNG):
    """Class representing an :class:`ast.UnaryOp` node.

    >>> import astroid
    >>> node = astroid.extract_node('-5')
    >>> node
    <UnaryOp l.1 at 0x7f23b2e4e198>
    """

    _astroid_fields = ("operand",)
    _other_fields = ("op",)

    @decorators.deprecate_default_argument_values(op="str")
    def __init__(
        self,
        op: Optional[str] = None,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param op: The operator.

        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.op: Optional[str] = op
        """The operator."""

        self.operand: Optional[NodeNG] = None
        """What the unary operator is applied to."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, operand: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param operand: What the unary operator is applied to.
        """
        self.operand = operand

    # This is set by inference.py
    def _infer_unaryop(self, context=None):
        raise NotImplementedError

    def type_errors(self, context=None):
        """Get a list of type errors which can occur during inference.

        Each TypeError is represented by a :class:`BadBinaryOperationMessage`,
        which holds the original exception.

        :returns: The list of possible type errors.
        :rtype: list(BadBinaryOperationMessage)
        """
        try:
            results = self._infer_unaryop(context=context)
            return [
                result
                for result in results
                if isinstance(result, util.BadUnaryOperationMessage)
            ]
        except InferenceError:
            return []

    def get_children(self):
        yield self.operand

    def op_precedence(self):
        if self.op == "not":
            return OP_PRECEDENCE[self.op]

        return super().op_precedence()


class While(mixins.MultiLineBlockMixin, mixins.BlockRangeMixIn, Statement):
    """Class representing an :class:`ast.While` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    while condition():
        print("True")
    ''')
    >>> node
    <While l.2 at 0x7f23b2e4e390>
    """

    _astroid_fields = ("test", "body", "orelse")
    _multi_line_block_fields = ("body", "orelse")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.test: Optional[NodeNG] = None
        """The condition that the loop tests."""

        self.body: typing.List[NodeNG] = []
        """The contents of the loop."""

        self.orelse: typing.List[NodeNG] = []
        """The contents of the ``else`` block."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        test: Optional[NodeNG] = None,
        body: Optional[typing.List[NodeNG]] = None,
        orelse: Optional[typing.List[NodeNG]] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param test: The condition that the loop tests.

        :param body: The contents of the loop.

        :param orelse: The contents of the ``else`` block.
        """
        self.test = test
        if body is not None:
            self.body = body
        if orelse is not None:
            self.orelse = orelse

    @decorators.cachedproperty
    def blockstart_tolineno(self):
        """The line on which the beginning of this block ends.

        :type: int
        """
        return self.test.tolineno

    def block_range(self, lineno):
        """Get a range from the given line number to where this node ends.

        :param lineno: The line number to start the range at.
        :type lineno: int

        :returns: The range of line numbers that this node belongs to,
            starting at the given line number.
        :rtype: tuple(int, int)
        """
        return self._elsed_block_range(lineno, self.orelse)

    def get_children(self):
        yield self.test

        yield from self.body
        yield from self.orelse

    def _get_yield_nodes_skip_lambdas(self):
        """A While node can contain a Yield node in the test"""
        yield from self.test._get_yield_nodes_skip_lambdas()
        yield from super()._get_yield_nodes_skip_lambdas()


class With(
    mixins.MultiLineBlockMixin,
    mixins.BlockRangeMixIn,
    mixins.AssignTypeMixin,
    Statement,
):
    """Class representing an :class:`ast.With` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    with open(file_path) as file_:
        print(file_.read())
    ''')
    >>> node
    <With l.2 at 0x7f23b2e4e710>
    """

    _astroid_fields = ("items", "body")
    _other_other_fields = ("type_annotation",)
    _multi_line_block_fields = ("body",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.items: typing.List[typing.Tuple[NodeNG, Optional[NodeNG]]] = []
        """The pairs of context managers and the names they are assigned to."""

        self.body: typing.List[NodeNG] = []
        """The contents of the ``with`` block."""

        self.type_annotation: Optional[NodeNG] = None  # can be None
        """If present, this will contain the type annotation passed by a type comment"""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        items: Optional[typing.List[typing.Tuple[NodeNG, Optional[NodeNG]]]] = None,
        body: Optional[typing.List[NodeNG]] = None,
        type_annotation: Optional[NodeNG] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param items: The pairs of context managers and the names
            they are assigned to.

        :param body: The contents of the ``with`` block.
        """
        if items is not None:
            self.items = items
        if body is not None:
            self.body = body
        self.type_annotation = type_annotation

    assigned_stmts: AssignedStmtsCall["With"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    @decorators.cachedproperty
    def blockstart_tolineno(self):
        """The line on which the beginning of this block ends.

        :type: int
        """
        return self.items[-1][0].tolineno

    def get_children(self):
        """Get the child nodes below this node.

        :returns: The children.
        :rtype: iterable(NodeNG)
        """
        for expr, var in self.items:
            yield expr
            if var:
                yield var
        yield from self.body


class AsyncWith(With):
    """Asynchronous ``with`` built with the ``async`` keyword."""


class Yield(NodeNG):
    """Class representing an :class:`ast.Yield` node.

    >>> import astroid
    >>> node = astroid.extract_node('yield True')
    >>> node
    <Yield l.1 at 0x7f23b2e4e5f8>
    """

    _astroid_fields = ("value",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.value: Optional[NodeNG] = None  # can be None
        """The value to yield."""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, value: Optional[NodeNG] = None) -> None:
        """Do some setup after initialisation.

        :param value: The value to yield.
        """
        self.value = value

    def get_children(self):
        if self.value is not None:
            yield self.value

    def _get_yield_nodes_skip_lambdas(self):
        yield self


class YieldFrom(Yield):  # TODO value is required, not optional
    """Class representing an :class:`ast.YieldFrom` node."""


class DictUnpack(mixins.NoChildrenMixin, NodeNG):
    """Represents the unpacking of dicts into dicts using :pep:`448`."""


class FormattedValue(NodeNG):
    """Class representing an :class:`ast.FormattedValue` node.

    Represents a :pep:`498` format string.

    >>> import astroid
    >>> node = astroid.extract_node('f"Format {type_}"')
    >>> node
    <JoinedStr l.1 at 0x7f23b2e4ed30>
    >>> node.values
    [<Const.str l.1 at 0x7f23b2e4eda0>, <FormattedValue l.1 at 0x7f23b2e4edd8>]
    """

    _astroid_fields = ("value", "format_spec")
    _other_fields = ("conversion",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.value: NodeNG
        """The value to be formatted into the string."""

        self.conversion: Optional[int] = None  # can be None
        """The type of formatting to be applied to the value.

        .. seealso::
            :class:`ast.FormattedValue`
        """

        self.format_spec: Optional[NodeNG] = None  # can be None
        """The formatting to be applied to the value.

        .. seealso::
            :class:`ast.FormattedValue`

        :type: JoinedStr or None
        """

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        value: NodeNG,
        conversion: Optional[int] = None,
        format_spec: Optional[NodeNG] = None,
    ) -> None:
        """Do some setup after initialisation.

        :param value: The value to be formatted into the string.

        :param conversion: The type of formatting to be applied to the value.

        :param format_spec: The formatting to be applied to the value.
        :type format_spec: JoinedStr or None
        """
        self.value = value
        self.conversion = conversion
        self.format_spec = format_spec

    def get_children(self):
        yield self.value

        if self.format_spec is not None:
            yield self.format_spec


class JoinedStr(NodeNG):
    """Represents a list of string expressions to be joined.

    >>> import astroid
    >>> node = astroid.extract_node('f"Format {type_}"')
    >>> node
    <JoinedStr l.1 at 0x7f23b2e4ed30>
    """

    _astroid_fields = ("values",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.values: typing.List[NodeNG] = []
        """The string expressions to be joined.

        :type: list(FormattedValue or Const)
        """

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, values: Optional[typing.List[NodeNG]] = None) -> None:
        """Do some setup after initialisation.

        :param value: The string expressions to be joined.

        :type: list(FormattedValue or Const)
        """
        if values is not None:
            self.values = values

    def get_children(self):
        yield from self.values


class NamedExpr(mixins.AssignTypeMixin, NodeNG):
    """Represents the assignment from the assignment expression

    >>> import astroid
    >>> module = astroid.parse('if a := 1: pass')
    >>> module.body[0].test
    <NamedExpr l.1 at 0x7f23b2e4ed30>
    """

    _astroid_fields = ("target", "value")

    optional_assign = True
    """Whether this node optionally assigns a variable.

    Since NamedExpr are not always called they do not always assign."""

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """
        :param lineno: The line that this node appears on in the source code.

        :param col_offset: The column that this node appears on in the
            source code.

        :param parent: The parent node in the syntax tree.

        :param end_lineno: The last line this node appears on in the source code.

        :param end_col_offset: The end column this node appears on in the
            source code. Note: This is after the last symbol.
        """
        self.target: NodeNG
        """The assignment target

        :type: Name
        """

        self.value: NodeNG
        """The value that gets assigned in the expression"""

        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, target: NodeNG, value: NodeNG) -> None:
        self.target = target
        self.value = value

    assigned_stmts: AssignedStmtsCall["NamedExpr"]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """

    def frame(
        self, *, future: Literal[None, True] = None
    ) -> Union["nodes.FunctionDef", "nodes.Module", "nodes.ClassDef", "nodes.Lambda"]:
        """The first parent frame node.

        A frame node is a :class:`Module`, :class:`FunctionDef`,
        or :class:`ClassDef`.

        :returns: The first parent frame node.
        """
        if not self.parent:
            raise ParentMissingError(target=self)

        # For certain parents NamedExpr evaluate to the scope of the parent
        if isinstance(self.parent, (Arguments, Keyword, Comprehension)):
            if not self.parent.parent:
                raise ParentMissingError(target=self.parent)
            if not self.parent.parent.parent:
                raise ParentMissingError(target=self.parent.parent)
            return self.parent.parent.parent.frame(future=True)

        return self.parent.frame(future=True)

    def scope(self) -> "LocalsDictNodeNG":
        """The first parent node defining a new scope.
        These can be Module, FunctionDef, ClassDef, Lambda, or GeneratorExp nodes.

        :returns: The first parent scope node.
        """
        if not self.parent:
            raise ParentMissingError(target=self)

        # For certain parents NamedExpr evaluate to the scope of the parent
        if isinstance(self.parent, (Arguments, Keyword, Comprehension)):
            if not self.parent.parent:
                raise ParentMissingError(target=self.parent)
            if not self.parent.parent.parent:
                raise ParentMissingError(target=self.parent.parent)
            return self.parent.parent.parent.scope()

        return self.parent.scope()

    def set_local(self, name: str, stmt: AssignName) -> None:
        """Define that the given name is declared in the given statement node.
        NamedExpr's in Arguments, Keyword or Comprehension are evaluated in their
        parent's parent scope. So we add to their frame's locals.

        .. seealso:: :meth:`scope`

        :param name: The name that is being defined.

        :param stmt: The statement that defines the given name.
        """
        self.frame(future=True).set_local(name, stmt)


class Unknown(mixins.AssignTypeMixin, NodeNG):
    """This node represents a node in a constructed AST where
    introspection is not possible.  At the moment, it's only used in
    the args attribute of FunctionDef nodes where function signature
    introspection failed.
    """

    name = "Unknown"

    def qname(self):
        return "Unknown"

    def _infer(self, context=None, **kwargs):
        """Inference on an Unknown node immediately terminates."""
        yield util.Uninferable


class EvaluatedObject(NodeNG):
    """Contains an object that has already been inferred

    This class is useful to pre-evaluate a particular node,
    with the resulting class acting as the non-evaluated node.
    """

    name = "EvaluatedObject"
    _astroid_fields = ("original",)
    _other_fields = ("value",)

    def __init__(
        self, original: NodeNG, value: typing.Union[NodeNG, util.Uninferable]
    ) -> None:
        self.original: NodeNG = original
        """The original node that has already been evaluated"""

        self.value: typing.Union[NodeNG, util.Uninferable] = value
        """The inferred value"""

        super().__init__(
            lineno=self.original.lineno,
            col_offset=self.original.col_offset,
            parent=self.original.parent,
        )

    def infer(self, context=None, **kwargs):
        yield self.value


# Pattern matching #######################################################


class Match(Statement):
    """Class representing a :class:`ast.Match` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case 200:
            ...
        case _:
            ...
    ''')
    >>> node
    <Match l.2 at 0x10c24e170>
    """

    _astroid_fields = ("subject", "cases")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.subject: NodeNG
        self.cases: typing.List["MatchCase"]
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        *,
        subject: NodeNG,
        cases: typing.List["MatchCase"],
    ) -> None:
        self.subject = subject
        self.cases = cases


class Pattern(NodeNG):
    """Base class for all Pattern nodes."""


class MatchCase(mixins.MultiLineBlockMixin, NodeNG):
    """Class representing a :class:`ast.match_case` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case 200:
            ...
    ''')
    >>> node.cases[0]
    <MatchCase l.3 at 0x10c24e590>
    """

    _astroid_fields = ("pattern", "guard", "body")
    _multi_line_block_fields = ("body",)

    lineno: None
    col_offset: None
    end_lineno: None
    end_col_offset: None

    def __init__(self, *, parent: Optional[NodeNG] = None) -> None:
        self.pattern: Pattern
        self.guard: Optional[NodeNG]
        self.body: typing.List[NodeNG]
        super().__init__(parent=parent)

    def postinit(
        self,
        *,
        pattern: Pattern,
        guard: Optional[NodeNG],
        body: typing.List[NodeNG],
    ) -> None:
        self.pattern = pattern
        self.guard = guard
        self.body = body


class MatchValue(Pattern):
    """Class representing a :class:`ast.MatchValue` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case 200:
            ...
    ''')
    >>> node.cases[0].pattern
    <MatchValue l.3 at 0x10c24e200>
    """

    _astroid_fields = ("value",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.value: NodeNG
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, *, value: NodeNG) -> None:
        self.value = value


class MatchSingleton(Pattern):
    """Class representing a :class:`ast.MatchSingleton` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case True:
            ...
        case False:
            ...
        case None:
            ...
    ''')
    >>> node.cases[0].pattern
    <MatchSingleton l.3 at 0x10c2282e0>
    >>> node.cases[1].pattern
    <MatchSingleton l.5 at 0x10c228af0>
    >>> node.cases[2].pattern
    <MatchSingleton l.7 at 0x10c229f90>
    """

    _other_fields = ("value",)

    def __init__(
        self,
        *,
        value: Literal[True, False, None],
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
    ) -> None:
        self.value = value
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )


class MatchSequence(Pattern):
    """Class representing a :class:`ast.MatchSequence` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case [1, 2]:
            ...
        case (1, 2, *_):
            ...
    ''')
    >>> node.cases[0].pattern
    <MatchSequence l.3 at 0x10ca80d00>
    >>> node.cases[1].pattern
    <MatchSequence l.5 at 0x10ca80b20>
    """

    _astroid_fields = ("patterns",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.patterns: typing.List[Pattern]
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, *, patterns: typing.List[Pattern]) -> None:
        self.patterns = patterns


class MatchMapping(mixins.AssignTypeMixin, Pattern):
    """Class representing a :class:`ast.MatchMapping` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case {1: "Hello", 2: "World", 3: _, **rest}:
            ...
    ''')
    >>> node.cases[0].pattern
    <MatchMapping l.3 at 0x10c8a8850>
    """

    _astroid_fields = ("keys", "patterns", "rest")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.keys: typing.List[NodeNG]
        self.patterns: typing.List[Pattern]
        self.rest: Optional[AssignName]
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        *,
        keys: typing.List[NodeNG],
        patterns: typing.List[Pattern],
        rest: Optional[AssignName],
    ) -> None:
        self.keys = keys
        self.patterns = patterns
        self.rest = rest

    assigned_stmts: Callable[
        [
            "MatchMapping",
            AssignName,
            Optional[InferenceContext],
            Literal[None],
        ],
        Generator[NodeNG, None, None],
    ]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """


class MatchClass(Pattern):
    """Class representing a :class:`ast.MatchClass` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case Point2D(0, 0):
            ...
        case Point3D(x=0, y=0, z=0):
            ...
    ''')
    >>> node.cases[0].pattern
    <MatchClass l.3 at 0x10ca83940>
    >>> node.cases[1].pattern
    <MatchClass l.5 at 0x10ca80880>
    """

    _astroid_fields = ("cls", "patterns", "kwd_patterns")
    _other_fields = ("kwd_attrs",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.cls: NodeNG
        self.patterns: typing.List[Pattern]
        self.kwd_attrs: typing.List[str]
        self.kwd_patterns: typing.List[Pattern]
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        *,
        cls: NodeNG,
        patterns: typing.List[Pattern],
        kwd_attrs: typing.List[str],
        kwd_patterns: typing.List[Pattern],
    ) -> None:
        self.cls = cls
        self.patterns = patterns
        self.kwd_attrs = kwd_attrs
        self.kwd_patterns = kwd_patterns


class MatchStar(mixins.AssignTypeMixin, Pattern):
    """Class representing a :class:`ast.MatchStar` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case [1, *_]:
            ...
    ''')
    >>> node.cases[0].pattern.patterns[1]
    <MatchStar l.3 at 0x10ca809a0>
    """

    _astroid_fields = ("name",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.name: Optional[AssignName]
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, *, name: Optional[AssignName]) -> None:
        self.name = name

    assigned_stmts: Callable[
        [
            "MatchStar",
            AssignName,
            Optional[InferenceContext],
            Literal[None],
        ],
        Generator[NodeNG, None, None],
    ]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """


class MatchAs(mixins.AssignTypeMixin, Pattern):
    """Class representing a :class:`ast.MatchAs` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case [1, a]:
            ...
        case {'key': b}:
            ...
        case Point2D(0, 0) as c:
            ...
        case d:
            ...
    ''')
    >>> node.cases[0].pattern.patterns[1]
    <MatchAs l.3 at 0x10d0b2da0>
    >>> node.cases[1].pattern.patterns[0]
    <MatchAs l.5 at 0x10d0b2920>
    >>> node.cases[2].pattern
    <MatchAs l.7 at 0x10d0b06a0>
    >>> node.cases[3].pattern
    <MatchAs l.9 at 0x10d09b880>
    """

    _astroid_fields = ("pattern", "name")

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.pattern: Optional[Pattern]
        self.name: Optional[AssignName]
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(
        self,
        *,
        pattern: Optional[Pattern],
        name: Optional[AssignName],
    ) -> None:
        self.pattern = pattern
        self.name = name

    assigned_stmts: Callable[
        [
            "MatchAs",
            AssignName,
            Optional[InferenceContext],
            Literal[None],
        ],
        Generator[NodeNG, None, None],
    ]
    """Returns the assigned statement (non inferred) according to the assignment type.
    See astroid/protocols.py for actual implementation.
    """


class MatchOr(Pattern):
    """Class representing a :class:`ast.MatchOr` node.

    >>> import astroid
    >>> node = astroid.extract_node('''
    match x:
        case 400 | 401 | 402:
            ...
    ''')
    >>> node.cases[0].pattern
    <MatchOr l.3 at 0x10d0b0b50>
    """

    _astroid_fields = ("patterns",)

    def __init__(
        self,
        lineno: Optional[int] = None,
        col_offset: Optional[int] = None,
        parent: Optional[NodeNG] = None,
        *,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.patterns: typing.List[Pattern]
        super().__init__(
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )

    def postinit(self, *, patterns: typing.List[Pattern]) -> None:
        self.patterns = patterns


# constants ##############################################################

CONST_CLS = {
    list: List,
    tuple: Tuple,
    dict: Dict,
    set: Set,
    type(None): Const,
    type(NotImplemented): Const,
    type(...): Const,
}


def _update_const_classes():
    """update constant classes, so the keys of CONST_CLS can be reused"""
    klasses = (bool, int, float, complex, str, bytes)
    for kls in klasses:
        CONST_CLS[kls] = Const


_update_const_classes()


def _two_step_initialization(cls, value):
    instance = cls()
    instance.postinit(value)
    return instance


def _dict_initialization(cls, value):
    if isinstance(value, dict):
        value = tuple(value.items())
    return _two_step_initialization(cls, value)


_CONST_CLS_CONSTRUCTORS = {
    List: _two_step_initialization,
    Tuple: _two_step_initialization,
    Dict: _dict_initialization,
    Set: _two_step_initialization,
    Const: lambda cls, value: cls(value),
}


def const_factory(value):
    """return an astroid node for a python value"""
    # XXX we should probably be stricter here and only consider stuff in
    # CONST_CLS or do better treatment: in case where value is not in CONST_CLS,
    # we should rather recall the builder on this value than returning an empty
    # node (another option being that const_factory shouldn't be called with something
    # not in CONST_CLS)
    assert not isinstance(value, NodeNG)

    # Hack for ignoring elements of a sequence
    # or a mapping, in order to avoid transforming
    # each element to an AST. This is fixed in 2.0
    # and this approach is a temporary hack.
    if isinstance(value, (list, set, tuple, dict)):
        elts = []
    else:
        elts = value

    try:
        initializer_cls = CONST_CLS[value.__class__]
        initializer = _CONST_CLS_CONSTRUCTORS[initializer_cls]
        return initializer(initializer_cls, elts)
    except (KeyError, AttributeError):
        node = EmptyNode()
        node.object = value
        return node
