# Copyright (c) 2006-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2009 James Lingard <jchl@aristanetworks.com>
# Copyright (c) 2012-2014 Google, Inc.
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 David Shea <dshea@redhat.com>
# Copyright (c) 2014 Steven Myint <hg@stevenmyint.com>
# Copyright (c) 2014 Holger Peters <email@holger-peters.de>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Anentropic <ego@anentropic.com>
# Copyright (c) 2015 Dmitry Pribysh <dmand@yandex.ru>
# Copyright (c) 2015 Rene Zhang <rz99@cornell.edu>
# Copyright (c) 2015 Radu Ciorba <radu@devrandom.ro>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016, 2019 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2016 Alexander Todorov <atodorov@otb.bg>
# Copyright (c) 2016 Jürgen Hermann <jh@web.de>
# Copyright (c) 2016 Jakub Wilk <jwilk@jwilk.net>
# Copyright (c) 2016 Filipe Brandenburger <filbranden@google.com>
# Copyright (c) 2017, 2021 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2017-2018, 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2017 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2018-2019, 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018 Pablo Galindo <Pablogsal@gmail.com>
# Copyright (c) 2018 Jim Robertson <jrobertson98atx@gmail.com>
# Copyright (c) 2018 Lucas Cimon <lucas.cimon@gmail.com>
# Copyright (c) 2018 Mike Frysinger <vapier@gmail.com>
# Copyright (c) 2018 Ben Green <benhgreen@icloud.com>
# Copyright (c) 2018 Konstantin <Github@pheanex.de>
# Copyright (c) 2018 Justin Li <justinnhli@users.noreply.github.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019 Andy Palmer <25123779+ninezerozeronine@users.noreply.github.com>
# Copyright (c) 2019 mattlbeck <17108752+mattlbeck@users.noreply.github.com>
# Copyright (c) 2019 Martin Vielsmaier <martin.vielsmaier@gmail.com>
# Copyright (c) 2019 Santiago Castro <bryant@montevideo.com.uy>
# Copyright (c) 2019 yory8 <39745367+yory8@users.noreply.github.com>
# Copyright (c) 2019 Federico Bond <federicobond@gmail.com>
# Copyright (c) 2019 Pascal Corpet <pcorpet@users.noreply.github.com>
# Copyright (c) 2020 Peter Kolbus <peter.kolbus@gmail.com>
# Copyright (c) 2020 Julien Palard <julien@palard.fr>
# Copyright (c) 2020 Ram Rachum <ram@rachum.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2020 Anubhav <35621759+anubh-v@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Tushar Sadhwani <tushar.sadhwani000@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 David Liu <david@cs.toronto.edu>
# Copyright (c) 2021 doranid <ddandd@gmail.com>
# Copyright (c) 2021 Yu Shao, Pang <36848472+yushao2@users.noreply.github.com>
# Copyright (c) 2021 Andrew Haigh <nelfin@gmail.com>
# Copyright (c) 2021 Jens H. Nielsen <Jens.Nielsen@microsoft.com>
# Copyright (c) 2021 Ikraduya Edian <ikraduya@gmail.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""try to find more bugs in the code using astroid inference capabilities
"""

import fnmatch
import heapq
import itertools
import operator
import re
import shlex
import sys
import types
from collections import deque
from collections.abc import Sequence
from functools import singledispatch
from typing import Any, Callable, Iterator, List, Optional, Pattern, Tuple

from metaflow._vendor import astroid
from metaflow._vendor.astroid import bases, nodes

from metaflow._vendor.pylint.checkers import BaseChecker, utils
from metaflow._vendor.pylint.checkers.utils import (
    check_messages,
    decorated_with,
    decorated_with_property,
    has_known_bases,
    is_builtin_object,
    is_classdef_type,
    is_comprehension,
    is_inside_abstract_class,
    is_iterable,
    is_mapping,
    is_overload_stub,
    is_postponed_evaluation_enabled,
    is_super,
    node_ignores_exception,
    safe_infer,
    supports_delitem,
    supports_getitem,
    supports_membership_test,
    supports_setitem,
)
from metaflow._vendor.pylint.interfaces import INFERENCE, IAstroidChecker
from metaflow._vendor.pylint.utils import get_global_option

STR_FORMAT = {"builtins.str.format"}
ASYNCIO_COROUTINE = "asyncio.coroutines.coroutine"
BUILTIN_TUPLE = "builtins.tuple"
TYPE_ANNOTATION_NODES_TYPES = (
    nodes.AnnAssign,
    nodes.Arguments,
    nodes.FunctionDef,
)


def _unflatten(iterable):
    for index, elem in enumerate(iterable):
        if isinstance(elem, Sequence) and not isinstance(elem, str):
            yield from _unflatten(elem)
        elif elem and not index:
            # We're interested only in the first element.
            yield elem


def _flatten_container(iterable):
    # Flatten nested containers into a single iterable
    for item in iterable:
        if isinstance(item, (list, tuple, types.GeneratorType)):
            yield from _flatten_container(item)
        else:
            yield item


def _is_owner_ignored(owner, attrname, ignored_classes, ignored_modules):
    """Check if the given owner should be ignored

    This will verify if the owner's module is in *ignored_modules*
    or the owner's module fully qualified name is in *ignored_modules*
    or if the *ignored_modules* contains a pattern which catches
    the fully qualified name of the module.

    Also, similar checks are done for the owner itself, if its name
    matches any name from the *ignored_classes* or if its qualified
    name can be found in *ignored_classes*.
    """
    ignored_modules = set(ignored_modules)
    module_name = owner.root().name
    module_qname = owner.root().qname()

    for ignore in ignored_modules:
        # Try to match the module name / fully qualified name directly
        if module_qname in ignored_modules or module_name in ignored_modules:
            return True

        # Try to see if the ignores pattern match against the module name.
        if fnmatch.fnmatch(module_qname, ignore):
            return True

        # Otherwise we might have a root module name being ignored,
        # and the qualified owner has more levels of depth.
        parts = deque(module_name.split("."))
        current_module = ""

        while parts:
            part = parts.popleft()
            if not current_module:
                current_module = part
            else:
                current_module += f".{part}"
            if current_module in ignored_modules:
                return True

    # Match against ignored classes.
    ignored_classes = set(ignored_classes)
    qname = owner.qname() if hasattr(owner, "qname") else ""
    return any(ignore in (attrname, qname) for ignore in ignored_classes)


@singledispatch
def _node_names(node):
    if not hasattr(node, "locals"):
        return []
    return node.locals.keys()


@_node_names.register(nodes.ClassDef)
@_node_names.register(astroid.Instance)
def _(node):
    values = itertools.chain(node.instance_attrs.keys(), node.locals.keys())

    try:
        mro = node.mro()[1:]
    except (NotImplementedError, TypeError, astroid.MroError):
        mro = node.ancestors()

    other_values = [value for cls in mro for value in _node_names(cls)]
    return itertools.chain(values, other_values)


def _string_distance(seq1, seq2):
    seq2_length = len(seq2)

    row = list(range(1, seq2_length + 1)) + [0]
    for seq1_index, seq1_char in enumerate(seq1):
        last_row = row
        row = [0] * seq2_length + [seq1_index + 1]

        for seq2_index, seq2_char in enumerate(seq2):
            row[seq2_index] = min(
                last_row[seq2_index] + 1,
                row[seq2_index - 1] + 1,
                last_row[seq2_index - 1] + (seq1_char != seq2_char),
            )

    return row[seq2_length - 1]


def _similar_names(owner, attrname, distance_threshold, max_choices):
    """Given an owner and a name, try to find similar names

    The similar names are searched given a distance metric and only
    a given number of choices will be returned.
    """
    possible_names = []
    names = _node_names(owner)

    for name in names:
        if name == attrname:
            continue

        distance = _string_distance(attrname, name)
        if distance <= distance_threshold:
            possible_names.append((name, distance))

    # Now get back the values with a minimum, up to the given
    # limit or choices.
    picked = [
        name
        for (name, _) in heapq.nsmallest(
            max_choices, possible_names, key=operator.itemgetter(1)
        )
    ]
    return sorted(picked)


def _missing_member_hint(owner, attrname, distance_threshold, max_choices):
    names = _similar_names(owner, attrname, distance_threshold, max_choices)
    if not names:
        # No similar name.
        return ""

    names = [repr(name) for name in names]
    if len(names) == 1:
        names = ", ".join(names)
    else:
        names = f"one of {', '.join(names[:-1])} or {names[-1]}"

    return f"; maybe {names}?"


MSGS = {
    "E1101": (
        "%s %r has no %r member%s",
        "no-member",
        "Used when a variable is accessed for an unexistent member.",
        {"old_names": [("E1103", "maybe-no-member")]},
    ),
    "I1101": (
        "%s %r has no %r member%s, but source is unavailable. Consider "
        "adding this module to extension-pkg-allow-list if you want "
        "to perform analysis based on run-time introspection of living objects.",
        "c-extension-no-member",
        "Used when a variable is accessed for non-existent member of C "
        "extension. Due to unavailability of source static analysis is impossible, "
        "but it may be performed by introspecting living objects in run-time.",
    ),
    "E1102": (
        "%s is not callable",
        "not-callable",
        "Used when an object being called has been inferred to a non "
        "callable object.",
    ),
    "E1111": (
        "Assigning result of a function call, where the function has no return",
        "assignment-from-no-return",
        "Used when an assignment is done on a function call but the "
        "inferred function doesn't return anything.",
    ),
    "E1120": (
        "No value for argument %s in %s call",
        "no-value-for-parameter",
        "Used when a function call passes too few arguments.",
    ),
    "E1121": (
        "Too many positional arguments for %s call",
        "too-many-function-args",
        "Used when a function call passes too many positional arguments.",
    ),
    "E1123": (
        "Unexpected keyword argument %r in %s call",
        "unexpected-keyword-arg",
        "Used when a function call passes a keyword argument that "
        "doesn't correspond to one of the function's parameter names.",
    ),
    "E1124": (
        "Argument %r passed by position and keyword in %s call",
        "redundant-keyword-arg",
        "Used when a function call would result in assigning multiple "
        "values to a function parameter, one value from a positional "
        "argument and one from a keyword argument.",
    ),
    "E1125": (
        "Missing mandatory keyword argument %r in %s call",
        "missing-kwoa",
        (
            "Used when a function call does not pass a mandatory"
            " keyword-only argument."
        ),
    ),
    "E1126": (
        "Sequence index is not an int, slice, or instance with __index__",
        "invalid-sequence-index",
        "Used when a sequence type is indexed with an invalid type. "
        "Valid types are ints, slices, and objects with an __index__ "
        "method.",
    ),
    "E1127": (
        "Slice index is not an int, None, or instance with __index__",
        "invalid-slice-index",
        "Used when a slice index is not an integer, None, or an object "
        "with an __index__ method.",
    ),
    "E1128": (
        "Assigning result of a function call, where the function returns None",
        "assignment-from-none",
        "Used when an assignment is done on a function call but the "
        "inferred function returns nothing but None.",
        {"old_names": [("W1111", "old-assignment-from-none")]},
    ),
    "E1129": (
        "Context manager '%s' doesn't implement __enter__ and __exit__.",
        "not-context-manager",
        "Used when an instance in a with statement doesn't implement "
        "the context manager protocol(__enter__/__exit__).",
    ),
    "E1130": (
        "%s",
        "invalid-unary-operand-type",
        "Emitted when a unary operand is used on an object which does not "
        "support this type of operation.",
    ),
    "E1131": (
        "%s",
        "unsupported-binary-operation",
        "Emitted when a binary arithmetic operation between two "
        "operands is not supported.",
    ),
    "E1132": (
        "Got multiple values for keyword argument %r in function call",
        "repeated-keyword",
        "Emitted when a function call got multiple values for a keyword.",
    ),
    "E1135": (
        "Value '%s' doesn't support membership test",
        "unsupported-membership-test",
        "Emitted when an instance in membership test expression doesn't "
        "implement membership protocol (__contains__/__iter__/__getitem__).",
    ),
    "E1136": (
        "Value '%s' is unsubscriptable",
        "unsubscriptable-object",
        "Emitted when a subscripted value doesn't support subscription "
        "(i.e. doesn't define __getitem__ method or __class_getitem__ for a class).",
    ),
    "E1137": (
        "%r does not support item assignment",
        "unsupported-assignment-operation",
        "Emitted when an object does not support item assignment "
        "(i.e. doesn't define __setitem__ method).",
    ),
    "E1138": (
        "%r does not support item deletion",
        "unsupported-delete-operation",
        "Emitted when an object does not support item deletion "
        "(i.e. doesn't define __delitem__ method).",
    ),
    "E1139": (
        "Invalid metaclass %r used",
        "invalid-metaclass",
        "Emitted whenever we can detect that a class is using, "
        "as a metaclass, something which might be invalid for using as "
        "a metaclass.",
    ),
    "E1140": (
        "Dict key is unhashable",
        "unhashable-dict-key",
        "Emitted when a dict key is not hashable "
        "(i.e. doesn't define __hash__ method).",
    ),
    "E1141": (
        "Unpacking a dictionary in iteration without calling .items()",
        "dict-iter-missing-items",
        "Emitted when trying to iterate through a dict without calling .items()",
    ),
    "E1142": (
        "'await' should be used within an async function",
        "await-outside-async",
        "Emitted when await is used outside an async function.",
    ),
    "W1113": (
        "Keyword argument before variable positional arguments list "
        "in the definition of %s function",
        "keyword-arg-before-vararg",
        "When defining a keyword argument before variable positional arguments, one can "
        "end up in having multiple values passed for the aforementioned parameter in "
        "case the method is called with keyword arguments.",
    ),
    "W1114": (
        "Positional arguments appear to be out of order",
        "arguments-out-of-order",
        "Emitted  when the caller's argument names fully match the parameter "
        "names in the function signature but do not have the same order.",
    ),
    "W1115": (
        "Non-string value assigned to __name__",
        "non-str-assignment-to-dunder-name",
        "Emitted when a non-string value is assigned to __name__",
    ),
    "W1116": (
        "Second argument of isinstance is not a type",
        "isinstance-second-argument-not-valid-type",
        "Emitted when the second argument of an isinstance call is not a type.",
    ),
}

# builtin sequence types in Python 2 and 3.
SEQUENCE_TYPES = {
    "str",
    "unicode",
    "list",
    "tuple",
    "bytearray",
    "xrange",
    "range",
    "bytes",
    "memoryview",
}


def _emit_no_member(
    node,
    owner,
    owner_name,
    mixin_class_rgx: Pattern[str],
    ignored_mixins=True,
    ignored_none=True,
):
    """Try to see if no-member should be emitted for the given owner.

    The following cases are ignored:

        * the owner is a function and it has decorators.
        * the owner is an instance and it has __getattr__, __getattribute__ implemented
        * the module is explicitly ignored from no-member checks
        * the owner is a class and the name can be found in its metaclass.
        * The access node is protected by an except handler, which handles
          AttributeError, Exception or bare except.
        * The node is guarded behind and `IF` or `IFExp` node
    """
    # pylint: disable=too-many-return-statements
    if node_ignores_exception(node, AttributeError):
        return False
    if ignored_none and isinstance(owner, nodes.Const) and owner.value is None:
        return False
    if is_super(owner) or getattr(owner, "type", None) == "metaclass":
        return False
    if owner_name and ignored_mixins and mixin_class_rgx.match(owner_name):
        return False
    if isinstance(owner, nodes.FunctionDef) and (
        owner.decorators or owner.is_abstract()
    ):
        return False
    if isinstance(owner, (astroid.Instance, nodes.ClassDef)):
        if owner.has_dynamic_getattr():
            # Issue #2565: Don't ignore enums, as they have a `__getattr__` but it's not
            # invoked at this point.
            try:
                metaclass = owner.metaclass()
            except astroid.MroError:
                return False
            if metaclass:
                # Renamed in Python 3.10 to `EnumType`
                return metaclass.qname() in {"enum.EnumMeta", "enum.EnumType"}
            return False
        if not has_known_bases(owner):
            return False

        # Exclude typed annotations, since these might actually exist
        # at some point during the runtime of the program.
        if utils.is_attribute_typed_annotation(owner, node.attrname):
            return False
    if isinstance(owner, astroid.objects.Super):
        # Verify if we are dealing with an invalid Super object.
        # If it is invalid, then there's no point in checking that
        # it has the required attribute. Also, don't fail if the
        # MRO is invalid.
        try:
            owner.super_mro()
        except (astroid.MroError, astroid.SuperError):
            return False
        if not all(has_known_bases(base) for base in owner.type.mro()):
            return False
    if isinstance(owner, nodes.Module):
        try:
            owner.getattr("__getattr__")
            return False
        except astroid.NotFoundError:
            pass
    if owner_name and node.attrname.startswith("_" + owner_name):
        # Test if an attribute has been mangled ('private' attribute)
        unmangled_name = node.attrname.split("_" + owner_name)[-1]
        try:
            if owner.getattr(unmangled_name, context=None) is not None:
                return False
        except astroid.NotFoundError:
            return True
    if (
        owner.parent
        and isinstance(owner.parent, nodes.ClassDef)
        and owner.parent.name == "EnumMeta"
        and owner_name == "__members__"
        and node.attrname in {"items", "values", "keys"}
    ):
        # Avoid false positive on Enum.__members__.{items(), values, keys}
        # See https://github.com/PyCQA/pylint/issues/4123
        return False
    # Don't emit no-member if guarded behind `IF` or `IFExp`
    #   * Walk up recursively until if statement is found.
    #   * Check if condition can be inferred as `Const`,
    #       would evaluate as `False`,
    #       and wheater the node is part of the `body`.
    #   * Continue checking until scope of node is reached.
    scope: nodes.NodeNG = node.scope()
    node_origin: nodes.NodeNG = node
    parent: nodes.NodeNG = node.parent
    while parent != scope:
        if isinstance(parent, (nodes.If, nodes.IfExp)):
            inferred = safe_infer(parent.test)
            if (  # pylint: disable=too-many-boolean-expressions
                isinstance(inferred, nodes.Const)
                and inferred.bool_value() is False
                and (
                    isinstance(parent, nodes.If)
                    and node_origin in parent.body
                    or isinstance(parent, nodes.IfExp)
                    and node_origin == parent.body
                )
            ):
                return False
        node_origin, parent = parent, parent.parent

    return True


def _determine_callable(callable_obj):
    # Ordering is important, since BoundMethod is a subclass of UnboundMethod,
    # and Function inherits Lambda.
    parameters = 0
    if hasattr(callable_obj, "implicit_parameters"):
        parameters = callable_obj.implicit_parameters()
    if isinstance(callable_obj, astroid.BoundMethod):
        # Bound methods have an extra implicit 'self' argument.
        return callable_obj, parameters, callable_obj.type
    if isinstance(callable_obj, astroid.UnboundMethod):
        return callable_obj, parameters, "unbound method"
    if isinstance(callable_obj, nodes.FunctionDef):
        return callable_obj, parameters, callable_obj.type
    if isinstance(callable_obj, nodes.Lambda):
        return callable_obj, parameters, "lambda"
    if isinstance(callable_obj, nodes.ClassDef):
        # Class instantiation, lookup __new__ instead.
        # If we only find object.__new__, we can safely check __init__
        # instead. If __new__ belongs to builtins, then we look
        # again for __init__ in the locals, since we won't have
        # argument information for the builtin __new__ function.
        try:
            # Use the last definition of __new__.
            new = callable_obj.local_attr("__new__")[-1]
        except astroid.NotFoundError:
            new = None

        from_object = new and new.parent.scope().name == "object"
        from_builtins = new and new.root().name in sys.builtin_module_names

        if not new or from_object or from_builtins:
            try:
                # Use the last definition of __init__.
                callable_obj = callable_obj.local_attr("__init__")[-1]
            except astroid.NotFoundError as e:
                # do nothing, covered by no-init.
                raise ValueError from e
        else:
            callable_obj = new

        if not isinstance(callable_obj, nodes.FunctionDef):
            raise ValueError
        # both have an extra implicit 'cls'/'self' argument.
        return callable_obj, parameters, "constructor"

    raise ValueError


def _has_parent_of_type(node, node_type, statement):
    """Check if the given node has a parent of the given type."""
    parent = node.parent
    while not isinstance(parent, node_type) and statement.parent_of(parent):
        parent = parent.parent
    return isinstance(parent, node_type)


def _no_context_variadic_keywords(node, scope):
    statement = node.statement()
    variadics = ()

    if isinstance(scope, nodes.Lambda) and not isinstance(scope, nodes.FunctionDef):
        variadics = list(node.keywords or []) + node.kwargs
    elif isinstance(statement, (nodes.Return, nodes.Expr, nodes.Assign)) and isinstance(
        statement.value, nodes.Call
    ):
        call = statement.value
        variadics = list(call.keywords or []) + call.kwargs

    return _no_context_variadic(node, scope.args.kwarg, nodes.Keyword, variadics)


def _no_context_variadic_positional(node, scope):
    variadics = ()
    if isinstance(scope, nodes.Lambda) and not isinstance(scope, nodes.FunctionDef):
        variadics = node.starargs + node.kwargs
    else:
        statement = node.statement()
        if isinstance(
            statement, (nodes.Expr, nodes.Return, nodes.Assign)
        ) and isinstance(statement.value, nodes.Call):
            call = statement.value
            variadics = call.starargs + call.kwargs

    return _no_context_variadic(node, scope.args.vararg, nodes.Starred, variadics)


def _no_context_variadic(node, variadic_name, variadic_type, variadics):
    """Verify if the given call node has variadic nodes without context

    This is a workaround for handling cases of nested call functions
    which don't have the specific call context at hand.
    Variadic arguments (variable positional arguments and variable
    keyword arguments) are inferred, inherently wrong, by astroid
    as a Tuple, respectively a Dict with empty elements.
    This can lead pylint to believe that a function call receives
    too few arguments.
    """
    scope = node.scope()
    is_in_lambda_scope = not isinstance(scope, nodes.FunctionDef) and isinstance(
        scope, nodes.Lambda
    )
    statement = node.statement()
    for name in statement.nodes_of_class(nodes.Name):
        if name.name != variadic_name:
            continue

        inferred = safe_infer(name)
        if isinstance(inferred, (nodes.List, nodes.Tuple)):
            length = len(inferred.elts)
        elif isinstance(inferred, nodes.Dict):
            length = len(inferred.items)
        else:
            continue

        if is_in_lambda_scope and isinstance(inferred.parent, nodes.Arguments):
            # The statement of the variadic will be the assignment itself,
            # so we need to go the lambda instead
            inferred_statement = inferred.parent.parent
        else:
            inferred_statement = inferred.statement()

        if not length and isinstance(inferred_statement, nodes.Lambda):
            is_in_starred_context = _has_parent_of_type(node, variadic_type, statement)
            used_as_starred_argument = any(
                variadic.value == name or variadic.value.parent_of(name)
                for variadic in variadics
            )
            if is_in_starred_context or used_as_starred_argument:
                return True
    return False


def _is_invalid_metaclass(metaclass):
    try:
        mro = metaclass.mro()
    except NotImplementedError:
        # Cannot have a metaclass which is not a newstyle class.
        return True
    else:
        if not any(is_builtin_object(cls) and cls.name == "type" for cls in mro):
            return True
    return False


def _infer_from_metaclass_constructor(cls, func: nodes.FunctionDef):
    """Try to infer what the given *func* constructor is building

    :param astroid.FunctionDef func:
        A metaclass constructor. Metaclass definitions can be
        functions, which should accept three arguments, the name of
        the class, the bases of the class and the attributes.
        The function could return anything, but usually it should
        be a proper metaclass.
    :param astroid.ClassDef cls:
        The class for which the *func* parameter should generate
        a metaclass.
    :returns:
        The class generated by the function or None,
        if we couldn't infer it.
    :rtype: astroid.ClassDef
    """
    context = astroid.context.InferenceContext()

    class_bases = nodes.List()
    class_bases.postinit(elts=cls.bases)

    attrs = nodes.Dict()
    local_names = [(name, values[-1]) for name, values in cls.locals.items()]
    attrs.postinit(local_names)

    builder_args = nodes.Tuple()
    builder_args.postinit([cls.name, class_bases, attrs])

    context.callcontext = astroid.context.CallContext(builder_args)
    try:
        inferred = next(func.infer_call_result(func, context), None)
    except astroid.InferenceError:
        return None
    return inferred or None


def _is_c_extension(module_node):
    return (
        not astroid.modutils.is_standard_module(module_node.name)
        and not module_node.fully_defined()
    )


def _is_invalid_isinstance_type(arg):
    # Return True if we are sure that arg is not a type
    inferred = utils.safe_infer(arg)
    if not inferred:
        # Cannot infer it so skip it.
        return False
    if isinstance(inferred, nodes.Tuple):
        return any(_is_invalid_isinstance_type(elt) for elt in inferred.elts)
    if isinstance(inferred, nodes.ClassDef):
        return False
    if isinstance(inferred, astroid.Instance) and inferred.qname() == BUILTIN_TUPLE:
        return False
    return True


class TypeChecker(BaseChecker):
    """try to find bugs in the code using type inference"""

    __implements__ = (IAstroidChecker,)

    # configuration section name
    name = "typecheck"
    # messages
    msgs = MSGS
    priority = -1
    # configuration options
    options = (
        (
            "ignore-on-opaque-inference",
            {
                "default": True,
                "type": "yn",
                "metavar": "<y or n>",
                "help": "This flag controls whether pylint should warn about "
                "no-member and similar checks whenever an opaque object "
                "is returned when inferring. The inference can return "
                "multiple potential results while evaluating a Python object, "
                "but some branches might not be evaluated, which results in "
                "partial inference. In that case, it might be useful to still emit "
                "no-member and other checks for the rest of the inferred objects.",
            },
        ),
        (
            "mixin-class-rgx",
            {
                "default": ".*[Mm]ixin",
                "type": "regexp",
                "metavar": "<regexp>",
                "help": "Regex pattern to define which classes are considered mixins "
                "ignore-mixin-members is set to 'yes'",
            },
        ),
        (
            "ignore-mixin-members",
            {
                "default": True,
                "type": "yn",
                "metavar": "<y or n>",
                "help": "Tells whether missing members accessed in mixin "
                "class should be ignored. A class is considered mixin if its name matches "
                "the mixin-class-rgx option.",
            },
        ),
        (
            "ignore-none",
            {
                "default": True,
                "type": "yn",
                "metavar": "<y or n>",
                "help": "Tells whether to warn about missing members when the owner "
                "of the attribute is inferred to be None.",
            },
        ),
        (
            "ignored-modules",
            {
                "default": (),
                "type": "csv",
                "metavar": "<module names>",
                "help": "List of module names for which member attributes "
                "should not be checked (useful for modules/projects "
                "where namespaces are manipulated during runtime and "
                "thus existing member attributes cannot be "
                "deduced by static analysis). It supports qualified "
                "module names, as well as Unix pattern matching.",
            },
        ),
        # the defaults here are *stdlib* names that (almost) always
        # lead to false positives, since their idiomatic use is
        # 'too dynamic' for pylint to grok.
        (
            "ignored-classes",
            {
                "default": ("optparse.Values", "thread._local", "_thread._local"),
                "type": "csv",
                "metavar": "<members names>",
                "help": "List of class names for which member attributes "
                "should not be checked (useful for classes with "
                "dynamically set attributes). This supports "
                "the use of qualified names.",
            },
        ),
        (
            "generated-members",
            {
                "default": (),
                "type": "string",
                "metavar": "<members names>",
                "help": "List of members which are set dynamically and \
missed by pylint inference system, and so shouldn't trigger E1101 when \
accessed. Python regular expressions are accepted.",
            },
        ),
        (
            "contextmanager-decorators",
            {
                "default": ["contextlib.contextmanager"],
                "type": "csv",
                "metavar": "<decorator names>",
                "help": "List of decorators that produce context managers, "
                "such as contextlib.contextmanager. Add to this list "
                "to register other decorators that produce valid "
                "context managers.",
            },
        ),
        (
            "missing-member-hint-distance",
            {
                "default": 1,
                "type": "int",
                "metavar": "<member hint edit distance>",
                "help": "The minimum edit distance a name should have in order "
                "to be considered a similar match for a missing member name.",
            },
        ),
        (
            "missing-member-max-choices",
            {
                "default": 1,
                "type": "int",
                "metavar": "<member hint max choices>",
                "help": "The total number of similar names that should be taken in "
                "consideration when showing a hint for a missing member.",
            },
        ),
        (
            "missing-member-hint",
            {
                "default": True,
                "type": "yn",
                "metavar": "<missing member hint>",
                "help": "Show a hint with possible names when a member name was not "
                "found. The aspect of finding the hint is based on edit distance.",
            },
        ),
        (
            "signature-mutators",
            {
                "default": [],
                "type": "csv",
                "metavar": "<decorator names>",
                "help": "List of decorators that change the signature of "
                "a decorated function.",
            },
        ),
    )

    def open(self) -> None:
        py_version = get_global_option(self, "py-version")
        self._py310_plus = py_version >= (3, 10)
        self._mixin_class_rgx = get_global_option(self, "mixin-class-rgx")

    @astroid.decorators.cachedproperty
    def _suggestion_mode(self):
        return get_global_option(self, "suggestion-mode", default=True)

    @astroid.decorators.cachedproperty
    def _compiled_generated_members(self) -> Tuple[Pattern, ...]:
        # do this lazily since config not fully initialized in __init__
        # generated_members may contain regular expressions
        # (surrounded by quote `"` and followed by a comma `,`)
        # REQUEST,aq_parent,"[a-zA-Z]+_set{1,2}"' =>
        # ('REQUEST', 'aq_parent', '[a-zA-Z]+_set{1,2}')
        generated_members = self.config.generated_members
        if isinstance(generated_members, str):
            gen = shlex.shlex(generated_members)
            gen.whitespace += ","
            gen.wordchars += r"[]-+\.*?()|"
            generated_members = tuple(tok.strip('"') for tok in gen)
        return tuple(re.compile(exp) for exp in generated_members)

    @check_messages("keyword-arg-before-vararg")
    def visit_functiondef(self, node: nodes.FunctionDef) -> None:
        # check for keyword arg before varargs
        if node.args.vararg and node.args.defaults:
            self.add_message("keyword-arg-before-vararg", node=node, args=(node.name))

    visit_asyncfunctiondef = visit_functiondef

    @check_messages("invalid-metaclass")
    def visit_classdef(self, node: nodes.ClassDef) -> None:
        def _metaclass_name(metaclass):
            # pylint: disable=unidiomatic-typecheck
            if isinstance(metaclass, (nodes.ClassDef, nodes.FunctionDef)):
                return metaclass.name
            if type(metaclass) is bases.Instance:
                # Really do mean type, not isinstance, since subclasses of bases.Instance
                # like Const or Dict should use metaclass.as_string below.
                return str(metaclass)
            return metaclass.as_string()

        metaclass = node.declared_metaclass()
        if not metaclass:
            return

        if isinstance(metaclass, nodes.FunctionDef):
            # Try to infer the result.
            metaclass = _infer_from_metaclass_constructor(node, metaclass)
            if not metaclass:
                # Don't do anything if we cannot infer the result.
                return

        if isinstance(metaclass, nodes.ClassDef):
            if _is_invalid_metaclass(metaclass):
                self.add_message(
                    "invalid-metaclass", node=node, args=(_metaclass_name(metaclass),)
                )
        else:
            self.add_message(
                "invalid-metaclass", node=node, args=(_metaclass_name(metaclass),)
            )

    def visit_assignattr(self, node: nodes.AssignAttr) -> None:
        if isinstance(node.assign_type(), nodes.AugAssign):
            self.visit_attribute(node)

    def visit_delattr(self, node: nodes.DelAttr) -> None:
        self.visit_attribute(node)

    @check_messages("no-member", "c-extension-no-member")
    def visit_attribute(self, node: nodes.Attribute) -> None:
        """check that the accessed attribute exists

        to avoid too much false positives for now, we'll consider the code as
        correct if a single of the inferred nodes has the accessed attribute.

        function/method, super call and metaclasses are ignored
        """
        if any(
            pattern.match(name)
            for name in (node.attrname, node.as_string())
            for pattern in self._compiled_generated_members
        ):
            return

        try:
            inferred = list(node.expr.infer())
        except astroid.InferenceError:
            return

        # list of (node, nodename) which are missing the attribute
        missingattr = set()

        non_opaque_inference_results = [
            owner
            for owner in inferred
            if owner is not astroid.Uninferable and not isinstance(owner, nodes.Unknown)
        ]
        if (
            len(non_opaque_inference_results) != len(inferred)
            and self.config.ignore_on_opaque_inference
        ):
            # There is an ambiguity in the inference. Since we can't
            # make sure that we won't emit a false positive, we just stop
            # whenever the inference returns an opaque inference object.
            return
        for owner in non_opaque_inference_results:
            name = getattr(owner, "name", None)
            if _is_owner_ignored(
                owner, name, self.config.ignored_classes, self.config.ignored_modules
            ):
                continue

            qualname = f"{owner.pytype()}.{node.attrname}"
            if any(
                pattern.match(qualname) for pattern in self._compiled_generated_members
            ):
                return

            try:
                if not [
                    n
                    for n in owner.getattr(node.attrname)
                    if not isinstance(n.statement(), nodes.AugAssign)
                ]:
                    missingattr.add((owner, name))
                    continue
            except AttributeError:
                continue
            except astroid.DuplicateBasesError:
                continue
            except astroid.NotFoundError:
                # This can't be moved before the actual .getattr call,
                # because there can be more values inferred and we are
                # stopping after the first one which has the attribute in question.
                # The problem is that if the first one has the attribute,
                # but we continue to the next values which doesn't have the
                # attribute, then we'll have a false positive.
                # So call this only after the call has been made.
                if not _emit_no_member(
                    node,
                    owner,
                    name,
                    self._mixin_class_rgx,
                    ignored_mixins=self.config.ignore_mixin_members,
                    ignored_none=self.config.ignore_none,
                ):
                    continue
                missingattr.add((owner, name))
                continue
            # stop on the first found
            break
        else:
            # we have not found any node with the attributes, display the
            # message for inferred nodes
            done = set()
            for owner, name in missingattr:
                if isinstance(owner, astroid.Instance):
                    actual = owner._proxied
                else:
                    actual = owner
                if actual in done:
                    continue
                done.add(actual)

                msg, hint = self._get_nomember_msgid_hint(node, owner)
                self.add_message(
                    msg,
                    node=node,
                    args=(owner.display_type(), name, node.attrname, hint),
                    confidence=INFERENCE,
                )

    def _get_nomember_msgid_hint(self, node, owner):
        suggestions_are_possible = self._suggestion_mode and isinstance(
            owner, nodes.Module
        )
        if suggestions_are_possible and _is_c_extension(owner):
            msg = "c-extension-no-member"
            hint = ""
        else:
            msg = "no-member"
            if self.config.missing_member_hint:
                hint = _missing_member_hint(
                    owner,
                    node.attrname,
                    self.config.missing_member_hint_distance,
                    self.config.missing_member_max_choices,
                )
            else:
                hint = ""
        return msg, hint

    @check_messages(
        "assignment-from-no-return",
        "assignment-from-none",
        "non-str-assignment-to-dunder-name",
    )
    def visit_assign(self, node: nodes.Assign) -> None:
        """
        Process assignments in the AST.
        """

        self._check_assignment_from_function_call(node)
        self._check_dundername_is_string(node)

    def _check_assignment_from_function_call(self, node):
        """check that if assigning to a function call, the function is
        possibly returning something valuable
        """
        if not isinstance(node.value, nodes.Call):
            return

        function_node = safe_infer(node.value.func)
        funcs = (nodes.FunctionDef, astroid.UnboundMethod, astroid.BoundMethod)
        if not isinstance(function_node, funcs):
            return

        # Unwrap to get the actual function object
        if isinstance(function_node, astroid.BoundMethod) and isinstance(
            function_node._proxied, astroid.UnboundMethod
        ):
            function_node = function_node._proxied._proxied

        # Make sure that it's a valid function that we can analyze.
        # Ordered from less expensive to more expensive checks.
        # pylint: disable=too-many-boolean-expressions
        if (
            not function_node.is_function
            or isinstance(function_node, nodes.AsyncFunctionDef)
            or function_node.decorators
            or function_node.is_generator()
            or function_node.is_abstract(pass_is_abstract=False)
            or utils.is_error(function_node)
            or not function_node.root().fully_defined()
        ):
            return

        returns = list(
            function_node.nodes_of_class(nodes.Return, skip_klass=nodes.FunctionDef)
        )
        if not returns:
            self.add_message("assignment-from-no-return", node=node)
        else:
            for rnode in returns:
                if not (
                    isinstance(rnode.value, nodes.Const)
                    and rnode.value.value is None
                    or rnode.value is None
                ):
                    break
            else:
                self.add_message("assignment-from-none", node=node)

    def _check_dundername_is_string(self, node):
        """
        Check a string is assigned to self.__name__
        """

        # Check the left hand side of the assignment is <something>.__name__
        lhs = node.targets[0]
        if not isinstance(lhs, nodes.AssignAttr):
            return
        if not lhs.attrname == "__name__":
            return

        # If the right hand side is not a string
        rhs = node.value
        if isinstance(rhs, nodes.Const) and isinstance(rhs.value, str):
            return
        inferred = utils.safe_infer(rhs)
        if not inferred:
            return
        if not (isinstance(inferred, nodes.Const) and isinstance(inferred.value, str)):
            # Add the message
            self.add_message("non-str-assignment-to-dunder-name", node=node)

    def _check_uninferable_call(self, node):
        """
        Check that the given uninferable Call node does not
        call an actual function.
        """
        if not isinstance(node.func, nodes.Attribute):
            return

        # Look for properties. First, obtain
        # the lhs of the Attribute node and search the attribute
        # there. If that attribute is a property or a subclass of properties,
        # then most likely it's not callable.

        expr = node.func.expr
        klass = safe_infer(expr)
        if (
            klass is None
            or klass is astroid.Uninferable
            or not isinstance(klass, astroid.Instance)
        ):
            return

        try:
            attrs = klass._proxied.getattr(node.func.attrname)
        except astroid.NotFoundError:
            return

        for attr in attrs:
            if attr is astroid.Uninferable:
                continue
            if not isinstance(attr, nodes.FunctionDef):
                continue

            # Decorated, see if it is decorated with a property.
            # Also, check the returns and see if they are callable.
            if decorated_with_property(attr):

                try:
                    all_returns_are_callable = all(
                        return_node.callable() or return_node is astroid.Uninferable
                        for return_node in attr.infer_call_result(node)
                    )
                except astroid.InferenceError:
                    continue

                if not all_returns_are_callable:
                    self.add_message(
                        "not-callable", node=node, args=node.func.as_string()
                    )
                    break

    def _check_argument_order(self, node, call_site, called, called_param_names):
        """Match the supplied argument names against the function parameters.
        Warn if some argument names are not in the same order as they are in
        the function signature.
        """
        # Check for called function being an object instance function
        # If so, ignore the initial 'self' argument in the signature
        try:
            is_classdef = isinstance(called.parent, nodes.ClassDef)
            if is_classdef and called_param_names[0] == "self":
                called_param_names = called_param_names[1:]
        except IndexError:
            return

        try:
            # extract argument names, if they have names
            calling_parg_names = [p.name for p in call_site.positional_arguments]

            # Additionally get names of keyword arguments to use in a full match
            # against parameters
            calling_kwarg_names = [
                arg.name for arg in call_site.keyword_arguments.values()
            ]
        except AttributeError:
            # the type of arg does not provide a `.name`. In this case we
            # stop checking for out-of-order arguments because it is only relevant
            # for named variables.
            return

        # Don't check for ordering if there is an unmatched arg or param
        arg_set = set(calling_parg_names) | set(calling_kwarg_names)
        param_set = set(called_param_names)
        if arg_set != param_set:
            return

        # Warn based on the equality of argument ordering
        if calling_parg_names != called_param_names[: len(calling_parg_names)]:
            self.add_message("arguments-out-of-order", node=node, args=())

    def _check_isinstance_args(self, node):
        if len(node.args) != 2:
            # isinstance called with wrong number of args
            return

        second_arg = node.args[1]
        if _is_invalid_isinstance_type(second_arg):
            self.add_message("isinstance-second-argument-not-valid-type", node=node)

    # pylint: disable=too-many-branches,too-many-locals
    @check_messages(*(list(MSGS.keys())))
    def visit_call(self, node: nodes.Call) -> None:
        """check that called functions/methods are inferred to callable objects,
        and that the arguments passed to the function match the parameters in
        the inferred function's definition
        """
        called = safe_infer(node.func)
        # only function, generator and object defining __call__ are allowed
        # Ignore instances of descriptors since astroid cannot properly handle them
        # yet
        if called and not called.callable():
            if isinstance(called, astroid.Instance) and (
                not has_known_bases(called)
                or (
                    called.parent is not None
                    and isinstance(called.scope(), nodes.ClassDef)
                    and "__get__" in called.locals
                )
            ):
                # Don't emit if we can't make sure this object is callable.
                pass
            else:
                self.add_message("not-callable", node=node, args=node.func.as_string())
        else:
            self._check_uninferable_call(node)

        try:
            called, implicit_args, callable_name = _determine_callable(called)
        except ValueError:
            # Any error occurred during determining the function type, most of
            # those errors are handled by different warnings.
            return

        if called.args.args is None:
            if called.name == "isinstance":
                # Verify whether second argument of isinstance is a valid type
                self._check_isinstance_args(node)
            # Built-in functions have no argument information.
            return

        if len(called.argnames()) != len(set(called.argnames())):
            # Duplicate parameter name (see duplicate-argument).  We can't really
            # make sense of the function call in this case, so just return.
            return

        # Build the set of keyword arguments, checking for duplicate keywords,
        # and count the positional arguments.
        call_site = astroid.arguments.CallSite.from_call(node)

        # Warn about duplicated keyword arguments, such as `f=24, **{'f': 24}`
        for keyword in call_site.duplicated_keywords:
            self.add_message("repeated-keyword", node=node, args=(keyword,))

        if call_site.has_invalid_arguments() or call_site.has_invalid_keywords():
            # Can't make sense of this.
            return

        # Has the function signature changed in ways we cannot reliably detect?
        if hasattr(called, "decorators") and decorated_with(
            called, self.config.signature_mutators
        ):
            return

        num_positional_args = len(call_site.positional_arguments)
        keyword_args = list(call_site.keyword_arguments.keys())
        overload_function = is_overload_stub(called)

        # Determine if we don't have a context for our call and we use variadics.
        node_scope = node.scope()
        if isinstance(node_scope, (nodes.Lambda, nodes.FunctionDef)):
            has_no_context_positional_variadic = _no_context_variadic_positional(
                node, node_scope
            )
            has_no_context_keywords_variadic = _no_context_variadic_keywords(
                node, node_scope
            )
        else:
            has_no_context_positional_variadic = (
                has_no_context_keywords_variadic
            ) = False

        # These are coming from the functools.partial implementation in astroid
        already_filled_positionals = getattr(called, "filled_positionals", 0)
        already_filled_keywords = getattr(called, "filled_keywords", {})

        keyword_args += list(already_filled_keywords)
        num_positional_args += implicit_args + already_filled_positionals

        # Analyze the list of formal parameters.
        args = list(itertools.chain(called.args.posonlyargs or (), called.args.args))
        num_mandatory_parameters = len(args) - len(called.args.defaults)
        parameters: List[List[Any]] = []
        parameter_name_to_index = {}
        for i, arg in enumerate(args):
            if isinstance(arg, nodes.Tuple):
                name = None
                # Don't store any parameter names within the tuple, since those
                # are not assignable from keyword arguments.
            else:
                assert isinstance(arg, nodes.AssignName)
                # This occurs with:
                #    def f( (a), (b) ): pass
                name = arg.name
                parameter_name_to_index[name] = i
            if i >= num_mandatory_parameters:
                defval = called.args.defaults[i - num_mandatory_parameters]
            else:
                defval = None
            parameters.append([(name, defval), False])

        kwparams = {}
        for i, arg in enumerate(called.args.kwonlyargs):
            if isinstance(arg, nodes.Keyword):
                name = arg.arg
            else:
                assert isinstance(arg, nodes.AssignName)
                name = arg.name
            kwparams[name] = [called.args.kw_defaults[i], False]

        self._check_argument_order(
            node, call_site, called, [p[0][0] for p in parameters]
        )

        # 1. Match the positional arguments.
        for i in range(num_positional_args):
            if i < len(parameters):
                parameters[i][1] = True
            elif called.args.vararg is not None:
                # The remaining positional arguments get assigned to the *args
                # parameter.
                break
            elif not overload_function:
                # Too many positional arguments.
                self.add_message(
                    "too-many-function-args", node=node, args=(callable_name,)
                )
                break

        # 2. Match the keyword arguments.
        for keyword in keyword_args:
            if keyword in parameter_name_to_index:
                i = parameter_name_to_index[keyword]
                if parameters[i][1]:
                    # Duplicate definition of function parameter.

                    # Might be too hardcoded, but this can actually
                    # happen when using str.format and `self` is passed
                    # by keyword argument, as in `.format(self=self)`.
                    # It's perfectly valid to so, so we're just skipping
                    # it if that's the case.
                    if not (keyword == "self" and called.qname() in STR_FORMAT):
                        self.add_message(
                            "redundant-keyword-arg",
                            node=node,
                            args=(keyword, callable_name),
                        )
                else:
                    parameters[i][1] = True
            elif keyword in kwparams:
                if kwparams[keyword][1]:
                    # Duplicate definition of function parameter.
                    self.add_message(
                        "redundant-keyword-arg",
                        node=node,
                        args=(keyword, callable_name),
                    )
                else:
                    kwparams[keyword][1] = True
            elif called.args.kwarg is not None:
                # The keyword argument gets assigned to the **kwargs parameter.
                pass
            elif not overload_function:
                # Unexpected keyword argument.
                self.add_message(
                    "unexpected-keyword-arg", node=node, args=(keyword, callable_name)
                )

        # 3. Match the **kwargs, if any.
        if node.kwargs:
            for i, [(name, defval), assigned] in enumerate(parameters):
                # Assume that *kwargs provides values for all remaining
                # unassigned named parameters.
                if name is not None:
                    parameters[i][1] = True
                else:
                    # **kwargs can't assign to tuples.
                    pass

        # Check that any parameters without a default have been assigned
        # values.
        for [(name, defval), assigned] in parameters:
            if (defval is None) and not assigned:
                display_name = "<tuple>" if name is None else repr(name)
                if not has_no_context_positional_variadic and not overload_function:
                    self.add_message(
                        "no-value-for-parameter",
                        node=node,
                        args=(display_name, callable_name),
                    )

        for name, val in kwparams.items():
            defval, assigned = val
            if (
                defval is None
                and not assigned
                and not has_no_context_keywords_variadic
                and not overload_function
            ):
                self.add_message("missing-kwoa", node=node, args=(name, callable_name))

    def _check_invalid_sequence_index(self, subscript: nodes.Subscript):
        # Look for index operations where the parent is a sequence type.
        # If the types can be determined, only allow indices to be int,
        # slice or instances with __index__.
        parent_type = safe_infer(subscript.value)
        if not isinstance(
            parent_type, (nodes.ClassDef, astroid.Instance)
        ) or not has_known_bases(parent_type):
            return None

        # Determine what method on the parent this index will use
        # The parent of this node will be a Subscript, and the parent of that
        # node determines if the Subscript is a get, set, or delete operation.
        if subscript.ctx is astroid.Store:
            methodname = "__setitem__"
        elif subscript.ctx is astroid.Del:
            methodname = "__delitem__"
        else:
            methodname = "__getitem__"

        # Check if this instance's __getitem__, __setitem__, or __delitem__, as
        # appropriate to the statement, is implemented in a builtin sequence
        # type. This way we catch subclasses of sequence types but skip classes
        # that override __getitem__ and which may allow non-integer indices.
        try:
            methods = astroid.interpreter.dunder_lookup.lookup(parent_type, methodname)
            if methods is astroid.Uninferable:
                return None
            itemmethod = methods[0]
        except (
            astroid.AttributeInferenceError,
            IndexError,
        ):
            return None
        if (
            not isinstance(itemmethod, nodes.FunctionDef)
            or itemmethod.root().name != "builtins"
            or not itemmethod.parent
            or itemmethod.parent.name not in SEQUENCE_TYPES
        ):
            return None

        # For ExtSlice objects coming from visit_extslice, no further
        # inference is necessary, since if we got this far the ExtSlice
        # is an error.
        if isinstance(subscript.value, nodes.ExtSlice):
            index_type = subscript.value
        else:
            index_type = safe_infer(subscript.slice)
        if index_type is None or index_type is astroid.Uninferable:
            return None
        # Constants must be of type int
        if isinstance(index_type, nodes.Const):
            if isinstance(index_type.value, int):
                return None
        # Instance values must be int, slice, or have an __index__ method
        elif isinstance(index_type, astroid.Instance):
            if index_type.pytype() in {"builtins.int", "builtins.slice"}:
                return None
            try:
                index_type.getattr("__index__")
                return None
            except astroid.NotFoundError:
                pass
        elif isinstance(index_type, nodes.Slice):
            # A slice can be present
            # here after inferring the index node, which could
            # be a `slice(...)` call for instance.
            return self._check_invalid_slice_index(index_type)

        # Anything else is an error
        self.add_message("invalid-sequence-index", node=subscript)
        return None

    @check_messages("invalid-sequence-index")
    def visit_extslice(self, node: nodes.ExtSlice) -> None:
        if not node.parent or not hasattr(node.parent, "value"):
            return None
        # Check extended slice objects as if they were used as a sequence
        # index to check if the object being sliced can support them
        return self._check_invalid_sequence_index(node.parent)

    def _check_invalid_slice_index(self, node: nodes.Slice) -> None:
        # Check the type of each part of the slice
        invalid_slices_nodes: List[nodes.NodeNG] = []
        for index in (node.lower, node.upper, node.step):
            if index is None:
                continue

            index_type = safe_infer(index)
            if index_type is None or index_type is astroid.Uninferable:
                continue

            # Constants must of type int or None
            if isinstance(index_type, nodes.Const):
                if isinstance(index_type.value, (int, type(None))):
                    continue
            # Instance values must be of type int, None or an object
            # with __index__
            elif isinstance(index_type, astroid.Instance):
                if index_type.pytype() in {"builtins.int", "builtins.NoneType"}:
                    continue

                try:
                    index_type.getattr("__index__")
                    return
                except astroid.NotFoundError:
                    pass
            invalid_slices_nodes.append(index)

        if not invalid_slices_nodes:
            return

        # Anything else is an error, unless the object that is indexed
        # is a custom object, which knows how to handle this kind of slices
        parent = node.parent
        if isinstance(parent, nodes.ExtSlice):
            parent = parent.parent
        if isinstance(parent, nodes.Subscript):
            inferred = safe_infer(parent.value)
            if inferred is None or inferred is astroid.Uninferable:
                # Don't know what this is
                return
            known_objects = (
                nodes.List,
                nodes.Dict,
                nodes.Tuple,
                astroid.objects.FrozenSet,
                nodes.Set,
            )
            if not isinstance(inferred, known_objects):
                # Might be an instance that knows how to handle this slice object
                return
        for snode in invalid_slices_nodes:
            self.add_message("invalid-slice-index", node=snode)

    @check_messages("not-context-manager")
    def visit_with(self, node: nodes.With) -> None:
        for ctx_mgr, _ in node.items:
            context = astroid.context.InferenceContext()
            inferred = safe_infer(ctx_mgr, context=context)
            if inferred is None or inferred is astroid.Uninferable:
                continue

            if isinstance(inferred, astroid.bases.Generator):
                # Check if we are dealing with a function decorated
                # with contextlib.contextmanager.
                if decorated_with(
                    inferred.parent, self.config.contextmanager_decorators
                ):
                    continue
                # If the parent of the generator is not the context manager itself,
                # that means that it could have been returned from another
                # function which was the real context manager.
                # The following approach is more of a hack rather than a real
                # solution: walk all the inferred statements for the
                # given *ctx_mgr* and if you find one function scope
                # which is decorated, consider it to be the real
                # manager and give up, otherwise emit not-context-manager.
                # See the test file for not_context_manager for a couple
                # of self explaining tests.

                # Retrieve node from all previusly visited nodes in the the inference history
                context_path_names: Iterator[Any] = filter(
                    None, _unflatten(context.path)
                )
                inferred_paths = _flatten_container(
                    safe_infer(path) for path in context_path_names
                )
                for inferred_path in inferred_paths:
                    if not inferred_path:
                        continue
                    scope = inferred_path.scope()
                    if not isinstance(scope, nodes.FunctionDef):
                        continue
                    if decorated_with(scope, self.config.contextmanager_decorators):
                        break
                else:
                    self.add_message(
                        "not-context-manager", node=node, args=(inferred.name,)
                    )
            else:
                try:
                    inferred.getattr("__enter__")
                    inferred.getattr("__exit__")
                except astroid.NotFoundError:
                    if isinstance(inferred, astroid.Instance):
                        # If we do not know the bases of this class,
                        # just skip it.
                        if not has_known_bases(inferred):
                            continue
                        # Just ignore mixin classes.
                        if self.config.ignore_mixin_members:
                            if inferred.name[-5:].lower() == "mixin":
                                continue

                    self.add_message(
                        "not-context-manager", node=node, args=(inferred.name,)
                    )

    @check_messages("invalid-unary-operand-type")
    def visit_unaryop(self, node: nodes.UnaryOp) -> None:
        """Detect TypeErrors for unary operands."""

        for error in node.type_errors():
            # Let the error customize its output.
            self.add_message("invalid-unary-operand-type", args=str(error), node=node)

    @check_messages("unsupported-binary-operation")
    def visit_binop(self, node: nodes.BinOp) -> None:
        if node.op == "|":
            self._detect_unsupported_alternative_union_syntax(node)

    def _detect_unsupported_alternative_union_syntax(self, node: nodes.BinOp) -> None:
        """Detect if unsupported alternative Union syntax (PEP 604) was used."""
        if self._py310_plus:  # 310+ supports the new syntax
            return

        if isinstance(
            node.parent, TYPE_ANNOTATION_NODES_TYPES
        ) and not is_postponed_evaluation_enabled(node):
            # Use in type annotations only allowed if
            # postponed evaluation is enabled.
            self._check_unsupported_alternative_union_syntax(node)

        if isinstance(
            node.parent,
            (
                nodes.Assign,
                nodes.Call,
                nodes.Keyword,
                nodes.Dict,
                nodes.Tuple,
                nodes.Set,
                nodes.List,
                nodes.BinOp,
            ),
        ):
            # Check other contexts the syntax might appear, but are invalid.
            # Make sure to filter context if postponed evaluation is enabled
            # and parent is allowed node type.
            allowed_nested_syntax = False
            if is_postponed_evaluation_enabled(node):
                parent_node = node.parent
                while True:
                    if isinstance(parent_node, TYPE_ANNOTATION_NODES_TYPES):
                        allowed_nested_syntax = True
                        break
                    parent_node = parent_node.parent
                    if isinstance(parent_node, nodes.Module):
                        break
            if not allowed_nested_syntax:
                self._check_unsupported_alternative_union_syntax(node)

    def _check_unsupported_alternative_union_syntax(self, node: nodes.BinOp) -> None:
        """Check if left or right node is of type `type`."""
        msg = "unsupported operand type(s) for |"
        for n in (node.left, node.right):
            n = astroid.helpers.object_type(n)
            if isinstance(n, nodes.ClassDef) and is_classdef_type(n):
                self.add_message("unsupported-binary-operation", args=msg, node=node)
                break

    @check_messages("unsupported-binary-operation")
    def _visit_binop(self, node: nodes.BinOp) -> None:
        """Detect TypeErrors for binary arithmetic operands."""
        self._check_binop_errors(node)

    @check_messages("unsupported-binary-operation")
    def _visit_augassign(self, node: nodes.AugAssign) -> None:
        """Detect TypeErrors for augmented binary arithmetic operands."""
        self._check_binop_errors(node)

    def _check_binop_errors(self, node):
        for error in node.type_errors():
            # Let the error customize its output.
            if any(
                isinstance(obj, nodes.ClassDef) and not has_known_bases(obj)
                for obj in (error.left_type, error.right_type)
            ):
                continue
            self.add_message("unsupported-binary-operation", args=str(error), node=node)

    def _check_membership_test(self, node):
        if is_inside_abstract_class(node):
            return
        if is_comprehension(node):
            return
        inferred = safe_infer(node)
        if inferred is None or inferred is astroid.Uninferable:
            return
        if not supports_membership_test(inferred):
            self.add_message(
                "unsupported-membership-test", args=node.as_string(), node=node
            )

    @check_messages("unsupported-membership-test")
    def visit_compare(self, node: nodes.Compare) -> None:
        if len(node.ops) != 1:
            return

        op, right = node.ops[0]
        if op in {"in", "not in"}:
            self._check_membership_test(right)

    @check_messages(
        "unsubscriptable-object",
        "unsupported-assignment-operation",
        "unsupported-delete-operation",
        "unhashable-dict-key",
        "invalid-sequence-index",
        "invalid-slice-index",
    )
    def visit_subscript(self, node: nodes.Subscript) -> None:
        self._check_invalid_sequence_index(node)

        supported_protocol: Optional[Callable[[Any, Any], bool]] = None
        if isinstance(node.value, (nodes.ListComp, nodes.DictComp)):
            return

        if isinstance(node.value, nodes.Dict):
            # Assert dict key is hashable
            inferred = safe_infer(node.slice)
            if inferred and inferred != astroid.Uninferable:
                try:
                    hash_fn = next(inferred.igetattr("__hash__"))
                except astroid.InferenceError:
                    pass
                else:
                    if getattr(hash_fn, "value", True) is None:
                        self.add_message("unhashable-dict-key", node=node.value)

        if node.ctx == astroid.Load:
            supported_protocol = supports_getitem
            msg = "unsubscriptable-object"
        elif node.ctx == astroid.Store:
            supported_protocol = supports_setitem
            msg = "unsupported-assignment-operation"
        elif node.ctx == astroid.Del:
            supported_protocol = supports_delitem
            msg = "unsupported-delete-operation"

        if isinstance(node.value, nodes.SetComp):
            self.add_message(msg, args=node.value.as_string(), node=node.value)
            return

        if is_inside_abstract_class(node):
            return

        inferred = safe_infer(node.value)

        if inferred is None or inferred is astroid.Uninferable:
            return

        if getattr(inferred, "decorators", None):
            first_decorator = astroid.helpers.safe_infer(inferred.decorators.nodes[0])
            if isinstance(first_decorator, nodes.ClassDef):
                inferred = first_decorator.instantiate_class()
            else:
                return  # It would be better to handle function
                # decorators, but let's start slow.

        if supported_protocol and not supported_protocol(inferred, node):
            self.add_message(msg, args=node.value.as_string(), node=node.value)

    @check_messages("dict-items-missing-iter")
    def visit_for(self, node: nodes.For) -> None:
        if not isinstance(node.target, nodes.Tuple):
            # target is not a tuple
            return
        if not len(node.target.elts) == 2:
            # target is not a tuple of two elements
            return

        iterable = node.iter
        if not isinstance(iterable, nodes.Name):
            # it's not a bare variable
            return

        inferred = safe_infer(iterable)
        if not inferred:
            return
        if not isinstance(inferred, nodes.Dict):
            # the iterable is not a dict
            return

        if all(isinstance(i[0], nodes.Tuple) for i in inferred.items):
            # if all keys are tuples
            return

        self.add_message("dict-iter-missing-items", node=node)


class IterableChecker(BaseChecker):
    """
    Checks for non-iterables used in an iterable context.
    Contexts include:
    - for-statement
    - starargs in function call
    - `yield from`-statement
    - list, dict and set comprehensions
    - generator expressions
    Also checks for non-mappings in function call kwargs.
    """

    __implements__ = (IAstroidChecker,)
    name = "typecheck"

    msgs = {
        "E1133": (
            "Non-iterable value %s is used in an iterating context",
            "not-an-iterable",
            "Used when a non-iterable value is used in place where "
            "iterable is expected",
        ),
        "E1134": (
            "Non-mapping value %s is used in a mapping context",
            "not-a-mapping",
            "Used when a non-mapping value is used in place where "
            "mapping is expected",
        ),
    }

    @staticmethod
    def _is_asyncio_coroutine(node):
        if not isinstance(node, nodes.Call):
            return False

        inferred_func = safe_infer(node.func)
        if not isinstance(inferred_func, nodes.FunctionDef):
            return False
        if not inferred_func.decorators:
            return False
        for decorator in inferred_func.decorators.nodes:
            inferred_decorator = safe_infer(decorator)
            if not isinstance(inferred_decorator, nodes.FunctionDef):
                continue
            if inferred_decorator.qname() != ASYNCIO_COROUTINE:
                continue
            return True
        return False

    def _check_iterable(self, node, check_async=False):
        if is_inside_abstract_class(node) or is_comprehension(node):
            return
        inferred = safe_infer(node)
        if not inferred:
            return
        if not is_iterable(inferred, check_async=check_async):
            self.add_message("not-an-iterable", args=node.as_string(), node=node)

    def _check_mapping(self, node):
        if is_inside_abstract_class(node):
            return
        if isinstance(node, nodes.DictComp):
            return
        inferred = safe_infer(node)
        if inferred is None or inferred is astroid.Uninferable:
            return
        if not is_mapping(inferred):
            self.add_message("not-a-mapping", args=node.as_string(), node=node)

    @check_messages("not-an-iterable")
    def visit_for(self, node: nodes.For) -> None:
        self._check_iterable(node.iter)

    @check_messages("not-an-iterable")
    def visit_asyncfor(self, node: nodes.AsyncFor) -> None:
        self._check_iterable(node.iter, check_async=True)

    @check_messages("not-an-iterable")
    def visit_yieldfrom(self, node: nodes.YieldFrom) -> None:
        if self._is_asyncio_coroutine(node.value):
            return
        self._check_iterable(node.value)

    @check_messages("not-an-iterable", "not-a-mapping")
    def visit_call(self, node: nodes.Call) -> None:
        for stararg in node.starargs:
            self._check_iterable(stararg.value)
        for kwarg in node.kwargs:
            self._check_mapping(kwarg.value)

    @check_messages("not-an-iterable")
    def visit_listcomp(self, node: nodes.ListComp) -> None:
        for gen in node.generators:
            self._check_iterable(gen.iter, check_async=gen.is_async)

    @check_messages("not-an-iterable")
    def visit_dictcomp(self, node: nodes.DictComp) -> None:
        for gen in node.generators:
            self._check_iterable(gen.iter, check_async=gen.is_async)

    @check_messages("not-an-iterable")
    def visit_setcomp(self, node: nodes.SetComp) -> None:
        for gen in node.generators:
            self._check_iterable(gen.iter, check_async=gen.is_async)

    @check_messages("not-an-iterable")
    def visit_generatorexp(self, node: nodes.GeneratorExp) -> None:
        for gen in node.generators:
            self._check_iterable(gen.iter, check_async=gen.is_async)

    @check_messages("await-outside-async")
    def visit_await(self, node: nodes.Await) -> None:
        self._check_await_outside_coroutine(node)

    def _check_await_outside_coroutine(self, node: nodes.Await) -> None:
        node_scope = node.scope()
        while not isinstance(node_scope, nodes.Module):
            if isinstance(node_scope, nodes.AsyncFunctionDef):
                return
            if isinstance(node_scope, nodes.FunctionDef):
                break
            node_scope = node_scope.parent.scope()
        self.add_message("await-outside-async", node=node)


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(TypeChecker(linter))
    linter.register_checker(IterableChecker(linter))
