# Copyright (c) 2009-2011, 2013-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2012 FELD Boris <lothiraldan@gmail.com>
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Google, Inc.
# Copyright (c) 2014 Eevee (Alex Munroe) <amunroe@yelp.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2016-2017 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2017 Calen Pennington <calen.pennington@gmail.com>
# Copyright (c) 2018-2019 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018-2019 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2018 Daniel Colascione <dancol@dancol.org>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Tushar Sadhwani <86737547+tushar-deepsource@users.noreply.github.com>
# Copyright (c) 2021 pre-commit-ci[bot] <bot@noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 David Liu <david@cs.toronto.edu>
# Copyright (c) 2021 doranid <ddandd@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Andrew Haigh <hello@nelf.in>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""This module contains base classes and functions for the nodes and some
inference utils.
"""

import collections

from metaflow._vendor.astroid import decorators
from metaflow._vendor.astroid.const import PY310_PLUS
from metaflow._vendor.astroid.context import (
    CallContext,
    InferenceContext,
    bind_context_to_node,
    copy_context,
)
from metaflow._vendor.astroid.exceptions import (
    AstroidTypeError,
    AttributeInferenceError,
    InferenceError,
    NameInferenceError,
)
from metaflow._vendor.astroid.util import Uninferable, lazy_descriptor, lazy_import

objectmodel = lazy_import("interpreter.objectmodel")
helpers = lazy_import("helpers")
manager = lazy_import("manager")


# TODO: check if needs special treatment
BOOL_SPECIAL_METHOD = "__bool__"
BUILTINS = "builtins"  # TODO Remove in 2.8

PROPERTIES = {"builtins.property", "abc.abstractproperty"}
if PY310_PLUS:
    PROPERTIES.add("enum.property")

# List of possible property names. We use this list in order
# to see if a method is a property or not. This should be
# pretty reliable and fast, the alternative being to check each
# decorator to see if its a real property-like descriptor, which
# can be too complicated.
# Also, these aren't qualified, because each project can
# define them, we shouldn't expect to know every possible
# property-like decorator!
POSSIBLE_PROPERTIES = {
    "cached_property",
    "cachedproperty",
    "lazyproperty",
    "lazy_property",
    "reify",
    "lazyattribute",
    "lazy_attribute",
    "LazyProperty",
    "lazy",
    "cache_readonly",
    "DynamicClassAttribute",
}


def _is_property(meth, context=None):
    decoratornames = meth.decoratornames(context=context)
    if PROPERTIES.intersection(decoratornames):
        return True
    stripped = {
        name.split(".")[-1] for name in decoratornames if name is not Uninferable
    }
    if any(name in stripped for name in POSSIBLE_PROPERTIES):
        return True

    # Lookup for subclasses of *property*
    if not meth.decorators:
        return False
    for decorator in meth.decorators.nodes or ():
        inferred = helpers.safe_infer(decorator, context=context)
        if inferred is None or inferred is Uninferable:
            continue
        if inferred.__class__.__name__ == "ClassDef":
            for base_class in inferred.bases:
                if base_class.__class__.__name__ != "Name":
                    continue
                module, _ = base_class.lookup(base_class.name)
                if module.name == "builtins" and base_class.name == "property":
                    return True

    return False


class Proxy:
    """a simple proxy object

    Note:

    Subclasses of this object will need a custom __getattr__
    if new instance attributes are created. See the Const class
    """

    _proxied = None  # proxied object may be set by class or by instance

    def __init__(self, proxied=None):
        if proxied is not None:
            self._proxied = proxied

    def __getattr__(self, name):
        if name == "_proxied":
            return self.__class__._proxied
        if name in self.__dict__:
            return self.__dict__[name]
        return getattr(self._proxied, name)

    def infer(self, context=None):
        yield self


def _infer_stmts(stmts, context, frame=None):
    """Return an iterator on statements inferred by each statement in *stmts*."""
    inferred = False
    if context is not None:
        name = context.lookupname
        context = context.clone()
    else:
        name = None
        context = InferenceContext()

    for stmt in stmts:
        if stmt is Uninferable:
            yield stmt
            inferred = True
            continue
        context.lookupname = stmt._infer_name(frame, name)
        try:
            for inf in stmt.infer(context=context):
                yield inf
                inferred = True
        except NameInferenceError:
            continue
        except InferenceError:
            yield Uninferable
            inferred = True
    if not inferred:
        raise InferenceError(
            "Inference failed for all members of {stmts!r}.",
            stmts=stmts,
            frame=frame,
            context=context,
        )


def _infer_method_result_truth(instance, method_name, context):
    # Get the method from the instance and try to infer
    # its return's truth value.
    meth = next(instance.igetattr(method_name, context=context), None)
    if meth and hasattr(meth, "infer_call_result"):
        if not meth.callable():
            return Uninferable
        try:
            context.callcontext = CallContext(args=[], callee=meth)
            for value in meth.infer_call_result(instance, context=context):
                if value is Uninferable:
                    return value
                try:
                    inferred = next(value.infer(context=context))
                except StopIteration as e:
                    raise InferenceError(context=context) from e
                return inferred.bool_value()
        except InferenceError:
            pass
    return Uninferable


class BaseInstance(Proxy):
    """An instance base class, which provides lookup methods for potential instances."""

    special_attributes = None

    def display_type(self):
        return "Instance of"

    def getattr(self, name, context=None, lookupclass=True):
        try:
            values = self._proxied.instance_attr(name, context)
        except AttributeInferenceError as exc:
            if self.special_attributes and name in self.special_attributes:
                return [self.special_attributes.lookup(name)]

            if lookupclass:
                # Class attributes not available through the instance
                # unless they are explicitly defined.
                return self._proxied.getattr(name, context, class_context=False)

            raise AttributeInferenceError(
                target=self, attribute=name, context=context
            ) from exc
        # since we've no context information, return matching class members as
        # well
        if lookupclass:
            try:
                return values + self._proxied.getattr(
                    name, context, class_context=False
                )
            except AttributeInferenceError:
                pass
        return values

    def igetattr(self, name, context=None):
        """inferred getattr"""
        if not context:
            context = InferenceContext()
        try:
            context.lookupname = name
            # avoid recursively inferring the same attr on the same class
            if context.push(self._proxied):
                raise InferenceError(
                    message="Cannot infer the same attribute again",
                    node=self,
                    context=context,
                )

            # XXX frame should be self._proxied, or not ?
            get_attr = self.getattr(name, context, lookupclass=False)
            yield from _infer_stmts(
                self._wrap_attr(get_attr, context), context, frame=self
            )
        except AttributeInferenceError:
            try:
                # fallback to class.igetattr since it has some logic to handle
                # descriptors
                # But only if the _proxied is the Class.
                if self._proxied.__class__.__name__ != "ClassDef":
                    raise
                attrs = self._proxied.igetattr(name, context, class_context=False)
                yield from self._wrap_attr(attrs, context)
            except AttributeInferenceError as error:
                raise InferenceError(**vars(error)) from error

    def _wrap_attr(self, attrs, context=None):
        """wrap bound methods of attrs in a InstanceMethod proxies"""
        for attr in attrs:
            if isinstance(attr, UnboundMethod):
                if _is_property(attr):
                    yield from attr.infer_call_result(self, context)
                else:
                    yield BoundMethod(attr, self)
            elif hasattr(attr, "name") and attr.name == "<lambda>":
                if attr.args.arguments and attr.args.arguments[0].name == "self":
                    yield BoundMethod(attr, self)
                    continue
                yield attr
            else:
                yield attr

    def infer_call_result(self, caller, context=None):
        """infer what a class instance is returning when called"""
        context = bind_context_to_node(context, self)
        inferred = False
        for node in self._proxied.igetattr("__call__", context):
            if node is Uninferable or not node.callable():
                continue
            for res in node.infer_call_result(caller, context):
                inferred = True
                yield res
        if not inferred:
            raise InferenceError(node=self, caller=caller, context=context)


class Instance(BaseInstance):
    """A special node representing a class instance."""

    # pylint: disable=unnecessary-lambda
    special_attributes = lazy_descriptor(lambda: objectmodel.InstanceModel())

    def __repr__(self):
        return "<Instance of {}.{} at 0x{}>".format(
            self._proxied.root().name, self._proxied.name, id(self)
        )

    def __str__(self):
        return f"Instance of {self._proxied.root().name}.{self._proxied.name}"

    def callable(self):
        try:
            self._proxied.getattr("__call__", class_context=False)
            return True
        except AttributeInferenceError:
            return False

    def pytype(self):
        return self._proxied.qname()

    def display_type(self):
        return "Instance of"

    def bool_value(self, context=None):
        """Infer the truth value for an Instance

        The truth value of an instance is determined by these conditions:

           * if it implements __bool__ on Python 3 or __nonzero__
             on Python 2, then its bool value will be determined by
             calling this special method and checking its result.
           * when this method is not defined, __len__() is called, if it
             is defined, and the object is considered true if its result is
             nonzero. If a class defines neither __len__() nor __bool__(),
             all its instances are considered true.
        """
        context = context or InferenceContext()
        context.boundnode = self

        try:
            result = _infer_method_result_truth(self, BOOL_SPECIAL_METHOD, context)
        except (InferenceError, AttributeInferenceError):
            # Fallback to __len__.
            try:
                result = _infer_method_result_truth(self, "__len__", context)
            except (AttributeInferenceError, InferenceError):
                return True
        return result

    def getitem(self, index, context=None):
        # TODO: Rewrap index to Const for this case
        new_context = bind_context_to_node(context, self)
        if not context:
            context = new_context
        method = next(self.igetattr("__getitem__", context=context), None)
        # Create a new CallContext for providing index as an argument.
        new_context.callcontext = CallContext(args=[index], callee=method)
        if not isinstance(method, BoundMethod):
            raise InferenceError(
                "Could not find __getitem__ for {node!r}.", node=self, context=context
            )
        if len(method.args.arguments) != 2:  # (self, index)
            raise AstroidTypeError(
                "__getitem__ for {node!r} does not have correct signature",
                node=self,
                context=context,
            )
        return next(method.infer_call_result(self, new_context), None)


class UnboundMethod(Proxy):
    """a special node representing a method not bound to an instance"""

    # pylint: disable=unnecessary-lambda
    special_attributes = lazy_descriptor(lambda: objectmodel.UnboundMethodModel())

    def __repr__(self):
        frame = self._proxied.parent.frame(future=True)
        return "<{} {} of {} at 0x{}".format(
            self.__class__.__name__, self._proxied.name, frame.qname(), id(self)
        )

    def implicit_parameters(self):
        return 0

    def is_bound(self):
        return False

    def getattr(self, name, context=None):
        if name in self.special_attributes:
            return [self.special_attributes.lookup(name)]
        return self._proxied.getattr(name, context)

    def igetattr(self, name, context=None):
        if name in self.special_attributes:
            return iter((self.special_attributes.lookup(name),))
        return self._proxied.igetattr(name, context)

    def infer_call_result(self, caller, context):
        """
        The boundnode of the regular context with a function called
        on ``object.__new__`` will be of type ``object``,
        which is incorrect for the argument in general.
        If no context is given the ``object.__new__`` call argument will
        correctly inferred except when inside a call that requires
        the additional context (such as a classmethod) of the boundnode
        to determine which class the method was called from
        """

        # If we're unbound method __new__ of builtin object, the result is an
        # instance of the class given as first argument.
        if (
            self._proxied.name == "__new__"
            and self._proxied.parent.frame(future=True).qname() == "builtins.object"
        ):
            if caller.args:
                node_context = context.extra_context.get(caller.args[0])
                infer = caller.args[0].infer(context=node_context)
            else:
                infer = []
            return (Instance(x) if x is not Uninferable else x for x in infer)
        return self._proxied.infer_call_result(caller, context)

    def bool_value(self, context=None):
        return True


class BoundMethod(UnboundMethod):
    """a special node representing a method bound to an instance"""

    # pylint: disable=unnecessary-lambda
    special_attributes = lazy_descriptor(lambda: objectmodel.BoundMethodModel())

    def __init__(self, proxy, bound):
        super().__init__(proxy)
        self.bound = bound

    def implicit_parameters(self):
        if self.name == "__new__":
            # __new__ acts as a classmethod but the class argument is not implicit.
            return 0
        return 1

    def is_bound(self):
        return True

    def _infer_type_new_call(self, caller, context):
        """Try to infer what type.__new__(mcs, name, bases, attrs) returns.

        In order for such call to be valid, the metaclass needs to be
        a subtype of ``type``, the name needs to be a string, the bases
        needs to be a tuple of classes
        """
        # pylint: disable=import-outside-toplevel; circular import
        from metaflow._vendor.astroid.nodes import Pass

        # Verify the metaclass
        try:
            mcs = next(caller.args[0].infer(context=context))
        except StopIteration as e:
            raise InferenceError(context=context) from e
        if mcs.__class__.__name__ != "ClassDef":
            # Not a valid first argument.
            return None
        if not mcs.is_subtype_of("builtins.type"):
            # Not a valid metaclass.
            return None

        # Verify the name
        try:
            name = next(caller.args[1].infer(context=context))
        except StopIteration as e:
            raise InferenceError(context=context) from e
        if name.__class__.__name__ != "Const":
            # Not a valid name, needs to be a const.
            return None
        if not isinstance(name.value, str):
            # Needs to be a string.
            return None

        # Verify the bases
        try:
            bases = next(caller.args[2].infer(context=context))
        except StopIteration as e:
            raise InferenceError(context=context) from e
        if bases.__class__.__name__ != "Tuple":
            # Needs to be a tuple.
            return None
        try:
            inferred_bases = [next(elt.infer(context=context)) for elt in bases.elts]
        except StopIteration as e:
            raise InferenceError(context=context) from e
        if any(base.__class__.__name__ != "ClassDef" for base in inferred_bases):
            # All the bases needs to be Classes
            return None

        # Verify the attributes.
        try:
            attrs = next(caller.args[3].infer(context=context))
        except StopIteration as e:
            raise InferenceError(context=context) from e
        if attrs.__class__.__name__ != "Dict":
            # Needs to be a dictionary.
            return None
        cls_locals = collections.defaultdict(list)
        for key, value in attrs.items:
            try:
                key = next(key.infer(context=context))
            except StopIteration as e:
                raise InferenceError(context=context) from e
            try:
                value = next(value.infer(context=context))
            except StopIteration as e:
                raise InferenceError(context=context) from e
            # Ignore non string keys
            if key.__class__.__name__ == "Const" and isinstance(key.value, str):
                cls_locals[key.value].append(value)

        # Build the class from now.
        cls = mcs.__class__(
            name=name.value,
            lineno=caller.lineno,
            col_offset=caller.col_offset,
            parent=caller,
        )
        empty = Pass()
        cls.postinit(
            bases=bases.elts,
            body=[empty],
            decorators=[],
            newstyle=True,
            metaclass=mcs,
            keywords=[],
        )
        cls.locals = cls_locals
        return cls

    def infer_call_result(self, caller, context=None):
        context = bind_context_to_node(context, self.bound)
        if (
            self.bound.__class__.__name__ == "ClassDef"
            and self.bound.name == "type"
            and self.name == "__new__"
            and len(caller.args) == 4
        ):
            # Check if we have a ``type.__new__(mcs, name, bases, attrs)`` call.
            new_cls = self._infer_type_new_call(caller, context)
            if new_cls:
                return iter((new_cls,))

        return super().infer_call_result(caller, context)

    def bool_value(self, context=None):
        return True


class Generator(BaseInstance):
    """a special node representing a generator.

    Proxied class is set once for all in raw_building.
    """

    special_attributes = lazy_descriptor(objectmodel.GeneratorModel)

    def __init__(self, parent=None, generator_initial_context=None):
        super().__init__()
        self.parent = parent
        self._call_context = copy_context(generator_initial_context)

    @decorators.cached
    def infer_yield_types(self):
        yield from self.parent.infer_yield_result(self._call_context)

    def callable(self):
        return False

    def pytype(self):
        return "builtins.generator"

    def display_type(self):
        return "Generator"

    def bool_value(self, context=None):
        return True

    def __repr__(self):
        return f"<Generator({self._proxied.name}) l.{self.lineno} at 0x{id(self)}>"

    def __str__(self):
        return f"Generator({self._proxied.name})"


class AsyncGenerator(Generator):
    """Special node representing an async generator"""

    def pytype(self):
        return "builtins.async_generator"

    def display_type(self):
        return "AsyncGenerator"

    def __repr__(self):
        return f"<AsyncGenerator({self._proxied.name}) l.{self.lineno} at 0x{id(self)}>"

    def __str__(self):
        return f"AsyncGenerator({self._proxied.name})"
