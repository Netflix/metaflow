# Copyright (c) 2007, 2009-2010, 2013 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2014 Google, Inc.
# Copyright (c) 2015-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2016 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Andrew Haigh <hello@nelf.in>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""this module contains exceptions used in the astroid library
"""
from typing import TYPE_CHECKING

from metaflow._vendor.astroid import util

if TYPE_CHECKING:
    from metaflow._vendor.astroid import nodes

__all__ = (
    "AstroidBuildingError",
    "AstroidBuildingException",
    "AstroidError",
    "AstroidImportError",
    "AstroidIndexError",
    "AstroidSyntaxError",
    "AstroidTypeError",
    "AstroidValueError",
    "AttributeInferenceError",
    "BinaryOperationError",
    "DuplicateBasesError",
    "InconsistentMroError",
    "InferenceError",
    "InferenceOverwriteError",
    "MroError",
    "NameInferenceError",
    "NoDefault",
    "NotFoundError",
    "OperationError",
    "ResolveError",
    "SuperArgumentTypeError",
    "SuperError",
    "TooManyLevelsError",
    "UnaryOperationError",
    "UnresolvableName",
    "UseInferenceDefault",
)


class AstroidError(Exception):
    """base exception class for all astroid related exceptions

    AstroidError and its subclasses are structured, intended to hold
    objects representing state when the exception is thrown.  Field
    values are passed to the constructor as keyword-only arguments.
    Each subclass has its own set of standard fields, but use your
    best judgment to decide whether a specific exception instance
    needs more or fewer fields for debugging.  Field values may be
    used to lazily generate the error message: self.message.format()
    will be called with the field names and values supplied as keyword
    arguments.
    """

    def __init__(self, message="", **kws):
        super().__init__(message)
        self.message = message
        for key, value in kws.items():
            setattr(self, key, value)

    def __str__(self):
        return self.message.format(**vars(self))


class AstroidBuildingError(AstroidError):
    """exception class when we are unable to build an astroid representation

    Standard attributes:
        modname: Name of the module that AST construction failed for.
        error: Exception raised during construction.
    """

    def __init__(self, message="Failed to import module {modname}.", **kws):
        super().__init__(message, **kws)


class AstroidImportError(AstroidBuildingError):
    """Exception class used when a module can't be imported by astroid."""


class TooManyLevelsError(AstroidImportError):
    """Exception class which is raised when a relative import was beyond the top-level.

    Standard attributes:
        level: The level which was attempted.
        name: the name of the module on which the relative import was attempted.
    """

    level = None
    name = None

    def __init__(
        self,
        message="Relative import with too many levels " "({level}) for module {name!r}",
        **kws,
    ):
        super().__init__(message, **kws)


class AstroidSyntaxError(AstroidBuildingError):
    """Exception class used when a module can't be parsed."""


class NoDefault(AstroidError):
    """raised by function's `default_value` method when an argument has
    no default value

    Standard attributes:
        func: Function node.
        name: Name of argument without a default.
    """

    func = None
    name = None

    def __init__(self, message="{func!r} has no default for {name!r}.", **kws):
        super().__init__(message, **kws)


class ResolveError(AstroidError):
    """Base class of astroid resolution/inference error.

    ResolveError is not intended to be raised.

    Standard attributes:
        context: InferenceContext object.
    """

    context = None


class MroError(ResolveError):
    """Error raised when there is a problem with method resolution of a class.

    Standard attributes:
        mros: A sequence of sequences containing ClassDef nodes.
        cls: ClassDef node whose MRO resolution failed.
        context: InferenceContext object.
    """

    mros = ()
    cls = None

    def __str__(self):
        mro_names = ", ".join(f"({', '.join(b.name for b in m)})" for m in self.mros)
        return self.message.format(mros=mro_names, cls=self.cls)


class DuplicateBasesError(MroError):
    """Error raised when there are duplicate bases in the same class bases."""


class InconsistentMroError(MroError):
    """Error raised when a class's MRO is inconsistent."""


class SuperError(ResolveError):
    """Error raised when there is a problem with a *super* call.

    Standard attributes:
        *super_*: The Super instance that raised the exception.
        context: InferenceContext object.
    """

    super_ = None

    def __str__(self):
        return self.message.format(**vars(self.super_))


class InferenceError(ResolveError):
    """raised when we are unable to infer a node

    Standard attributes:
        node: The node inference was called on.
        context: InferenceContext object.
    """

    node = None
    context = None

    def __init__(self, message="Inference failed for {node!r}.", **kws):
        super().__init__(message, **kws)


# Why does this inherit from InferenceError rather than ResolveError?
# Changing it causes some inference tests to fail.
class NameInferenceError(InferenceError):
    """Raised when a name lookup fails, corresponds to NameError.

    Standard attributes:
        name: The name for which lookup failed, as a string.
        scope: The node representing the scope in which the lookup occurred.
        context: InferenceContext object.
    """

    name = None
    scope = None

    def __init__(self, message="{name!r} not found in {scope!r}.", **kws):
        super().__init__(message, **kws)


class AttributeInferenceError(ResolveError):
    """Raised when an attribute lookup fails, corresponds to AttributeError.

    Standard attributes:
        target: The node for which lookup failed.
        attribute: The attribute for which lookup failed, as a string.
        context: InferenceContext object.
    """

    target = None
    attribute = None

    def __init__(self, message="{attribute!r} not found on {target!r}.", **kws):
        super().__init__(message, **kws)


class UseInferenceDefault(Exception):
    """exception to be raised in custom inference function to indicate that it
    should go back to the default behaviour
    """


class _NonDeducibleTypeHierarchy(Exception):
    """Raised when is_subtype / is_supertype can't deduce the relation between two types."""


class AstroidIndexError(AstroidError):
    """Raised when an Indexable / Mapping does not have an index / key."""


class AstroidTypeError(AstroidError):
    """Raised when a TypeError would be expected in Python code."""


class AstroidValueError(AstroidError):
    """Raised when a ValueError would be expected in Python code."""


class InferenceOverwriteError(AstroidError):
    """Raised when an inference tip is overwritten

    Currently only used for debugging.
    """


class ParentMissingError(AstroidError):
    """Raised when a node which is expected to have a parent attribute is missing one

    Standard attributes:
        target: The node for which the parent lookup failed.
    """

    def __init__(self, target: "nodes.NodeNG") -> None:
        self.target = target
        super().__init__(message=f"Parent not found on {target!r}.")


class StatementMissing(ParentMissingError):
    """Raised when a call to node.statement() does not return a node. This is because
    a node in the chain does not have a parent attribute and therefore does not
    return a node for statement().

    Standard attributes:
        target: The node for which the parent lookup failed.
    """

    def __init__(self, target: "nodes.NodeNG") -> None:
        # pylint: disable-next=bad-super-call
        # https://github.com/PyCQA/pylint/issues/2903
        # https://github.com/PyCQA/astroid/pull/1217#discussion_r744149027
        super(ParentMissingError, self).__init__(
            message=f"Statement not found on {target!r}"
        )


# Backwards-compatibility aliases
OperationError = util.BadOperationMessage
UnaryOperationError = util.BadUnaryOperationMessage
BinaryOperationError = util.BadBinaryOperationMessage

SuperArgumentTypeError = SuperError
UnresolvableName = NameInferenceError
NotFoundError = AttributeInferenceError
AstroidBuildingException = AstroidBuildingError
