# Copyright (c) 2015-2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2016 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2018, 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018 Tomas Gavenciak <gavento@ucw.cz>
# Copyright (c) 2018 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2018 HoverHell <hoverhell@gmail.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Ram Rachum <ram@rachum.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

""" A few useful function/method decorators."""

import functools
import inspect
import sys
import warnings
from typing import Callable, TypeVar

from metaflow._vendor import wrapt

from metaflow._vendor.astroid import util
from metaflow._vendor.astroid.context import InferenceContext
from metaflow._vendor.astroid.exceptions import InferenceError

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from metaflow._vendor.typing_extensions import ParamSpec

R = TypeVar("R")
P = ParamSpec("P")


@wrapt.decorator
def cached(func, instance, args, kwargs):
    """Simple decorator to cache result of method calls without args."""
    cache = getattr(instance, "__cache", None)
    if cache is None:
        instance.__cache = cache = {}
    try:
        return cache[func]
    except KeyError:
        cache[func] = result = func(*args, **kwargs)
        return result


class cachedproperty:
    """Provides a cached property equivalent to the stacking of
    @cached and @property, but more efficient.

    After first usage, the <property_name> becomes part of the object's
    __dict__. Doing:

      del obj.<property_name> empties the cache.

    Idea taken from the pyramid_ framework and the mercurial_ project.

    .. _pyramid: http://pypi.python.org/pypi/pyramid
    .. _mercurial: http://pypi.python.org/pypi/Mercurial
    """

    __slots__ = ("wrapped",)

    def __init__(self, wrapped):
        try:
            wrapped.__name__
        except AttributeError as exc:
            raise TypeError(f"{wrapped} must have a __name__ attribute") from exc
        self.wrapped = wrapped

    @property
    def __doc__(self):
        doc = getattr(self.wrapped, "__doc__", None)
        return "<wrapped by the cachedproperty decorator>%s" % (
            "\n%s" % doc if doc else ""
        )

    def __get__(self, inst, objtype=None):
        if inst is None:
            return self
        val = self.wrapped(inst)
        setattr(inst, self.wrapped.__name__, val)
        return val


def path_wrapper(func):
    """return the given infer function wrapped to handle the path

    Used to stop inference if the node has already been looked
    at for a given `InferenceContext` to prevent infinite recursion
    """

    @functools.wraps(func)
    def wrapped(node, context=None, _func=func, **kwargs):
        """wrapper function handling context"""
        if context is None:
            context = InferenceContext()
        if context.push(node):
            return

        yielded = set()

        for res in _func(node, context, **kwargs):
            # unproxy only true instance, not const, tuple, dict...
            if res.__class__.__name__ == "Instance":
                ares = res._proxied
            else:
                ares = res
            if ares not in yielded:
                yield res
                yielded.add(ares)

    return wrapped


@wrapt.decorator
def yes_if_nothing_inferred(func, instance, args, kwargs):
    generator = func(*args, **kwargs)

    try:
        yield next(generator)
    except StopIteration:
        # generator is empty
        yield util.Uninferable
        return

    yield from generator


@wrapt.decorator
def raise_if_nothing_inferred(func, instance, args, kwargs):
    generator = func(*args, **kwargs)
    try:
        yield next(generator)
    except StopIteration as error:
        # generator is empty
        if error.args:
            # pylint: disable=not-a-mapping
            raise InferenceError(**error.args[0]) from error
        raise InferenceError(
            "StopIteration raised without any error information."
        ) from error

    yield from generator


def deprecate_default_argument_values(
    astroid_version: str = "3.0", **arguments: str
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator which emitts a DeprecationWarning if any arguments specified
    are None or not passed at all.

    Arguments should be a key-value mapping, with the key being the argument to check
    and the value being a type annotation as string for the value of the argument.
    """
    # Helpful links
    # Decorator for DeprecationWarning: https://stackoverflow.com/a/49802489
    # Typing of stacked decorators: https://stackoverflow.com/a/68290080

    def deco(func: Callable[P, R]) -> Callable[P, R]:
        """Decorator function."""

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            """Emit DeprecationWarnings if conditions are met."""

            keys = list(inspect.signature(func).parameters.keys())
            for arg, type_annotation in arguments.items():
                try:
                    index = keys.index(arg)
                except ValueError:
                    raise Exception(
                        f"Can't find argument '{arg}' for '{args[0].__class__.__qualname__}'"
                    ) from None
                if (
                    # Check kwargs
                    # - if found, check it's not None
                    (arg in kwargs and kwargs[arg] is None)
                    # Check args
                    # - make sure not in kwargs
                    # - len(args) needs to be long enough, if too short
                    #   arg can't be in args either
                    # - args[index] should not be None
                    or arg not in kwargs
                    and (
                        index == -1
                        or len(args) <= index
                        or (len(args) > index and args[index] is None)
                    )
                ):
                    warnings.warn(
                        f"'{arg}' will be a required argument for "
                        f"'{args[0].__class__.__qualname__}.{func.__name__}' in astroid {astroid_version} "
                        f"('{arg}' should be of type: '{type_annotation}')",
                        DeprecationWarning,
                    )
            return func(*args, **kwargs)

        return wrapper

    return deco
