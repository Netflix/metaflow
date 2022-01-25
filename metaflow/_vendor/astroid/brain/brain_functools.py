# Copyright (c) 2016, 2018-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2018 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Alphadelta14 <alpha@alphaservcomputing.solutions>

"""Astroid hooks for understanding functools library module."""
from functools import partial
from itertools import chain

from metaflow._vendor.astroid import BoundMethod, arguments, extract_node, helpers, objects
from metaflow._vendor.astroid.exceptions import InferenceError, UseInferenceDefault
from metaflow._vendor.astroid.inference_tip import inference_tip
from metaflow._vendor.astroid.interpreter import objectmodel
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes.node_classes import AssignName, Attribute, Call, Name
from metaflow._vendor.astroid.nodes.scoped_nodes import FunctionDef
from metaflow._vendor.astroid.util import Uninferable

LRU_CACHE = "functools.lru_cache"


class LruWrappedModel(objectmodel.FunctionModel):
    """Special attribute model for functions decorated with functools.lru_cache.

    The said decorators patches at decoration time some functions onto
    the decorated function.
    """

    @property
    def attr___wrapped__(self):
        return self._instance

    @property
    def attr_cache_info(self):
        cache_info = extract_node(
            """
        from functools import _CacheInfo
        _CacheInfo(0, 0, 0, 0)
        """
        )

        class CacheInfoBoundMethod(BoundMethod):
            def infer_call_result(self, caller, context=None):
                yield helpers.safe_infer(cache_info)

        return CacheInfoBoundMethod(proxy=self._instance, bound=self._instance)

    @property
    def attr_cache_clear(self):
        node = extract_node("""def cache_clear(self): pass""")
        return BoundMethod(proxy=node, bound=self._instance.parent.scope())


def _transform_lru_cache(node, context=None) -> None:
    # TODO: this is not ideal, since the node should be immutable,
    # but due to https://github.com/PyCQA/astroid/issues/354,
    # there's not much we can do now.
    # Replacing the node would work partially, because,
    # in pylint, the old node would still be available, leading
    # to spurious false positives.
    node.special_attributes = LruWrappedModel()(node)


def _functools_partial_inference(node, context=None):
    call = arguments.CallSite.from_call(node, context=context)
    number_of_positional = len(call.positional_arguments)
    if number_of_positional < 1:
        raise UseInferenceDefault("functools.partial takes at least one argument")
    if number_of_positional == 1 and not call.keyword_arguments:
        raise UseInferenceDefault(
            "functools.partial needs at least to have some filled arguments"
        )

    partial_function = call.positional_arguments[0]
    try:
        inferred_wrapped_function = next(partial_function.infer(context=context))
    except (InferenceError, StopIteration) as exc:
        raise UseInferenceDefault from exc
    if inferred_wrapped_function is Uninferable:
        raise UseInferenceDefault("Cannot infer the wrapped function")
    if not isinstance(inferred_wrapped_function, FunctionDef):
        raise UseInferenceDefault("The wrapped function is not a function")

    # Determine if the passed keywords into the callsite are supported
    # by the wrapped function.
    if not inferred_wrapped_function.args:
        function_parameters = []
    else:
        function_parameters = chain(
            inferred_wrapped_function.args.args or (),
            inferred_wrapped_function.args.posonlyargs or (),
            inferred_wrapped_function.args.kwonlyargs or (),
        )
    parameter_names = {
        param.name for param in function_parameters if isinstance(param, AssignName)
    }
    if set(call.keyword_arguments) - parameter_names:
        raise UseInferenceDefault("wrapped function received unknown parameters")

    partial_function = objects.PartialFunction(
        call,
        name=inferred_wrapped_function.name,
        doc=inferred_wrapped_function.doc,
        lineno=inferred_wrapped_function.lineno,
        col_offset=inferred_wrapped_function.col_offset,
        parent=node.parent,
    )
    partial_function.postinit(
        args=inferred_wrapped_function.args,
        body=inferred_wrapped_function.body,
        decorators=inferred_wrapped_function.decorators,
        returns=inferred_wrapped_function.returns,
        type_comment_returns=inferred_wrapped_function.type_comment_returns,
        type_comment_args=inferred_wrapped_function.type_comment_args,
    )
    return iter((partial_function,))


def _looks_like_lru_cache(node):
    """Check if the given function node is decorated with lru_cache."""
    if not node.decorators:
        return False
    for decorator in node.decorators.nodes:
        if not isinstance(decorator, Call):
            continue
        if _looks_like_functools_member(decorator, "lru_cache"):
            return True
    return False


def _looks_like_functools_member(node, member) -> bool:
    """Check if the given Call node is a functools.partial call"""
    if isinstance(node.func, Name):
        return node.func.name == member
    if isinstance(node.func, Attribute):
        return (
            node.func.attrname == member
            and isinstance(node.func.expr, Name)
            and node.func.expr.name == "functools"
        )
    return False


_looks_like_partial = partial(_looks_like_functools_member, member="partial")


AstroidManager().register_transform(
    FunctionDef, _transform_lru_cache, _looks_like_lru_cache
)


AstroidManager().register_transform(
    Call,
    inference_tip(_functools_partial_inference),
    _looks_like_partial,
)
