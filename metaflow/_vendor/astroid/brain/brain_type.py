"""
Astroid hooks for type support.

Starting from python3.9, type object behaves as it had __class_getitem__ method.
However it was not possible to simply add this method inside type's body, otherwise
all types would also have this method. In this case it would have been possible
to write str[int].
Guido Van Rossum proposed a hack to handle this in the interpreter:
https://github.com/python/cpython/blob/67e394562d67cbcd0ac8114e5439494e7645b8f5/Objects/abstract.c#L181-L184

This brain follows the same logic. It is no wise to add permanently the __class_getitem__ method
to the type object. Instead we choose to add it only in the case of a subscript node
which inside name node is type.
Doing this type[int] is allowed whereas str[int] is not.

Thanks to Lukasz Langa for fruitful discussion.
"""

from metaflow._vendor.astroid import extract_node, inference_tip, nodes
from metaflow._vendor.astroid.const import PY39_PLUS
from metaflow._vendor.astroid.exceptions import UseInferenceDefault
from metaflow._vendor.astroid.manager import AstroidManager


def _looks_like_type_subscript(node):
    """
    Try to figure out if a Name node is used inside a type related subscript

    :param node: node to check
    :type node: astroid.nodes.node_classes.NodeNG
    :return: true if the node is a Name node inside a type related subscript
    :rtype: bool
    """
    if isinstance(node, nodes.Name) and isinstance(node.parent, nodes.Subscript):
        return node.name == "type"
    return False


def infer_type_sub(node, context=None):
    """
    Infer a type[...] subscript

    :param node: node to infer
    :type node: astroid.nodes.node_classes.NodeNG
    :param context: inference context
    :type context: astroid.context.InferenceContext
    :return: the inferred node
    :rtype: nodes.NodeNG
    """
    node_scope, _ = node.scope().lookup("type")
    if not isinstance(node_scope, nodes.Module) or node_scope.qname() != "builtins":
        raise UseInferenceDefault()
    class_src = """
    class type:
        def __class_getitem__(cls, key):
            return cls
     """
    node = extract_node(class_src)
    return node.infer(context=context)


if PY39_PLUS:
    AstroidManager().register_transform(
        nodes.Name, inference_tip(infer_type_sub), _looks_like_type_subscript
    )
