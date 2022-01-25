# Copyright (c) 2019-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2019-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE


"""Different utilities for the numpy brains"""
from typing import Tuple

from metaflow._vendor.astroid.builder import extract_node
from metaflow._vendor.astroid.nodes.node_classes import Attribute, Import, Name, NodeNG

# Class subscript is available in numpy starting with version 1.20.0
NUMPY_VERSION_TYPE_HINTS_SUPPORT = ("1", "20", "0")


def numpy_supports_type_hints() -> bool:
    """
    Returns True if numpy supports type hints
    """
    np_ver = _get_numpy_version()
    return np_ver and np_ver > NUMPY_VERSION_TYPE_HINTS_SUPPORT


def _get_numpy_version() -> Tuple[str, str, str]:
    """
    Return the numpy version number if numpy can be imported. Otherwise returns
    ('0', '0', '0')
    """
    try:
        import numpy  # pylint: disable=import-outside-toplevel

        return tuple(numpy.version.version.split("."))
    except ImportError:
        return ("0", "0", "0")


def infer_numpy_member(src, node, context=None):
    node = extract_node(src)
    return node.infer(context=context)


def _is_a_numpy_module(node: Name) -> bool:
    """
    Returns True if the node is a representation of a numpy module.

    For example in :
        import numpy as np
        x = np.linspace(1, 2)
    The node <Name.np> is a representation of the numpy module.

    :param node: node to test
    :return: True if the node is a representation of the numpy module.
    """
    module_nickname = node.name
    potential_import_target = [
        x for x in node.lookup(module_nickname)[1] if isinstance(x, Import)
    ]
    return any(
        ("numpy", module_nickname) in target.names or ("numpy", None) in target.names
        for target in potential_import_target
    )


def looks_like_numpy_member(member_name: str, node: NodeNG) -> bool:
    """
    Returns True if the node is a member of numpy whose
    name is member_name.

    :param member_name: name of the member
    :param node: node to test
    :return: True if the node is a member of numpy
    """
    if (
        isinstance(node, Attribute)
        and node.attrname == member_name
        and isinstance(node.expr, Name)
        and _is_a_numpy_module(node.expr)
    ):
        return True
    if (
        isinstance(node, Name)
        and node.name == member_name
        and node.root().name.startswith("numpy")
    ):
        return True
    return False
