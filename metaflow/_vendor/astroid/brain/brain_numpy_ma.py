# Copyright (c) 2021 hippo91 <guillaume.peillex@gmail.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE
"""Astroid hooks for numpy ma module"""

from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import parse
from metaflow._vendor.astroid.manager import AstroidManager


def numpy_ma_transform():
    """
    Infer the call of the masked_where function

    :param node: node to infer
    :param context: inference context
    """
    return parse(
        """
    import numpy.ma
    def masked_where(condition, a, copy=True):
        return numpy.ma.masked_array(a, mask=[])
    """
    )


register_module_extender(AstroidManager(), "numpy.ma", numpy_ma_transform)
