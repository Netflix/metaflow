# Copyright (c) 2019-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE


"""Astroid hooks for numpy.core.numeric module."""

import functools

from metaflow._vendor.astroid.brain.brain_numpy_utils import infer_numpy_member, looks_like_numpy_member
from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import parse
from metaflow._vendor.astroid.inference_tip import inference_tip
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes.node_classes import Attribute


def numpy_core_numeric_transform():
    return parse(
        """
    # different functions defined in numeric.py
    import numpy
    def zeros_like(a, dtype=None, order='K', subok=True): return numpy.ndarray((0, 0))
    def ones_like(a, dtype=None, order='K', subok=True): return numpy.ndarray((0, 0))
    def full_like(a, fill_value, dtype=None, order='K', subok=True): return numpy.ndarray((0, 0))
        """
    )


register_module_extender(
    AstroidManager(), "numpy.core.numeric", numpy_core_numeric_transform
)


METHODS_TO_BE_INFERRED = {
    "ones": """def ones(shape, dtype=None, order='C'):
            return numpy.ndarray([0, 0])"""
}


for method_name, function_src in METHODS_TO_BE_INFERRED.items():
    inference_function = functools.partial(infer_numpy_member, function_src)
    AstroidManager().register_transform(
        Attribute,
        inference_tip(inference_function),
        functools.partial(looks_like_numpy_member, method_name),
    )
