# Copyright (c) 2019-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE


"""Astroid hooks for numpy.core.fromnumeric module."""
from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import parse
from metaflow._vendor.astroid.manager import AstroidManager


def numpy_core_fromnumeric_transform():
    return parse(
        """
    def sum(a, axis=None, dtype=None, out=None, keepdims=None, initial=None):
        return numpy.ndarray([0, 0])
    """
    )


register_module_extender(
    AstroidManager(), "numpy.core.fromnumeric", numpy_core_fromnumeric_transform
)
