# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE


"""This module contains all classes that are considered a "scoped" node and anything related.
A scope node is a node that opens a new local scope in the language definition:
Module, ClassDef, FunctionDef (and Lambda, GeneratorExp, DictComp and SetComp to some extent).
"""
from metaflow._vendor.astroid.nodes.scoped_nodes.scoped_nodes import (
    AsyncFunctionDef,
    ClassDef,
    ComprehensionScope,
    DictComp,
    FunctionDef,
    GeneratorExp,
    Lambda,
    ListComp,
    LocalsDictNodeNG,
    Module,
    SetComp,
    _is_metaclass,
    builtin_lookup,
    function_to_method,
    get_wrapping_class,
)

__all__ = (
    "AsyncFunctionDef",
    "ClassDef",
    "ComprehensionScope",
    "DictComp",
    "FunctionDef",
    "GeneratorExp",
    "Lambda",
    "ListComp",
    "LocalsDictNodeNG",
    "Module",
    "SetComp",
    "builtin_lookup",
    "function_to_method",
    "get_wrapping_class",
    "_is_metaclass",
)
