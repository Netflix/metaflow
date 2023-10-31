# Copyright 2018 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import types
from typing import Any, Callable, Mapping, Sequence
from inspect import Parameter, Signature


class KwParameter(Parameter):

    def __init__(self,
                 name,
                 default=Parameter.empty,
                 annotation=Parameter.empty):
        super().__init__(
            name,
            Parameter.POSITIONAL_OR_KEYWORD,
            default=default,
            annotation=annotation)


def create_function_from_parameter_names(func: Callable[[Mapping[str, Any]],
                                                        Any],
                                         parameter_names: Sequence[str],
                                         documentation=None,
                                         func_name=None,
                                         func_filename=None):
    return create_function_from_parameters(
        func, [KwParameter(name) for name in parameter_names], documentation,
        func_name, func_filename)


def create_function_from_parameters(func: Callable[[Mapping[str, Any]], Any],
                                    parameters: Sequence[Parameter],
                                    documentation=None,
                                    func_name=None,
                                    func_filename=None):
    new_signature = Signature(parameters)  # Checks the parameter consistency

    def pass_locals():
        return dict_func(locals())  # noqa: F821 TODO

    code = pass_locals.__code__
    mod_co_argcount = len(parameters)
    mod_co_nlocals = len(parameters)
    mod_co_varnames = tuple(param.name for param in parameters)
    mod_co_name = func_name or code.co_name
    if func_filename:
        mod_co_filename = func_filename
        mod_co_firstlineno = 1
    else:
        mod_co_filename = code.co_filename
        mod_co_firstlineno = code.co_firstlineno

    if sys.version_info >= (3, 8):
        modified_code = code.replace(
            co_argcount=mod_co_argcount,
            co_nlocals=mod_co_nlocals,
            co_varnames=mod_co_varnames,
            co_filename=mod_co_filename,
            co_name=mod_co_name,
            co_firstlineno=mod_co_firstlineno,
        )
    else:
        modified_code = types.CodeType(
            mod_co_argcount, code.co_kwonlyargcount, mod_co_nlocals,
            code.co_stacksize, code.co_flags, code.co_code, code.co_consts,
            code.co_names, mod_co_varnames, mod_co_filename, mod_co_name,
            mod_co_firstlineno, code.co_lnotab)

    default_arg_values = tuple(
        p.default for p in parameters if p.default != Parameter.empty
    )  #!argdefs "starts from the right"/"is right-aligned"
    modified_func = types.FunctionType(
        modified_code, {
            'dict_func': func,
            'locals': locals
        },
        name=func_name,
        argdefs=default_arg_values)
    modified_func.__doc__ = documentation
    modified_func.__signature__ = new_signature

    return modified_func
