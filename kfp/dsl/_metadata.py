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

import warnings
from .types import BaseType, _check_valid_type_dict
from ..components._data_passing import serialize_value
from ..components.structures import ComponentSpec, InputSpec, OutputSpec


def _annotation_to_typemeta(annotation):
    """_annotation_to_type_meta converts an annotation to a type structure.

    Args:
      annotation(BaseType/str/dict): input/output annotations
        BaseType: registered in kfp.dsl.types
        str: either a string of a dict serialization or a string of the type name
        dict: type name and properties. note that the properties values can be
          dict.

    Returns:
      dict or string representing the type
    """
    if isinstance(annotation, BaseType):
        arg_type = annotation.to_dict()
    elif isinstance(annotation, str):
        arg_type = annotation
    elif isinstance(annotation, dict):
        if not _check_valid_type_dict(annotation):
            raise ValueError('Annotation ' + str(annotation) +
                             ' is not a valid type dictionary.')
        arg_type = annotation
    else:
        return None
    return arg_type


def _extract_pipeline_metadata(func):
    """Creates pipeline metadata structure instance based on the function
    signature."""

    # Most of this code is only needed for verifying the default values against
    # "openapi_schema_validator" type properties.
    # TODO: Move the value verification code to some other place

    from ._pipeline_param import PipelineParam

    import inspect
    fullargspec = inspect.getfullargspec(func)
    args = fullargspec.args
    annotations = fullargspec.annotations

    # defaults
    arg_defaults = {}
    if fullargspec.defaults:
        for arg, default in zip(
                reversed(fullargspec.args), reversed(fullargspec.defaults)):
            arg_defaults[arg] = default

    for arg in args:
        arg_type = None
        arg_default = arg_defaults[arg] if arg in arg_defaults else None
        if isinstance(arg_default, PipelineParam):
            warnings.warn(
                'Explicit creation of `kfp.dsl.PipelineParam`s by the users is '
                'deprecated. The users should define the parameter type and default '
                'values using standard pythonic constructs: '
                'def my_func(a: int = 1, b: str = "default"):')
            arg_default = arg_default.value
        if arg in annotations:
            arg_type = _annotation_to_typemeta(annotations[arg])
        arg_type_properties = list(arg_type.values())[0] if isinstance(
            arg_type, dict) else {}
        if 'openapi_schema_validator' in arg_type_properties and (arg_default
                                                                  is not None):
            from jsonschema import validate
            import json
            schema_object = arg_type_properties['openapi_schema_validator']
            if isinstance(schema_object, str):
                # In case the property value for the schema validator is a string
                # instead of a dict.
                schema_object = json.loads(schema_object)
            # Only validating non-serialized values
            validate(instance=arg_default, schema=schema_object)

    from kfp.components._python_op import _extract_component_interface
    component_spec = _extract_component_interface(func)
    return component_spec
