# Copyright 2019 The Kubeflow Authors
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

__all__ = [
    'get_canonical_type_name_for_type',
    'get_canonical_type_for_type_name',
    'get_deserializer_code_for_type_name',
    'get_serializer_func_for_type_name',
]

import inspect
from typing import Any, Callable, NamedTuple, Optional, Sequence, Type
import warnings

from kfp.components import type_annotation_utils

Converter = NamedTuple('Converter', [
    ('types', Sequence[Type]),
    ('type_names', Sequence[str]),
    ('serializer', Callable[[Any], str]),
    ('deserializer_code', str),
    ('definitions', str),
])


def _serialize_str(str_value: str) -> str:
    if not isinstance(str_value, str):
        raise TypeError('Value "{}" has type "{}" instead of str.'.format(
            str(str_value), str(type(str_value))))
    return str_value


def _serialize_int(int_value: int) -> str:
    if isinstance(int_value, str):
        return int_value
    if not isinstance(int_value, int):
        raise TypeError('Value "{}" has type "{}" instead of int.'.format(
            str(int_value), str(type(int_value))))
    return str(int_value)


def _serialize_float(float_value: float) -> str:
    if isinstance(float_value, str):
        return float_value
    if not isinstance(float_value, (float, int)):
        raise TypeError('Value "{}" has type "{}" instead of float.'.format(
            str(float_value), str(type(float_value))))
    return str(float_value)


def _serialize_bool(bool_value: bool) -> str:
    if isinstance(bool_value, str):
        return bool_value
    if not isinstance(bool_value, bool):
        raise TypeError('Value "{}" has type "{}" instead of bool.'.format(
            str(bool_value), str(type(bool_value))))
    return str(bool_value)


def _deserialize_bool(s) -> bool:
    from distutils.util import strtobool
    return strtobool(s) == 1


_bool_deserializer_definitions = inspect.getsource(_deserialize_bool)
_bool_deserializer_code = _deserialize_bool.__name__


def _serialize_json(obj) -> str:
    if isinstance(obj, str):
        return obj
    import json

    def default_serializer(obj):
        if hasattr(obj, 'to_struct'):
            return obj.to_struct()
        else:
            raise TypeError(
                "Object of type '%s' is not JSON serializable and does not have .to_struct() method."
                % obj.__class__.__name__)

    return json.dumps(obj, default=default_serializer, sort_keys=True)


def _serialize_base64_pickle(obj) -> str:
    if isinstance(obj, str):
        return obj
    import base64
    import pickle
    return base64.b64encode(pickle.dumps(obj)).decode('ascii')


def _deserialize_base64_pickle(s):
    import base64
    import pickle
    return pickle.loads(base64.b64decode(s))


_deserialize_base64_pickle_definitions = inspect.getsource(
    _deserialize_base64_pickle)
_deserialize_base64_pickle_code = _deserialize_base64_pickle.__name__

_converters = [
    Converter([str], ['String', 'str'], _serialize_str, 'str', None),
    Converter([int], ['Integer', 'int'], _serialize_int, 'int', None),
    Converter([float], ['Float', 'float'], _serialize_float, 'float', None),
    Converter([bool], ['Boolean', 'Bool', 'bool'], _serialize_bool,
              _bool_deserializer_code, _bool_deserializer_definitions),
    Converter(
        [list], ['JsonArray', 'List', 'list'], _serialize_json, 'json.loads',
        'import json'
    ),  # ! JSON map keys are always strings. Python converts all keys to strings without warnings
    Converter(
        [dict], ['JsonObject', 'Dictionary', 'Dict', 'dict'], _serialize_json,
        'json.loads', 'import json'
    ),  # ! JSON map keys are always strings. Python converts all keys to strings without warnings
    Converter([], ['Json'], _serialize_json, 'json.loads', 'import json'),
    Converter([], ['Base64Pickle'], _serialize_base64_pickle,
              _deserialize_base64_pickle_code,
              _deserialize_base64_pickle_definitions),
]

type_to_type_name = {
    typ: converter.type_names[0] for converter in _converters
    for typ in converter.types
}
type_name_to_type = {
    type_name: converter.types[0] for converter in _converters
    for type_name in converter.type_names
    if converter.types
}
type_to_deserializer = {
    typ: (converter.deserializer_code, converter.definitions)
    for converter in _converters for typ in converter.types
}
type_name_to_deserializer = {
    type_name: (converter.deserializer_code, converter.definitions)
    for converter in _converters for type_name in converter.type_names
}
type_name_to_serializer = {
    type_name: converter.serializer for converter in _converters
    for type_name in converter.type_names
}


def get_canonical_type_name_for_type(typ: Type) -> str:
    """Find the canonical type name for a given type.

    Args:
        typ: The type to search for.

    Returns:
        The canonical name of the type found.
    """
    try:
        return type_to_type_name.get(typ, None)
    except:
        return None


def get_canonical_type_for_type_name(type_name: str) -> Optional[Type]:
    """Find the canonical type for a given type name.

    Args:
        type_name: The type name to search for.

    Returns:
        The canonical type found.
    """
    try:
        return type_name_to_type.get(type_name, None)
    except:
        return None


def get_deserializer_code_for_type_name(type_name: str) -> Optional[str]:
    """Find the deserializer code for the given type name.

    Args:
        type_name: The type name to search for.

    Returns:
        The deserializer code needed to deserialize the type.
    """
    try:
        return type_name_to_deserializer.get(
            type_annotation_utils.get_short_type_name(type_name), None)
    except:
        return None


def get_serializer_func_for_type_name(type_name: str) -> Optional[Callable]:
    """Find the serializer code for the given type name.

    Args:
        type_name: The type name to search for.

    Returns:
        The serializer func needed to serialize the type.
    """
    try:
        return type_name_to_serializer.get(
            type_annotation_utils.get_short_type_name(type_name), None)
    except:
        return None


def serialize_value(value, type_name: str) -> str:
    """serialize_value converts the passed value to string based on the
    serializer associated with the passed type_name."""
    if isinstance(value, str):
        return value  # The value is supposedly already serialized

    if type_name is None:
        type_name = type_to_type_name.get(type(value), type(value).__name__)
        warnings.warn(
            'Missing type name was inferred as "{}" based on the value "{}".'
            .format(type_name, str(value)))

    serializer = type_name_to_serializer.get(
        type_annotation_utils.get_short_type_name(type_name))
    if serializer:
        try:
            serialized_value = serializer(value)
            if not isinstance(serialized_value, str):
                raise TypeError(
                    'Serializer {} returned result of type "{}" instead of string.'
                    .format(serializer, type(serialized_value)))
            return serialized_value
        except Exception as e:
            raise ValueError(
                'Failed to serialize the value "{}" of type "{}" to type "{}". Exception: {}'
                .format(
                    str(value),
                    str(type(value).__name__),
                    str(type_name),
                    str(e),
                ))

    raise TypeError('There are no registered serializers for type "{}".'.format(
        str(type_name),))
