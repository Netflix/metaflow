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
"""Module for input/output types in Pipeline DSL.

Feature stage:
[Beta](https://github.com/kubeflow/pipelines/blob/07328e5094ac2981d3059314cc848fbb71437a76/docs/release/feature-stages.md#beta).
"""
from typing import Dict, Union
import warnings

from kfp.v2.components.types import type_utils


class BaseType:
    """BaseType is a base type for all scalar and artifact types."""

    def to_dict(self) -> Union[Dict, str]:
        """to_dict serializes the type instance into a python dictionary or
        string."""
        return {
            type(self).__name__: self.__dict__
        } if self.__dict__ else type(self).__name__


# Primitive Types
class Integer(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {"type": "integer"}


class String(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {"type": "string"}


class Float(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {"type": "number"}


class Bool(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {"type": "boolean"}


class List(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {"type": "array"}


class Dict(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {
            "type": "object",
        }


# GCP Types
class GCSPath(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {
            "type": "string",
            "pattern": "^gs://.*$"
        }


class GCRPath(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {
            "type": "string",
            "pattern": "^.*gcr\\.io/.*$"
        }


class GCPRegion(BaseType):

    def __init__(self):
        self.openapi_schema_validator = {"type": "string"}


class GCPProjectID(BaseType):
    """MetaGCPProjectID: GCP project id"""

    def __init__(self):
        self.openapi_schema_validator = {"type": "string"}


# General Types
class LocalPath(BaseType):
    #TODO: add restriction to path
    def __init__(self):
        self.openapi_schema_validator = {"type": "string"}


class InconsistentTypeException(Exception):
    """InconsistencyTypeException is raised when two types are not
    consistent."""
    pass


class InconsistentTypeWarning(Warning):
    """InconsistentTypeWarning is issued when two types are not consistent."""
    pass


TypeSpecType = Union[str, Dict]


def verify_type_compatibility(given_type: TypeSpecType,
                              expected_type: TypeSpecType,
                              error_message_prefix: str = ""):
    """verify_type_compatibility verifies that the given argument type is
    compatible with the expected input type.

    Args:
            given_type (str/dict): The type of the argument passed to the
              input
            expected_type (str/dict): The declared type of the input
    """
    # Missing types are treated as being compatible with missing types.
    if given_type is None or expected_type is None:
        return True

    # Generic artifacts resulted from missing type or explicit "Artifact" type
    # is compatible with any artifact types.
    # However, generic artifacts resulted from arbitrary unknown types do not
    # have such "compatible" feature.
    if not type_utils.is_parameter_type(
            str(expected_type)) and str(given_type).lower() == "artifact":
        return True
    if not type_utils.is_parameter_type(
            str(given_type)) and str(expected_type).lower() == "artifact":
        return True

    types_are_compatible = check_types(given_type, expected_type)

    if not types_are_compatible:
        error_text = error_message_prefix + (
            'Argument type "{}" is incompatible with the input type "{}"'
        ).format(str(given_type), str(expected_type))
        import kfp
        if kfp.TYPE_CHECK:
            raise InconsistentTypeException(error_text)
        else:
            warnings.warn(InconsistentTypeWarning(error_text))
    return types_are_compatible


def check_types(checked_type, expected_type):
    """check_types checks the type consistency.

    For each of the attribute in checked_type, there is the same attribute
    in expected_type with the same value.
    However, expected_type could contain more attributes that checked_type
    does not contain.
    Args:
            checked_type (BaseType/str/dict): it describes a type from the
              upstream component output
            expected_type (BaseType/str/dict): it describes a type from the
              downstream component input
    """
    if isinstance(checked_type, BaseType):
        checked_type = checked_type.to_dict()
    if isinstance(checked_type, str):
        checked_type = {checked_type: {}}
    if isinstance(expected_type, BaseType):
        expected_type = expected_type.to_dict()
    if isinstance(expected_type, str):
        expected_type = {expected_type: {}}
    return _check_dict_types(checked_type, expected_type)


def _check_valid_type_dict(payload):
    """_check_valid_type_dict checks whether a dict is a correct serialization
    of a type.

    Args: payload(dict)
    """
    if not isinstance(payload, dict) or len(payload) != 1:
        return False
    for type_name in payload:
        if not isinstance(payload[type_name], dict):
            return False
        property_types = (int, str, float, bool)
        property_value_types = (int, str, float, bool, dict)
        for property_name in payload[type_name]:
            if not isinstance(property_name, property_types) or not isinstance(
                    payload[type_name][property_name], property_value_types):
                return False
    return True


def _check_dict_types(checked_type, expected_type):
    """_check_dict_types checks the type consistency.

    Args:
    checked_type (dict): A dict that describes a type from the upstream
      component output
    expected_type (dict): A dict that describes a type from the downstream
      component input
    """
    if not checked_type or not expected_type:
        # If the type is empty, it matches any types
        return True
    checked_type_name, _ = list(checked_type.items())[0]
    expected_type_name, _ = list(expected_type.items())[0]
    if checked_type_name == "" or expected_type_name == "":
        # If the type name is empty, it matches any types
        return True
    if checked_type_name != expected_type_name:
        print("type name " + str(checked_type_name) +
              " is different from expected: " + str(expected_type_name))
        return False
    type_name = checked_type_name
    for type_property in checked_type[type_name]:
        if type_property not in expected_type[type_name]:
            print(type_name + " has a property " + str(type_property) +
                  " that the latter does not.")
            return False
        if checked_type[type_name][type_property] != expected_type[type_name][
                type_property]:
            print(type_name + " has a property " + str(type_property) +
                  " with value: " +
                  str(checked_type[type_name][type_property]) + " and " +
                  str(expected_type[type_name][type_property]))
            return False
    return True
