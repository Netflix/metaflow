# Copyright 2021 The Kubeflow Authors
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
"""Helper utils used in artifact and ontology_artifact classes."""

from typing import Any, Dict, Tuple

import enum
import jsonschema
import os
import yaml


class SchemaFieldType(enum.Enum):
    """Supported Schema field types."""
    NUMBER = 'number'
    INTEGER = 'integer'
    STRING = 'string'
    BOOL = 'bool'
    OBJECT = 'object'
    ARRAY = 'array'


def parse_schema(yaml_schema: str) -> Tuple[str, Dict[str, SchemaFieldType]]:
    """Parses yaml schema.

    Ensures that schema is well-formed and returns dictionary of properties and
    its type for type-checking.

    Args:
      yaml_schema: Yaml schema to be parsed.

    Returns:
      str: Title set in the schema.
      Dict: Property name to SchemaFieldType enum.

    Raises:
      ValueError if title field is not set in schema or an
        unsupported(i.e. not defined in SchemaFieldType)
        type is specified for the field.
    """

    schema = yaml.full_load(yaml_schema)
    if 'title' not in schema.keys():
        raise ValueError('Invalid _schema, title must be set. \
      Got: {}'.format(yaml_schema))

    title = schema['title']
    properties = {}
    if 'properties' in schema.keys():
        schema_properties = schema['properties'] or {}
        for property_name, property_def in schema_properties.items():
            try:
                properties[property_name] = SchemaFieldType(
                    property_def['type'])
            except ValueError:
                raise ValueError('Unsupported type:{} specified for field: {} \
          in schema'.format(property_def['type'], property_name))

    return title, properties


def verify_schema_instance(schema: str, instance: Dict[str, Any]):
    """Verifies instnace is well-formed against the schema.

    Args:
      schema: Schema to use for verification.
      instance: Object represented as Dict to be verified.

    Raises:
      RuntimeError if schema is not well-formed or instance is invalid against
       the schema.
    """

    if len(instance) == 0:
        return

    try:
        jsonschema.validate(instance=instance, schema=yaml.full_load(schema))
    except jsonschema.exceptions.SchemaError:
        raise RuntimeError('Invalid schema schema: {} used for \
        verification'.format(schema))
    except jsonschema.exceptions.ValidationError:
        raise RuntimeError('Invalid values set: {} in object for schema: \
      {}'.format(instance, schema))


def read_schema_file(schema_file: str) -> str:
    """Reads yamls schema from type_scheams folder.

    Args:
      schema_file: Name of the file to read schema from.

    Returns:
      Read schema from the schema file.
    """
    schema_file_path = os.path.join(
        os.path.dirname(__file__), 'type_schemas', schema_file)

    with open(schema_file_path) as schema_file:
        return schema_file.read()
