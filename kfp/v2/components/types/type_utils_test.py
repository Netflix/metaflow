# Copyright 2020 The Kubeflow Authors
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
import unittest
from typing import Any, Dict, List, Union

from absl.testing import parameterized
from kfp.components import structures
from kfp.pipeline_spec import pipeline_spec_pb2 as pb
from kfp.v2.components.types import artifact_types, type_utils
from kfp.v2.components.types.type_utils import InconsistentTypeException

_PARAMETER_TYPES = [
    'String',
    'str',
    'Integer',
    'int',
    'Float',
    'Double',
    'bool',
    'Boolean',
    'Dict',
    'List',
    'JsonObject',
    'JsonArray',
    {
        'JsonObject': {
            'data_type': 'proto:tfx.components.trainer.TrainArgs'
        }
    },
]
_KNOWN_ARTIFACT_TYPES = ['Model', 'Dataset', 'Schema', 'Metrics']
_UNKNOWN_ARTIFACT_TYPES = [None, 'Arbtrary Model', 'dummy']


class _ArbitraryClass:
    pass


class _VertexDummy(artifact_types.Artifact):
    TYPE_NAME = 'google.VertexDummy'
    VERSION = '0.0.2'

    def __init__(self):
        super().__init__(uri='uri', name='name', metadata={'dummy': '123'})


class TypeUtilsTest(parameterized.TestCase):

    def test_is_parameter_type(self):
        for type_name in _PARAMETER_TYPES:
            self.assertTrue(type_utils.is_parameter_type(type_name))
        for type_name in _KNOWN_ARTIFACT_TYPES + _UNKNOWN_ARTIFACT_TYPES:
            self.assertFalse(type_utils.is_parameter_type(type_name))

    @parameterized.parameters(
        {
            'artifact_class_or_type_name':
                'Model',
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Model', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                artifact_types.Model,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Model', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                'Dataset',
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Dataset', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                artifact_types.Dataset,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Dataset', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                'Metrics',
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Metrics', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                artifact_types.Metrics,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Metrics', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                'ClassificationMetrics',
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.ClassificationMetrics',
                    schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                artifact_types.ClassificationMetrics,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.ClassificationMetrics',
                    schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                'SlicedClassificationMetrics',
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.SlicedClassificationMetrics',
                    schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                artifact_types.SlicedClassificationMetrics,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.SlicedClassificationMetrics',
                    schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                'arbitrary name',
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Artifact', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                _ArbitraryClass,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Artifact', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                artifact_types.HTML,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.HTML', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                artifact_types.Markdown,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Markdown', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                'some-google-type',
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='system.Artifact', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                'google.VertexModel',
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='google.VertexModel', schema_version='0.0.1')
        },
        {
            'artifact_class_or_type_name':
                _VertexDummy,
            'expected_result':
                pb.ArtifactTypeSchema(
                    schema_title='google.VertexDummy', schema_version='0.0.2')
        },
    )
    def test_get_artifact_type_schema(self, artifact_class_or_type_name,
                                      expected_result):
        self.assertEqual(
            expected_result,
            type_utils.get_artifact_type_schema(artifact_class_or_type_name))

    @parameterized.parameters(
        {
            'given_type': 'Int',
            'expected_type': pb.PrimitiveType.INT,
        },
        {
            'given_type': 'Integer',
            'expected_type': pb.PrimitiveType.INT,
        },
        {
            'given_type': int,
            'expected_type': pb.PrimitiveType.INT,
        },
        {
            'given_type': 'Double',
            'expected_type': pb.PrimitiveType.DOUBLE,
        },
        {
            'given_type': 'Float',
            'expected_type': pb.PrimitiveType.DOUBLE,
        },
        {
            'given_type': float,
            'expected_type': pb.PrimitiveType.DOUBLE,
        },
        {
            'given_type': 'String',
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': 'Text',
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': str,
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': 'Boolean',
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': bool,
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': 'Dict',
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': dict,
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': 'List',
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': list,
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': Dict[str, int],
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': List[Any],
            'expected_type': pb.PrimitiveType.STRING,
        },
        {
            'given_type': {
                'JsonObject': {
                    'data_type': 'proto:tfx.components.trainer.TrainArgs'
                }
            },
            'expected_type': pb.PrimitiveType.STRING,
        },
    )
    def test_get_parameter_type(self, given_type, expected_type):
        self.assertEqual(expected_type,
                         type_utils.get_parameter_type(given_type))

        # Test get parameter by Python type.
        self.assertEqual(pb.PrimitiveType.INT,
                         type_utils.get_parameter_type(int))

    def test_get_parameter_type_invalid(self):
        with self.assertRaises(AttributeError):
            type_utils.get_parameter_type_schema(None)

    def test_get_input_artifact_type_schema(self):
        input_specs = [
            structures.InputSpec(name='input1', type='String'),
            structures.InputSpec(name='input2', type='Model'),
            structures.InputSpec(name='input3', type=None),
        ]
        # input not found.
        with self.assertRaises(AssertionError) as cm:
            type_utils.get_input_artifact_type_schema('input0', input_specs)
            self.assertEqual('Input not found.', str(cm))

        # input found, but it doesn't map to an artifact type.
        with self.assertRaises(AssertionError) as cm:
            type_utils.get_input_artifact_type_schema('input1', input_specs)
            self.assertEqual('Input is not an artifact type.', str(cm))

        # input found, and a matching artifact type schema returned.
        self.assertEqual(
            'system.Model',
            type_utils.get_input_artifact_type_schema('input2',
                                                      input_specs).schema_title)

        # input found, and the default artifact type schema returned.
        self.assertEqual(
            'system.Artifact',
            type_utils.get_input_artifact_type_schema('input3',
                                                      input_specs).schema_title)

    def test_get_parameter_type_field_name(self):
        self.assertEqual('string_value',
                         type_utils.get_parameter_type_field_name('String'))
        self.assertEqual('int_value',
                         type_utils.get_parameter_type_field_name('Integer'))
        self.assertEqual('double_value',
                         type_utils.get_parameter_type_field_name('Float'))

    @parameterized.parameters(
        {
            'given_type': 'String',
            'expected_type': 'String',
            'is_compatible': True,
        },
        {
            'given_type': 'String',
            'expected_type': 'Integer',
            'is_compatible': False,
        },
        {
            'given_type': {
                'type_a': {
                    'property': 'property_b',
                }
            },
            'expected_type': {
                'type_a': {
                    'property': 'property_b',
                }
            },
            'is_compatible': True,
        },
        {
            'given_type': {
                'type_a': {
                    'property': 'property_b',
                }
            },
            'expected_type': {
                'type_a': {
                    'property': 'property_c',
                }
            },
            'is_compatible': False,
        },
        {
            'given_type': 'Artifact',
            'expected_type': 'Model',
            'is_compatible': True,
        },
        {
            'given_type': 'Metrics',
            'expected_type': 'Artifact',
            'is_compatible': True,
        },
    )
    def test_verify_type_compatibility(
        self,
        given_type: Union[str, dict],
        expected_type: Union[str, dict],
        is_compatible: bool,
    ):
        if is_compatible:
            self.assertTrue(
                type_utils.verify_type_compatibility(
                    given_type=given_type,
                    expected_type=expected_type,
                    error_message_prefix='',
                ))
        else:
            with self.assertRaises(InconsistentTypeException):
                type_utils.verify_type_compatibility(
                    given_type=given_type,
                    expected_type=expected_type,
                    error_message_prefix='',
                )


if __name__ == '__main__':
    unittest.main()
