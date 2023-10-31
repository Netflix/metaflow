# Copyright 2021 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for kfp.v2.google.client.runtime_config_builder."""

import unittest

import frozendict
from kfp.v2.google.client import runtime_config_builder


class RuntimeConfigBuilderTest(unittest.TestCase):

    SAMPLE_JOB_SPEC = frozendict.frozendict({
        'pipelineSpec': {
            'root': {
                'inputDefinitions': {
                    'parameters': {
                        'string_param': {
                            'type': 'STRING'
                        },
                        'int_param': {
                            'type': 'INT'
                        },
                        'float_param': {
                            'type': 'DOUBLE'
                        },
                        'new_param': {
                            'type': 'STRING'
                        },
                        'bool_param': {
                            'type': 'STRING'
                        },
                        'dict_param': {
                            'type': 'STRING'
                        },
                        'list_param': {
                            'type': 'STRING'
                        },
                    }
                }
            }
        },
        'runtimeConfig': {
            'gcsOutputDirectory': 'path/to/my/root',
            'parameters': {
                'string_param': {
                    'stringValue': 'test-string'
                },
                'int_param': {
                    'intValue': 42
                },
                'float_param': {
                    'doubleValue': 3.14
                },
            }
        }
    })

    def testBuildRuntimeConfigFromIndividualValues(self):
        my_builder = runtime_config_builder.RuntimeConfigBuilder(
            pipeline_root='path/to/my/root',
            parameter_types={
                'string_param': 'STRING',
                'int_param': 'INT',
                'float_param': 'DOUBLE'
            },
            parameter_values={
                'string_param': 'test-string',
                'int_param': 42,
                'float_param': 3.14
            })
        actual_runtime_config = my_builder.build()

        expected_runtime_config = self.SAMPLE_JOB_SPEC['runtimeConfig']
        self.assertEqual(expected_runtime_config, actual_runtime_config)

    def testBuildRuntimeConfigFromJobSpecJson(self):
        my_builder = (
            runtime_config_builder.RuntimeConfigBuilder.from_job_spec_json(
                self.SAMPLE_JOB_SPEC))
        actual_runtime_config = my_builder.build()

        expected_runtime_config = self.SAMPLE_JOB_SPEC['runtimeConfig']
        self.assertEqual(expected_runtime_config, actual_runtime_config)

    def testBuildRuntimeConfigWithNoopUpdates(self):
        my_builder = (
            runtime_config_builder.RuntimeConfigBuilder.from_job_spec_json(
                self.SAMPLE_JOB_SPEC))
        my_builder.update_pipeline_root(None)
        my_builder.update_runtime_parameters(None)
        actual_runtime_config = my_builder.build()

        expected_runtime_config = self.SAMPLE_JOB_SPEC['runtimeConfig']
        self.assertEqual(expected_runtime_config, actual_runtime_config)

    def testBuildRuntimeConfigWithMergeUpdates(self):
        my_builder = (
            runtime_config_builder.RuntimeConfigBuilder.from_job_spec_json(
                self.SAMPLE_JOB_SPEC))
        my_builder.update_pipeline_root('path/to/my/new/root')
        my_builder.update_runtime_parameters({
            'int_param': 888,
            'new_param': 'new-string',
            'dict_param': {
                'a': 1
            },
            'list_param': [1, 2, 3],
            'bool_param': True,
        })
        actual_runtime_config = my_builder.build()

        expected_runtime_config = {
            'gcsOutputDirectory': 'path/to/my/new/root',
            'parameters': {
                'string_param': {
                    'stringValue': 'test-string'
                },
                'int_param': {
                    'intValue': 888
                },
                'float_param': {
                    'doubleValue': 3.14
                },
                'new_param': {
                    'stringValue': 'new-string'
                },
                'dict_param': {
                    'stringValue': '{"a": 1}'
                },
                'list_param': {
                    'stringValue': '[1, 2, 3]'
                },
                'bool_param': {
                    'stringValue': 'true'
                },
            }
        }
        self.assertEqual(expected_runtime_config, actual_runtime_config)

    def testBuildRuntimeConfigParameterNotFound(self):
        my_builder = (
            runtime_config_builder.RuntimeConfigBuilder.from_job_spec_json(
                self.SAMPLE_JOB_SPEC))
        my_builder.update_pipeline_root('path/to/my/new/root')
        my_builder.update_runtime_parameters({'no_such_param': 'new-string'})
        with self.assertRaisesRegex(
                ValueError,
                'The pipeline parameter no_such_param is not found'):
            my_builder.build()

    def testParseRuntimeParameters(self):
        expected_runtime_parameters = {
            'string_param': 'test-string',
            'int_param': 42,
            'float_param': 3.14,
        }
        actual_runtime_parameters = (
            runtime_config_builder._parse_runtime_parameters(
                self.SAMPLE_JOB_SPEC['runtimeConfig']))
        self.assertEqual(expected_runtime_parameters, actual_runtime_parameters)


if __name__ == '__main__':
    unittest.main()
