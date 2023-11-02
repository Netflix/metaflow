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
"""Tests for kfp.dsl.component_spec."""

from absl.testing import parameterized

from kfp.components import _structures as structures
from kfp.dsl import _pipeline_param
from kfp.dsl import component_spec as dsl_component_spec
from kfp.pipeline_spec import pipeline_spec_pb2

from google.protobuf import json_format


class ComponentSpecTest(parameterized.TestCase):

    TEST_PIPELINE_PARAMS = [
        _pipeline_param.PipelineParam(
            name='output1', param_type='Dataset', op_name='op-1'),
        _pipeline_param.PipelineParam(
            name='output2', param_type='Integer', op_name='op-2'),
        _pipeline_param.PipelineParam(
            name='output3', param_type='Model', op_name='op-3'),
        _pipeline_param.PipelineParam(
            name='output4', param_type='Double', op_name='op-4'),
        _pipeline_param.PipelineParam(
            name='arg_input', param_type='String', op_name=None),
    ]

    def setUp(self):
        self.maxDiff = None

    def test_build_component_spec_from_structure(self):
        structure_component_spec = structures.ComponentSpec(
            name='component1',
            description='component1 desc',
            inputs=[
                structures.InputSpec(
                    name='input1', description='input1 desc', type='Dataset'),
                structures.InputSpec(
                    name='input2', description='input2 desc', type='String'),
                structures.InputSpec(
                    name='input3', description='input3 desc', type='Integer'),
                structures.InputSpec(
                    name='input4', description='optional inputs',
                    optional=True),
            ],
            outputs=[
                structures.OutputSpec(
                    name='output1', description='output1 desc', type='Model')
            ])
        expected_dict = {
            'inputDefinitions': {
                'artifacts': {
                    'input1': {
                        'artifactType': {
                            'schemaTitle': 'system.Dataset',
                            'schemaVersion': '0.0.1'
                        }
                    }
                },
                'parameters': {
                    'input2': {
                        'type': 'STRING'
                    },
                    'input3': {
                        'type': 'INT'
                    }
                }
            },
            'outputDefinitions': {
                'artifacts': {
                    'output1': {
                        'artifactType': {
                            'schemaTitle': 'system.Model',
                            'schemaVersion': '0.0.1'
                        }
                    }
                }
            },
            'executorLabel': 'exec-component1'
        }
        expected_spec = pipeline_spec_pb2.ComponentSpec()
        json_format.ParseDict(expected_dict, expected_spec)

        component_spec = (
            dsl_component_spec.build_component_spec_from_structure(
                component_spec=structure_component_spec,
                executor_label='exec-component1',
                actual_inputs=['input1', 'input2', 'input3'],
            ))

        self.assertEqual(expected_spec, component_spec)

    @parameterized.parameters(
        {
            'is_root_component': True,
            'expected_result': {
                'inputDefinitions': {
                    'artifacts': {
                        'input1': {
                            'artifactType': {
                                'schemaTitle': 'system.Dataset',
                                'schemaVersion': '0.0.1'
                            }
                        }
                    },
                    'parameters': {
                        'input2': {
                            'type': 'INT'
                        },
                        'input3': {
                            'type': 'STRING'
                        },
                        'input4': {
                            'type': 'DOUBLE'
                        }
                    }
                }
            }
        },
        {
            'is_root_component': False,
            'expected_result': {
                'inputDefinitions': {
                    'artifacts': {
                        'pipelineparam--input1': {
                            'artifactType': {
                                'schemaTitle': 'system.Dataset',
                                'schemaVersion': '0.0.1'
                            }
                        }
                    },
                    'parameters': {
                        'pipelineparam--input2': {
                            'type': 'INT'
                        },
                        'pipelineparam--input3': {
                            'type': 'STRING'
                        },
                        'pipelineparam--input4': {
                            'type': 'DOUBLE'
                        }
                    }
                }
            }
        },
    )
    def test_build_component_inputs_spec(self, is_root_component,
                                         expected_result):
        pipeline_params = [
            _pipeline_param.PipelineParam(name='input1', param_type='Dataset'),
            _pipeline_param.PipelineParam(name='input2', param_type='Integer'),
            _pipeline_param.PipelineParam(name='input3', param_type='String'),
            _pipeline_param.PipelineParam(name='input4', param_type='Float'),
        ]
        expected_spec = pipeline_spec_pb2.ComponentSpec()
        json_format.ParseDict(expected_result, expected_spec)

        component_spec = pipeline_spec_pb2.ComponentSpec()
        dsl_component_spec.build_component_inputs_spec(component_spec,
                                                       pipeline_params,
                                                       is_root_component)

        self.assertEqual(expected_spec, component_spec)

    def test_build_component_outputs_spec(self):
        pipeline_params = [
            _pipeline_param.PipelineParam(name='output1', param_type='Dataset'),
            _pipeline_param.PipelineParam(name='output2', param_type='Integer'),
            _pipeline_param.PipelineParam(name='output3', param_type='String'),
            _pipeline_param.PipelineParam(name='output4', param_type='Float'),
        ]
        expected_dict = {
            'outputDefinitions': {
                'artifacts': {
                    'output1': {
                        'artifactType': {
                            'schemaTitle': 'system.Dataset',
                            'schemaVersion': '0.0.1'
                        }
                    }
                },
                'parameters': {
                    'output2': {
                        'type': 'INT'
                    },
                    'output3': {
                        'type': 'STRING'
                    },
                    'output4': {
                        'type': 'DOUBLE'
                    }
                }
            }
        }
        expected_spec = pipeline_spec_pb2.ComponentSpec()
        json_format.ParseDict(expected_dict, expected_spec)

        component_spec = pipeline_spec_pb2.ComponentSpec()
        dsl_component_spec.build_component_outputs_spec(component_spec,
                                                        pipeline_params)

        self.assertEqual(expected_spec, component_spec)

    @parameterized.parameters(
        {
            'is_parent_component_root': True,
            'expected_result': {
                'inputs': {
                    'artifacts': {
                        'pipelineparam--op-1-output1': {
                            'taskOutputArtifact': {
                                'producerTask': 'op-1',
                                'outputArtifactKey': 'output1'
                            }
                        },
                        'pipelineparam--op-3-output3': {
                            'componentInputArtifact': 'op-3-output3'
                        }
                    },
                    'parameters': {
                        'pipelineparam--op-2-output2': {
                            'taskOutputParameter': {
                                'producerTask': 'op-2',
                                'outputParameterKey': 'output2'
                            }
                        },
                        'pipelineparam--op-4-output4': {
                            'componentInputParameter': 'op-4-output4'
                        },
                        'pipelineparam--arg_input': {
                            'componentInputParameter': 'arg_input'
                        }
                    }
                }
            }
        },
        {
            'is_parent_component_root': False,
            'expected_result': {
                'inputs': {
                    'artifacts': {
                        'pipelineparam--op-1-output1': {
                            'taskOutputArtifact': {
                                'producerTask': 'op-1',
                                'outputArtifactKey': 'output1'
                            }
                        },
                        'pipelineparam--op-3-output3': {
                            'componentInputArtifact':
                                'pipelineparam--op-3-output3'
                        }
                    },
                    'parameters': {
                        'pipelineparam--op-2-output2': {
                            'taskOutputParameter': {
                                'producerTask': 'op-2',
                                'outputParameterKey': 'output2'
                            }
                        },
                        'pipelineparam--op-4-output4': {
                            'componentInputParameter':
                                'pipelineparam--op-4-output4'
                        },
                        'pipelineparam--arg_input': {
                            'componentInputParameter':
                                'pipelineparam--arg_input'
                        }
                    }
                }
            }
        },
    )
    def test_build_task_inputs_spec(self, is_parent_component_root,
                                    expected_result):
        pipeline_params = self.TEST_PIPELINE_PARAMS
        tasks_in_current_dag = ['op-1', 'op-2']
        expected_spec = pipeline_spec_pb2.PipelineTaskSpec()
        json_format.ParseDict(expected_result, expected_spec)

        task_spec = pipeline_spec_pb2.PipelineTaskSpec()
        dsl_component_spec.build_task_inputs_spec(task_spec, pipeline_params,
                                                  tasks_in_current_dag,
                                                  is_parent_component_root)

        self.assertEqual(expected_spec, task_spec)

    @parameterized.parameters(
        {
            'original_task_spec': {},
            'parent_component_inputs': {},
            'tasks_in_current_dag': [],
            'input_parameters_in_current_dag': [],
            'input_artifacts_in_current_dag': [],
            'expected_result': {},
        },
        { # Depending on tasks & inputs within the current DAG.
            'original_task_spec': {
                'inputs': {
                    'artifacts': {
                        'pipelineparam--op-1-output1': {
                            'taskOutputArtifact': {
                                'producerTask': 'op-1',
                                'outputArtifactKey': 'output1'
                            }
                        },
                        'artifact1': {
                          'componentInputArtifact': 'artifact1'
                        },
                    },
                    'parameters': {
                        'pipelineparam--op-2-output2': {
                            'taskOutputParameter': {
                                'producerTask': 'op-2',
                                'outputParameterKey': 'output2'
                            }
                        },
                        'param1': {
                          'componentInputParameter': 'param1'
                        },
                    }
                }
            },
            'parent_component_inputs': {
              'artifacts': {
                'artifact1': {
                  'artifactType': {
                    'instanceSchema': 'dummy_schema'
                  }
                },
              },
              'parameters': {
                'param1': {
                  'type': 'STRING'
                },
              }
            },
            'tasks_in_current_dag': ['op-1', 'op-2'],
            'input_parameters_in_current_dag': ['param1'],
            'input_artifacts_in_current_dag': ['artifact1'],
            'expected_result': {
                'inputs': {
                    'artifacts': {
                        'pipelineparam--op-1-output1': {
                            'taskOutputArtifact': {
                                'producerTask': 'op-1',
                                'outputArtifactKey': 'output1'
                            }
                        },
                        'artifact1': {
                          'componentInputArtifact': 'artifact1'
                        },
                    },
                    'parameters': {
                        'pipelineparam--op-2-output2': {
                            'taskOutputParameter': {
                                'producerTask': 'op-2',
                                'outputParameterKey': 'output2'
                            }
                        },
                        'param1': {
                          'componentInputParameter': 'param1'
                        },
                    }
                }
            },
        },
        { # Depending on tasks and inputs not available in the current DAG.
            'original_task_spec': {
                'inputs': {
                    'artifacts': {
                        'pipelineparam--op-1-output1': {
                            'taskOutputArtifact': {
                                'producerTask': 'op-1',
                                'outputArtifactKey': 'output1'
                            }
                        },
                        'artifact1': {
                          'componentInputArtifact': 'artifact1'
                        },
                    },
                    'parameters': {
                        'pipelineparam--op-2-output2': {
                            'taskOutputParameter': {
                                'producerTask': 'op-2',
                                'outputParameterKey': 'output2'
                            }
                        },
                        'param1': {
                          'componentInputParameter': 'param1'
                        },
                    }
                }
            },
            'parent_component_inputs': {
                'artifacts': {
                    'pipelineparam--op-1-output1': {
                        'artifactType': {
                            'instanceSchema': 'dummy_schema'
                        }
                    },
                    'pipelineparam--artifact1': {
                      'artifactType': {
                        'instanceSchema': 'dummy_schema'
                      }
                    },
                },
                'parameters': {
                  'pipelineparam--op-2-output2' : {
                    'type': 'INT'
                  },
                  'pipelineparam--param1': {
                    'type': 'STRING'
                  },
                }
            },
            'tasks_in_current_dag': ['op-3'],
            'input_parameters_in_current_dag': ['pipelineparam--op-2-output2', 'pipelineparam--param1'],
            'input_artifacts_in_current_dag': ['pipelineparam--op-1-output1', 'pipelineparam--artifact1'],
            'expected_result': {
                'inputs': {
                    'artifacts': {
                        'pipelineparam--op-1-output1': {
                            'componentInputArtifact':
                                'pipelineparam--op-1-output1'
                        },
                        'artifact1': {
                          'componentInputArtifact': 'pipelineparam--artifact1'
                        },
                    },
                    'parameters': {
                        'pipelineparam--op-2-output2': {
                            'componentInputParameter':
                                'pipelineparam--op-2-output2'
                        },
                        'param1': {
                          'componentInputParameter': 'pipelineparam--param1'
                        },
                    }
                }
            },
        },
    )
    def test_update_task_inputs_spec(self, original_task_spec,
                                     parent_component_inputs,
                                     tasks_in_current_dag,
                                     input_parameters_in_current_dag,
                                     input_artifacts_in_current_dag,
                                     expected_result):
        pipeline_params = self.TEST_PIPELINE_PARAMS

        expected_spec = pipeline_spec_pb2.PipelineTaskSpec()
        json_format.ParseDict(expected_result, expected_spec)

        task_spec = pipeline_spec_pb2.PipelineTaskSpec()
        json_format.ParseDict(original_task_spec, task_spec)
        parent_component_inputs_spec = pipeline_spec_pb2.ComponentInputsSpec()
        json_format.ParseDict(parent_component_inputs,
                              parent_component_inputs_spec)
        dsl_component_spec.update_task_inputs_spec(
            task_spec, parent_component_inputs_spec, pipeline_params,
            tasks_in_current_dag, input_parameters_in_current_dag,
            input_artifacts_in_current_dag)

        self.assertEqual(expected_spec, task_spec)

    def test_pop_input_from_component_spec(self):
        component_spec = pipeline_spec_pb2.ComponentSpec(
            executor_label='exec-component1')

        component_spec.input_definitions.artifacts[
            'input1'].artifact_type.schema_title = 'system.Dataset'
        component_spec.input_definitions.parameters[
            'input2'].type = pipeline_spec_pb2.PrimitiveType.STRING
        component_spec.input_definitions.parameters[
            'input3'].type = pipeline_spec_pb2.PrimitiveType.DOUBLE

        # pop an artifact, and there're other inputs left
        dsl_component_spec.pop_input_from_component_spec(
            component_spec, 'input1')
        expected_dict = {
            'inputDefinitions': {
                'parameters': {
                    'input2': {
                        'type': 'STRING'
                    },
                    'input3': {
                        'type': 'DOUBLE'
                    }
                }
            },
            'executorLabel': 'exec-component1'
        }
        expected_spec = pipeline_spec_pb2.ComponentSpec()
        json_format.ParseDict(expected_dict, expected_spec)
        self.assertEqual(expected_spec, component_spec)

        # pop an parameter, and there're other inputs left
        dsl_component_spec.pop_input_from_component_spec(
            component_spec, 'input2')
        expected_dict = {
            'inputDefinitions': {
                'parameters': {
                    'input3': {
                        'type': 'DOUBLE'
                    }
                }
            },
            'executorLabel': 'exec-component1'
        }
        expected_spec = pipeline_spec_pb2.ComponentSpec()
        json_format.ParseDict(expected_dict, expected_spec)
        self.assertEqual(expected_spec, component_spec)

        # pop the last input, expect no inputDefinitions
        dsl_component_spec.pop_input_from_component_spec(
            component_spec, 'input3')
        expected_dict = {'executorLabel': 'exec-component1'}
        expected_spec = pipeline_spec_pb2.ComponentSpec()
        json_format.ParseDict(expected_dict, expected_spec)
        self.assertEqual(expected_spec, component_spec)

        # pop an input that doesn't exist, expect no-op.
        dsl_component_spec.pop_input_from_component_spec(
            component_spec, 'input4')
        self.assertEqual(expected_spec, component_spec)

    def test_pop_input_from_task_spec(self):
        task_spec = pipeline_spec_pb2.PipelineTaskSpec()
        task_spec.component_ref.name = 'comp-component1'
        task_spec.inputs.artifacts[
            'input1'].task_output_artifact.producer_task = 'op-1'
        task_spec.inputs.artifacts[
            'input1'].task_output_artifact.output_artifact_key = 'output1'
        task_spec.inputs.parameters[
            'input2'].task_output_parameter.producer_task = 'op-2'
        task_spec.inputs.parameters[
            'input2'].task_output_parameter.output_parameter_key = 'output2'
        task_spec.inputs.parameters[
            'input3'].component_input_parameter = 'op3-output3'

        # pop an parameter, and there're other inputs left
        dsl_component_spec.pop_input_from_task_spec(task_spec, 'input3')
        expected_dict = {
            'inputs': {
                'artifacts': {
                    'input1': {
                        'taskOutputArtifact': {
                            'producerTask': 'op-1',
                            'outputArtifactKey': 'output1'
                        }
                    }
                },
                'parameters': {
                    'input2': {
                        'taskOutputParameter': {
                            'producerTask': 'op-2',
                            'outputParameterKey': 'output2'
                        }
                    }
                }
            },
            'component_ref': {
                'name': 'comp-component1'
            }
        }
        expected_spec = pipeline_spec_pb2.PipelineTaskSpec()
        json_format.ParseDict(expected_dict, expected_spec)
        self.assertEqual(expected_spec, task_spec)

        # pop an artifact, and there're other inputs left
        dsl_component_spec.pop_input_from_task_spec(task_spec, 'input1')
        expected_dict = {
            'inputs': {
                'parameters': {
                    'input2': {
                        'taskOutputParameter': {
                            'producerTask': 'op-2',
                            'outputParameterKey': 'output2'
                        }
                    }
                }
            },
            'component_ref': {
                'name': 'comp-component1'
            }
        }
        expected_spec = pipeline_spec_pb2.PipelineTaskSpec()
        json_format.ParseDict(expected_dict, expected_spec)
        self.assertEqual(expected_spec, task_spec)

        # pop the last input, expect no inputDefinitions
        dsl_component_spec.pop_input_from_task_spec(task_spec, 'input2')
        expected_dict = {'component_ref': {'name': 'comp-component1'}}
        expected_spec = pipeline_spec_pb2.PipelineTaskSpec()
        json_format.ParseDict(expected_dict, expected_spec)
        self.assertEqual(expected_spec, task_spec)

        # pop an input that doesn't exist, expect no-op.
        dsl_component_spec.pop_input_from_task_spec(task_spec, 'input4')
        self.assertEqual(expected_spec, task_spec)

    def test_additional_input_name_for_pipelineparam(self):
        self.assertEqual(
            'pipelineparam--op1-param1',
            dsl_component_spec.additional_input_name_for_pipelineparam(
                _pipeline_param.PipelineParam(name='param1', op_name='op1')))
        self.assertEqual(
            'pipelineparam--param2',
            dsl_component_spec.additional_input_name_for_pipelineparam(
                _pipeline_param.PipelineParam(name='param2')))
        self.assertEqual(
            'pipelineparam--param3',
            dsl_component_spec.additional_input_name_for_pipelineparam(
                'param3'))


if __name__ == '__main__':
    unittest.main()
