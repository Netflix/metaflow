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
"""Tests for kfp.v2.components.experimental.pipeline_task."""

import textwrap
import unittest
from typing import Union

from absl.testing import parameterized
from kfp.v2.components.experimental import pipeline_task
from kfp.v2.components.experimental import structures
from kfp.v2.components.experimental import pipeline_channel

V2_YAML = textwrap.dedent("""\
    name: component1
    inputs:
      input1: {type: String}
    outputs:
      output1: {type: Artifact}
    implementation:
      container:
        image: alpine
        commands:
        - sh
        - -c
        - echo "$0" >> "$1"
        arguments:
        - {inputValue: input1}
        - {outputPath: output1}
""")

V2_YAML_IF_PLACEHOLDER = textwrap.dedent("""\
    name: component_if
    inputs:
      optional_input_1: {type: String}
    implementation:
      container:
        image: alpine
        commands:
        - sh
        - -c
        - echo "$0" "$1"
        arguments:
        - ifPresent:
            inputName: optional_input_1
            then:
            - "input: "
            - {inputValue: optional_input_1}
            otherwise:
            - "default: "
            - "Hello world!"
""")

V2_YAML_CONCAT_PLACEHOLDER = textwrap.dedent("""\
    name: component_concat
    inputs:
      input1: {type: String}
      input2: {type: String}
    implementation:
      container:
        image: alpine
        commands:
        - sh
        - -c
        - echo "$0"
        - concat: [{inputValue: input1}, "+", {inputValue: input2}]
    """)


class PipelineTaskTest(parameterized.TestCase):

    def test_create_pipeline_task_valid(self):
        expected_component_spec = structures.ComponentSpec(
            name='component1',
            implementation=structures.Implementation(
                container=structures.ContainerSpec(
                    image='alpine',
                    commands=['sh', '-c', 'echo "$0" >> "$1"'],
                    arguments=[
                        structures.InputValuePlaceholder(input_name='input1'),
                        structures.OutputPathPlaceholder(output_name='output1'),
                    ],
                )),
            inputs={
                'input1': structures.InputSpec(type='String'),
            },
            outputs={
                'output1': structures.OutputSpec(type='Artifact'),
            },
        )
        expected_task_spec = structures.TaskSpec(
            name='component1',
            inputs={'input1': 'value'},
            dependent_tasks=[],
            component_ref='component1',
        )
        expected_container_spec = structures.ContainerSpec(
            image='alpine',
            commands=['sh', '-c', 'echo "$0" >> "$1"'],
            arguments=[
                "{{$.inputs.parameters['input1']}}",
                "{{$.outputs.artifacts['output1'].path}}",
            ],
        )

        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML),
            arguments={'input1': 'value'},
        )
        self.assertEqual(task.task_spec, expected_task_spec)
        self.assertEqual(task.component_spec, expected_component_spec)
        self.assertEqual(task.container_spec, expected_container_spec)

    def test_create_pipeline_task_invalid_missing_required_input(self):
        with self.assertRaisesRegex(ValueError,
                                    'No value provided for input: input1.'):
            task = pipeline_task.PipelineTask(
                component_spec=structures.ComponentSpec
                .load_from_component_yaml(V2_YAML),
                arguments={},
            )

    def test_create_pipeline_task_invalid_wrong_input(self):
        with self.assertRaisesRegex(
                ValueError,
                'Component "component1" got an unexpected input: input0.'):
            task = pipeline_task.PipelineTask(
                component_spec=structures.ComponentSpec
                .load_from_component_yaml(V2_YAML),
                arguments={
                    'input1': 'value',
                    'input0': 'abc',
                },
            )

    @parameterized.parameters(
        {
            'component_yaml':
                V2_YAML_IF_PLACEHOLDER,
            'arguments': {
                'optional_input_1': 'value'
            },
            'expected_container_spec':
                structures.ContainerSpec(
                    image='alpine',
                    commands=['sh', '-c', 'echo "$0" "$1"'],
                    arguments=[
                        'input: ',
                        "{{$.inputs.parameters['optional_input_1']}}",
                    ],
                )
        },
        {
            'component_yaml':
                V2_YAML_IF_PLACEHOLDER,
            'arguments': {},
            'expected_container_spec':
                structures.ContainerSpec(
                    image='alpine',
                    commands=['sh', '-c', 'echo "$0" "$1"'],
                    arguments=[
                        'default: ',
                        'Hello world!',
                    ],
                )
        },
    )
    def test_resolve_if_placeholder(
        self,
        component_yaml: str,
        arguments: dict,
        expected_container_spec: structures.ContainerSpec,
    ):
        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                component_yaml),
            arguments=arguments,
        )
        self.assertEqual(task.container_spec, expected_container_spec)

    def test_resolve_concat_placeholder(self):
        expected_container_spec = structures.ContainerSpec(
            image='alpine',
            commands=[
                'sh',
                '-c',
                'echo "$0"',
                "{{$.inputs.parameters['input1']}}+{{$.inputs.parameters['input2']}}",
            ],
        )

        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML_CONCAT_PLACEHOLDER),
            arguments={
                'input1': '1',
                'input2': '2',
            },
        )
        self.assertEqual(task.container_spec, expected_container_spec)

    def test_set_caching_options(self):
        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML),
            arguments={'input1': 'value'},
        )
        task.set_caching_options(False)
        self.assertEqual(False, task.task_spec.enable_caching)

    @parameterized.parameters(
        {
            'cpu_limit': '123',
            'expected_cpu_number': 123,
        },
        {
            'cpu_limit': '123m',
            'expected_cpu_number': 0.123,
        },
        {
            'cpu_limit': '123.0',
            'expected_cpu_number': 123,
        },
        {
            'cpu_limit': '123.0m',
            'expected_cpu_number': 0.123,
        },
    )
    def test_set_valid_cpu_limit(self, cpu_limit: str,
                                 expected_cpu_number: float):
        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML),
            arguments={'input1': 'value'},
        )
        task.set_cpu_limit(cpu_limit)
        self.assertEqual(expected_cpu_number,
                         task.container_spec.resources.cpu_limit)

    @parameterized.parameters(
        {
            'gpu_limit': '666',
            'expected_gpu_number': 666,
        },)
    def test_set_valid_gpu_limit(self, gpu_limit: str,
                                 expected_gpu_number: int):
        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML),
            arguments={'input1': 'value'},
        )
        task.set_gpu_limit(gpu_limit)
        self.assertEqual(expected_gpu_number,
                         task.container_spec.resources.accelerator_count)

    @parameterized.parameters(
        {
            'memory': '1E',
            'expected_memory_number': 1000000000,
        },
        {
            'memory': '15Ei',
            'expected_memory_number': 17293822569.102703,
        },
        {
            'memory': '2P',
            'expected_memory_number': 2000000,
        },
        {
            'memory': '25Pi',
            'expected_memory_number': 28147497.6710656,
        },
        {
            'memory': '3T',
            'expected_memory_number': 3000,
        },
        {
            'memory': '35Ti',
            'expected_memory_number': 38482.90697216,
        },
        {
            'memory': '4G',
            'expected_memory_number': 4,
        },
        {
            'memory': '45Gi',
            'expected_memory_number': 48.31838208,
        },
        {
            'memory': '5M',
            'expected_memory_number': 0.005,
        },
        {
            'memory': '55Mi',
            'expected_memory_number': 0.05767168,
        },
        {
            'memory': '6K',
            'expected_memory_number': 0.000006,
        },
        {
            'memory': '65Ki',
            'expected_memory_number': 0.00006656,
        },
        {
            'memory': '7000',
            'expected_memory_number': 0.000007,
        },
    )
    def test_set_memory_limit(self, memory: str, expected_memory_number: int):
        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML),
            arguments={'input1': 'value'},
        )
        task.set_memory_limit(memory)
        self.assertEqual(expected_memory_number,
                         task.container_spec.resources.memory_limit)

    def test_add_node_selector_constraint_type_only(self):
        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML),
            arguments={'input1': 'value'},
        )
        task.add_node_selector_constraint('NVIDIA_TESLA_K80')
        self.assertEqual(
            structures.ResourceSpec(
                accelerator_type='NVIDIA_TESLA_K80', accelerator_count=1),
            task.container_spec.resources)

    def test_add_node_selector_constraint_accelerator_count(self):
        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML),
            arguments={'input1': 'value'},
        )
        task.set_gpu_limit('5').add_node_selector_constraint('TPU_V3')
        self.assertEqual(
            structures.ResourceSpec(
                accelerator_type='TPU_V3', accelerator_count=5),
            task.container_spec.resources)

    def test_set_display_name(self):
        task = pipeline_task.PipelineTask(
            component_spec=structures.ComponentSpec.load_from_component_yaml(
                V2_YAML),
            arguments={'input1': 'value'},
        )
        task.set_display_name('test_name')
        self.assertEqual('test_name', task.task_spec.display_name)


if __name__ == '__main__':
    unittest.main()
