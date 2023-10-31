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
"""Tests for kfp.v2.components.experimental.structures."""

import textwrap
import unittest
from unittest import mock

import pydantic
from absl.testing import parameterized
from kfp.v2.components.experimental import structures

V1_YAML_IF_PLACEHOLDER = textwrap.dedent("""\
    name: component_if
    inputs:
    - {name: optional_input_1, type: String, optional: true}
    implementation:
      container:
        image: alpine
        args:
        - if:
            cond:
              isPresent: optional_input_1
            then:
              - --arg1
              - {inputValue: optional_input_1}
            else:
              - --arg2
              - default
    """)

V2_YAML_IF_PLACEHOLDER = textwrap.dedent("""\
    name: component_if
    inputs:
      optional_input_1: {type: String}
    implementation:
      container:
        image: alpine
        arguments:
        - ifPresent:
            inputName: optional_input_1
            then:
            - --arg1
            - {inputValue: optional_input_1}
            otherwise: [--arg2, default]
    """)

V2_COMPONENT_SPEC_IF_PLACEHOLDER = structures.ComponentSpec(
    name='component_if',
    implementation=structures.Implementation(
        container=structures.ContainerSpec(
            image='alpine',
            arguments=[
                structures.IfPresentPlaceholder(
                    if_structure=structures.IfPresentPlaceholderStructure(
                        input_name='optional_input_1',
                        then=[
                            '--arg1',
                            structures.InputValuePlaceholder(
                                input_name='optional_input_1'),
                        ],
                        otherwise=[
                            '--arg2',
                            'default',
                        ]))
            ])),
    inputs={'optional_input_1': structures.InputSpec(type='String')},
)

V1_YAML_CONCAT_PLACEHOLDER = textwrap.dedent("""\
    name: component_concat
    inputs:
    - {name: input_prefix, type: String}
    implementation:
      container:
        image: alpine
        args:
        - concat: ['--arg1', {inputValue: input_prefix}]
    """)

V2_YAML_CONCAT_PLACEHOLDER = textwrap.dedent("""\
    name: component_concat
    inputs:
      input_prefix: {type: String}
    implementation:
      container:
        image: alpine
        arguments:
        - concat:
          - --arg1
          - {inputValue: input_prefix}
    """)

V2_COMPONENT_SPEC_CONCAT_PLACEHOLDER = structures.ComponentSpec(
    name='component_concat',
    implementation=structures.Implementation(
        container=structures.ContainerSpec(
            image='alpine',
            arguments=[
                structures.ConcatPlaceholder(concat=[
                    '--arg1',
                    structures.InputValuePlaceholder(input_name='input_prefix'),
                ])
            ])),
    inputs={'input_prefix': structures.InputSpec(type='String')},
)

V2_YAML_NESTED_PLACEHOLDER = textwrap.dedent("""\
    name: component_nested
    inputs:
      input_prefix: {type: String}
    implementation:
      container:
        image: alpine
        arguments:
        - concat:
          - --arg1
          - ifPresent:
              inputName: input_prefix
              then:
              - --arg1
              - {inputValue: input_prefix}
              otherwise:
              - --arg2
              - default
              - concat:
                - --arg1
                - {inputValue: input_prefix}
    """)

V2_COMPONENT_SPEC_NESTED_PLACEHOLDER = structures.ComponentSpec(
    name='component_nested',
    implementation=structures.Implementation(
        container=structures.ContainerSpec(
            image='alpine',
            arguments=[
                structures.ConcatPlaceholder(concat=[
                    '--arg1',
                    structures.IfPresentPlaceholder(
                        if_structure=structures.IfPresentPlaceholderStructure(
                            input_name='input_prefix',
                            then=[
                                '--arg1',
                                structures.InputValuePlaceholder(
                                    input_name='input_prefix'),
                            ],
                            otherwise=[
                                '--arg2',
                                'default',
                                structures.ConcatPlaceholder(concat=[
                                    '--arg1',
                                    structures.InputValuePlaceholder(
                                        input_name='input_prefix'),
                                ]),
                            ])),
                ])
            ])),
    inputs={'input_prefix': structures.InputSpec(type='String')},
)


class StructuresTest(parameterized.TestCase):

    def test_component_spec_with_placeholder_referencing_nonexisting_input_output(
            self):
        with self.assertRaisesRegex(
                pydantic.ValidationError, 'Argument "input_name=\'input000\'" '
                'references non-existing input.'):
            structures.ComponentSpec(
                name='component_1',
                implementation=structures.Implementation(
                    container=structures.ContainerSpec(
                        image='alpine',
                        commands=[
                            'sh',
                            '-c',
                            'set -ex\necho "$0" > "$1"',
                            structures.InputValuePlaceholder(
                                input_name='input000'),
                            structures.OutputPathPlaceholder(
                                output_name='output1'),
                        ],
                    )),
                inputs={'input1': structures.InputSpec(type='String')},
                outputs={'output1': structures.OutputSpec(type='String')},
            )

        with self.assertRaisesRegex(
                pydantic.ValidationError,
                'Argument "output_name=\'output000\'" '
                'references non-existing output.'):
            structures.ComponentSpec(
                name='component_1',
                implementation=structures.Implementation(
                    container=structures.ContainerSpec(
                        image='alpine',
                        commands=[
                            'sh',
                            '-c',
                            'set -ex\necho "$0" > "$1"',
                            structures.InputValuePlaceholder(
                                input_name='input1'),
                            structures.OutputPathPlaceholder(
                                output_name='output000'),
                        ],
                    )),
                inputs={'input1': structures.InputSpec(type='String')},
                outputs={'output1': structures.OutputSpec(type='String')},
            )

    def test_simple_component_spec_save_to_component_yaml(self):
        open_mock = mock.mock_open()
        expected_yaml = textwrap.dedent("""\
        name: component_1
        inputs:
          input1: {type: String}
        outputs:
          output1: {type: String}
        implementation:
          container:
            image: alpine
            commands:
            - sh
            - -c
            - 'set -ex

              echo "$0" > "$1"'
            - {inputValue: input1}
            - {outputPath: output1}
        """)

        with mock.patch("builtins.open", open_mock, create=True):
            structures.ComponentSpec(
                name='component_1',
                implementation=structures.Implementation(
                    container=structures.ContainerSpec(
                        image='alpine',
                        commands=[
                            'sh',
                            '-c',
                            'set -ex\necho "$0" > "$1"',
                            structures.InputValuePlaceholder(
                                input_name='input1'),
                            structures.OutputPathPlaceholder(
                                output_name='output1'),
                        ],
                    )),
                inputs={
                    'input1': structures.InputSpec(type='String')
                },
                outputs={
                    'output1': structures.OutputSpec(type='String')
                },
            ).save_to_component_yaml('test_save_file.txt')

        open_mock.assert_called_with('test_save_file.txt', 'a')
        open_mock.return_value.write.assert_called_once_with(expected_yaml)

    @parameterized.parameters(
        {
            'expected_yaml': V2_YAML_IF_PLACEHOLDER,
            'component': V2_COMPONENT_SPEC_IF_PLACEHOLDER
        },
        {
            'expected_yaml': V2_YAML_CONCAT_PLACEHOLDER,
            'component': V2_COMPONENT_SPEC_CONCAT_PLACEHOLDER
        },
        {
            'expected_yaml': V2_YAML_NESTED_PLACEHOLDER,
            'component': V2_COMPONENT_SPEC_NESTED_PLACEHOLDER
        },
    )
    def test_component_spec_placeholder_save_to_component_yaml(
            self, expected_yaml, component):
        open_mock = mock.mock_open()

        with mock.patch("builtins.open", open_mock, create=True):
            component.save_to_component_yaml('test_save_file.txt')

        open_mock.assert_called_with('test_save_file.txt', 'a')
        open_mock.return_value.write.assert_called_once_with(expected_yaml)

    def test_simple_component_spec_load_from_v2_component_yaml(self):
        component_yaml_v2 = textwrap.dedent("""\
        name: component_1
        inputs:
          input1:
            type: String
        outputs:
          output1:
            type: String
        implementation:
          container:
            image: alpine
            commands:
            - sh
            - -c
            - 'set -ex

                echo "$0" > "$1"'
            - inputValue: input1
            - outputPath: output1
        """)

        generated_spec = structures.ComponentSpec.load_from_component_yaml(
            component_yaml_v2)

        expected_spec = structures.ComponentSpec(
            name='component_1',
            implementation=structures.Implementation(
                container=structures.ContainerSpec(
                    image='alpine',
                    commands=[
                        'sh',
                        '-c',
                        'set -ex\necho "$0" > "$1"',
                        structures.InputValuePlaceholder(input_name='input1'),
                        structures.OutputPathPlaceholder(output_name='output1'),
                    ],
                )),
            inputs={'input1': structures.InputSpec(type='String')},
            outputs={'output1': structures.OutputSpec(type='String')})
        self.assertEqual(generated_spec, expected_spec)

    @parameterized.parameters(
        {
            'yaml': V1_YAML_IF_PLACEHOLDER,
            'expected_component': V2_COMPONENT_SPEC_IF_PLACEHOLDER
        },
        {
            'yaml': V2_YAML_IF_PLACEHOLDER,
            'expected_component': V2_COMPONENT_SPEC_IF_PLACEHOLDER
        },
        {
            'yaml': V1_YAML_CONCAT_PLACEHOLDER,
            'expected_component': V2_COMPONENT_SPEC_CONCAT_PLACEHOLDER
        },
        {
            'yaml': V2_YAML_CONCAT_PLACEHOLDER,
            'expected_component': V2_COMPONENT_SPEC_CONCAT_PLACEHOLDER
        },
        {
            'yaml': V2_YAML_NESTED_PLACEHOLDER,
            'expected_component': V2_COMPONENT_SPEC_NESTED_PLACEHOLDER
        },
    )
    def test_component_spec_placeholder_load_from_v2_component_yaml(
            self, yaml, expected_component):
        generated_spec = structures.ComponentSpec.load_from_component_yaml(yaml)
        self.assertEqual(generated_spec, expected_component)

    def test_component_spec_load_from_v1_component_yaml(self):
        component_yaml_v1 = textwrap.dedent("""\
        name: Component with 2 inputs and 2 outputs
        inputs:
        - {name: Input parameter, type: String}
        - {name: Input artifact}
        outputs:
        - {name: Output 1}
        - {name: Output 2}
        implementation:
          container:
            image: busybox
            command: [sh, -c, '
                mkdir -p $(dirname "$2")
                mkdir -p $(dirname "$3")
                echo "$0" > "$2"
                cp "$1" "$3"
                '
            ]
            args:
            - {inputValue: Input parameter}
            - {inputPath: Input artifact}
            - {outputPath: Output 1}
            - {outputPath: Output 2}
        """)

        generated_spec = structures.ComponentSpec.load_from_component_yaml(
            component_yaml_v1)

        expected_spec = structures.ComponentSpec(
            name='Component with 2 inputs and 2 outputs',
            implementation=structures.Implementation(
                container=structures.ContainerSpec(
                    image='busybox',
                    commands=[
                        'sh',
                        '-c',
                        (' mkdir -p $(dirname "$2") mkdir -p $(dirname "$3") '
                         'echo "$0" > "$2" cp "$1" "$3" '),
                    ],
                    arguments=[
                        structures.InputValuePlaceholder(
                            input_name='Input parameter'),
                        structures.InputPathPlaceholder(
                            input_name='Input artifact'),
                        structures.OutputPathPlaceholder(
                            output_name='Output 1'),
                        structures.OutputPathPlaceholder(
                            output_name='Output 2'),
                    ],
                    env={},
                )),
            inputs={
                'Input parameter': structures.InputSpec(type='String'),
                'Input artifact': structures.InputSpec(type='Artifact')
            },
            outputs={
                'Output 1': structures.OutputSpec(type='Artifact'),
                'Output 2': structures.OutputSpec(type='Artifact'),
            })

        self.assertEqual(generated_spec, expected_spec)


if __name__ == '__main__':
    unittest.main()
