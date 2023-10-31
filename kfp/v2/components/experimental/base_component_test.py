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
"""Tests for kfp.v2.components.experimental.base_component."""

import unittest
from unittest.mock import patch

from kfp.v2.components.experimental import base_component
from kfp.v2.components.experimental import structures
from kfp.v2.components.experimental import pipeline_task


class TestComponent(base_component.BaseComponent):

    def execute(self, *args, **kwargs):
        pass


component_op = TestComponent(
    component_spec=structures.ComponentSpec(
        name='component_1',
        implementation=structures.ContainerSpec(
            image='alpine',
            commands=[
                'sh',
                '-c',
                'set -ex\necho "$0" "$1" "$2" > "$3"',
                structures.InputValuePlaceholder(input_name='input1'),
                structures.InputValuePlaceholder(input_name='input2'),
                structures.InputValuePlaceholder(input_name='input3'),
                structures.OutputPathPlaceholder(output_name='output1'),
            ],
        ),
        inputs={
            'input1': structures.InputSpec(type='String'),
            'input2': structures.InputSpec(type='Integer'),
            'input3': structures.InputSpec(type='Float', default=3.14),
        },
        outputs={
            'output1': structures.OutputSpec(name='output1', type='String'),
        },
    ))


class BaseComponentTest(unittest.TestCase):

    @patch.object(pipeline_task, 'create_pipeline_task', autospec=True)
    def test_instantiate_component_with_keyword_arguments(
            self, mock_create_pipeline_task):

        component_op(input1='hello', input2=100, input3=1.23)

        mock_create_pipeline_task.assert_called_once_with(
            component_spec=component_op.component_spec,
            arguments={
                'input1': 'hello',
                'input2': 100,
                'input3': 1.23,
            })

    @patch.object(pipeline_task, 'create_pipeline_task', autospec=True)
    def test_instantiate_component_omitting_arguments_with_default(
            self, mock_create_pipeline_task):

        component_op(input1='hello', input2=100)

        mock_create_pipeline_task.assert_called_once_with(
            component_spec=component_op.component_spec,
            arguments={
                'input1': 'hello',
                'input2': 100,
                'input3': '3.14',
            })

    def test_instantiate_component_with_positional_arugment(self):
        with self.assertRaisesRegex(
                TypeError,
                'Components must be instantiated using keyword arguments. Positional '
                'parameters are not allowed \(found 3 such parameters for component '
                '"component_1"\).'):
            component_op('abc', 1, 2.3)

    def test_instantiate_component_with_unexpected_keyword_arugment(self):
        with self.assertRaisesRegex(
                TypeError,
                'component_1\(\) got an unexpected keyword argument "input4".'):
            component_op(input1='abc', input2=1, input3=2.3, input4='extra')

    def test_instantiate_component_with_missing_arugments(self):
        with self.assertRaisesRegex(
                TypeError,
                'component_1\(\) missing 1 required positional argument: input1.'
        ):
            component_op(input2=1)

        with self.assertRaisesRegex(
                TypeError,
                'component_1\(\) missing 2 required positional arguments: input1,input2.'
        ):
            component_op()


if __name__ == '__main__':
    unittest.main()
