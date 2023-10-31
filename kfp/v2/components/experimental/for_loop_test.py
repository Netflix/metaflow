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
"""Tests for kfp.v2.components.experimental.for_loop."""
import unittest

from absl.testing import parameterized
from kfp.v2.components.experimental import pipeline_channel
from kfp.v2.components.experimental import for_loop


class ForLoopTest(parameterized.TestCase):

    @parameterized.parameters(
        {
            'collection_type': 'List[int]',
            'item_type': 'int',
        },
        {
            'collection_type': 'typing.List[str]',
            'item_type': 'str',
        },
        {
            'collection_type': 'typing.Tuple[  float   ]',
            'item_type': 'float',
        },
        {
            'collection_type': 'typing.Sequence[Dict[str, str]]',
            'item_type': 'Dict[str, str]',
        },
        {
            'collection_type': 'List',
            'item_type': None,
        },
    )
    def test_get_loop_item_type(self, collection_type, item_type):
        self.assertEqual(
            for_loop._get_loop_item_type(collection_type), item_type)

    @parameterized.parameters(
        {
            'dict_type': 'Dict[str, int]',
            'value_type': 'int',
        },
        {
            'dict_type': 'typing.Mapping[str,float]',
            'value_type': 'float',
        },
        {
            'dict_type': 'typing.Mapping[str, Dict[str, str] ]',
            'value_type': 'Dict[str, str]',
        },
        {
            'dict_type': 'dict',
            'value_type': None,
        },
    )
    def test_get_subvar_type(self, dict_type, value_type):
        self.assertEqual(for_loop._get_subvar_type(dict_type), value_type)

    @parameterized.parameters(
        {
            'channel':
                pipeline_channel.PipelineParameterChannel(
                    name='param1',
                    channel_type='List[str]',
                ),
            'expected_serialization_value':
                '{{channel:task=;name=param1-loop-item;type=str;}}',
        },
        {
            'channel':
                pipeline_channel.PipelineParameterChannel(
                    name='output1',
                    channel_type='List[Dict[str, str]]',
                    task_name='task1',
                ),
            'expected_serialization_value':
                '{{channel:task=task1;name=output1-loop-item;type=Dict[str, str];}}',
        },
    )
    def test_loop_argument_from_pipeline_channel(self, channel,
                                                 expected_serialization_value):
        loop_argument = for_loop.LoopArgument.from_pipeline_channel(channel)
        self.assertEqual(loop_argument.items_or_pipeline_channel, channel)
        self.assertEqual(str(loop_argument), expected_serialization_value)

    @parameterized.parameters(
        {
            'raw_items': ['a', 'b', 'c'],
            'name_code':
                '1',
            'expected_serialization_value':
                '{{channel:task=;name=loop-item-param-1;type=str;}}',
        },
        {
            'raw_items': [
                {
                    'A_a': 1
                },
                {
                    'A_a': 2
                },
            ],
            'name_code':
                '2',
            'expected_serialization_value':
                '{{channel:task=;name=loop-item-param-2;type=dict;}}',
        },
    )
    def test_loop_argument_from_raw_items(self, raw_items, name_code,
                                          expected_serialization_value):
        loop_argument = for_loop.LoopArgument.from_raw_items(
            raw_items, name_code)
        self.assertEqual(loop_argument.items_or_pipeline_channel, raw_items)
        self.assertEqual(str(loop_argument), expected_serialization_value)

    @parameterized.parameters(
        {
            'name': 'abc-loop-item',
            'expected_result': True
        },
        {
            'name': 'abc-loop-item-subvar-a',
            'expected_result': True
        },
        {
            'name': 'loop-item-param-1',
            'expected_result': True
        },
        {
            'name': 'loop-item-param-1-subvar-a',
            'expected_result': True
        },
        {
            'name': 'param1',
            'expected_result': False
        },
    )
    def test_name_is_loop_argument(self, name, expected_result):
        self.assertEqual(
            for_loop.LoopArgument.name_is_loop_argument(name), expected_result)

    @parameterized.parameters(
        {
            'subvar_name': 'a',
            'valid': True
        },
        {
            'subvar_name': 'A_a',
            'valid': True
        },
        {
            'subvar_name': 'a0',
            'valid': True
        },
        {
            'subvar_name': 'a-b',
            'valid': False
        },
        {
            'subvar_name': '0',
            'valid': False
        },
        {
            'subvar_name': 'a#',
            'valid': False
        },
    )
    def test_create_loop_argument_varaible(self, subvar_name, valid):
        loop_argument = for_loop.LoopArgument.from_pipeline_channel(
            pipeline_channel.PipelineParameterChannel(
                name='param1',
                channel_type='List[Dict[str, str]]',
            ))
        if valid:
            loop_arg_var = for_loop.LoopArgumentVariable(
                loop_argument=loop_argument,
                subvar_name=subvar_name,
            )
            self.assertEqual(loop_arg_var.loop_argument, loop_argument)
            self.assertEqual(loop_arg_var.subvar_name, subvar_name)
        else:
            with self.assertRaisesRegex(ValueError,
                                        'Tried to create subvariable'):
                for_loop.LoopArgumentVariable(
                    loop_argument=loop_argument,
                    subvar_name=subvar_name,
                )


if __name__ == '__main__':
    unittest.main()
