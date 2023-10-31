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
"""Tests for kfp.v2.components.experimental.pipeline_channel."""

import unittest

from absl.testing import parameterized
from kfp.v2.components.experimental import pipeline_channel


class PipelineChannelTest(parameterized.TestCase):

    def test_instantiate_pipline_channel(self):
        with self.assertRaisesRegex(
                TypeError, "Can't instantiate abstract class PipelineChannel"):
            p = pipeline_channel.PipelineChannel(
                name='channel',
                channel_type='String',
            )

    def test_invalid_name(self):
        with self.assertRaisesRegex(
                ValueError,
                'Only letters, numbers, spaces, "_", and "-" are allowed in the '
                'name. Must begin with a letter. Got name: 123_abc'):
            p = pipeline_channel.PipelineParameterChannel(
                name='123_abc',
                channel_type='String',
            )

    def test_task_name_and_value_both_set(self):
        with self.assertRaisesRegex(ValueError,
                                    'task_name and value cannot be both set.'):
            p = pipeline_channel.PipelineParameterChannel(
                name='abc',
                channel_type='Integer',
                task_name='task1',
                value=123,
            )

    def test_invalid_type(self):
        with self.assertRaisesRegex(TypeError,
                                    'Artifact is not a parameter type.'):
            p = pipeline_channel.PipelineParameterChannel(
                name='channel1',
                channel_type='Artifact',
            )

        with self.assertRaisesRegex(TypeError,
                                    'String is not an artifact type.'):
            p = pipeline_channel.PipelineArtifactChannel(
                name='channel1',
                channel_type='String',
                task_name='task1',
            )

    @parameterized.parameters(
        {
            'pipeline_channel':
                pipeline_channel.PipelineParameterChannel(
                    name='channel1',
                    task_name='task1',
                    channel_type='String',
                ),
            'str_repr':
                '{{channel:task=task1;name=channel1;type=String;}}',
        },
        {
            'pipeline_channel':
                pipeline_channel.PipelineParameterChannel(
                    name='channel2',
                    channel_type='Integer',
                ),
            'str_repr':
                '{{channel:task=;name=channel2;type=Integer;}}',
        },
        {
            'pipeline_channel':
                pipeline_channel.PipelineArtifactChannel(
                    name='channel3',
                    channel_type={'type_a': {
                        'property_b': 'c'
                    }},
                    task_name='task3',
                ),
            'str_repr':
                '{{channel:task=task3;name=channel3;type={"type_a": {"property_b": "c"}};}}',
        },
        {
            'pipeline_channel':
                pipeline_channel.PipelineParameterChannel(
                    name='channel4',
                    channel_type='Float',
                    value=1.23,
                ),
            'str_repr':
                '{{channel:task=;name=channel4;type=Float;}}',
        },
        {
            'pipeline_channel':
                pipeline_channel.PipelineArtifactChannel(
                    name='channel5',
                    channel_type='Artifact',
                    task_name='task5',
                ),
            'str_repr':
                '{{channel:task=task5;name=channel5;type=Artifact;}}',
        },
    )
    def test_str_repr(self, pipeline_channel, str_repr):
        self.assertEqual(str_repr, str(pipeline_channel))

    def test_extract_pipeline_channels(self):
        p1 = pipeline_channel.PipelineParameterChannel(
            name='channel1',
            channel_type='String',
            value='abc',
        )
        p2 = pipeline_channel.PipelineArtifactChannel(
            name='channel2',
            channel_type='customized_type_b',
            task_name='task2',
        )
        p3 = pipeline_channel.PipelineArtifactChannel(
            name='channel3',
            channel_type={'customized_type_c': {
                'property_c': 'value_c'
            }},
            task_name='task3',
        )
        stuff_chars = ' between '
        payload = str(p1) + stuff_chars + str(p2) + stuff_chars + str(p3)
        params = pipeline_channel.extract_pipeline_channels_from_string(payload)
        self.assertListEqual([p1, p2, p3], params)

        # Expecting the extract_pipelineparam_from_any to dedup pipeline channels
        # among all the payloads.
        payload = [
            str(p1) + stuff_chars + str(p2),
            str(p2) + stuff_chars + str(p3)
        ]
        params = pipeline_channel.extract_pipeline_channels_from_any(payload)
        self.assertListEqual([p1, p2, p3], params)


if __name__ == '__main__':
    unittest.main()
