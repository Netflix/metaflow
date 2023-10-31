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
"""Tests for kfp.dsl.dsl_utils."""

import unittest

from kfp.dsl import dsl_utils
from kfp.pipeline_spec import pipeline_spec_pb2
from google.protobuf import json_format


class _DummyClass(object):
    pass


class DslUtilsTest(unittest.TestCase):

    def test_sanitize_component_name(self):
        self.assertEqual('comp-my-component',
                         dsl_utils.sanitize_component_name('My component'))

    def test_sanitize_executor_label(self):
        self.assertEqual('exec-my-component',
                         dsl_utils.sanitize_executor_label('My component'))

    def test_sanitize_task_name(self):
        self.assertEqual('my-component-1',
                         dsl_utils.sanitize_task_name('My component 1'))

    def test_get_ir_value(self):
        self.assertDictEqual(
            json_format.MessageToDict(pipeline_spec_pb2.Value(int_value=42)),
            json_format.MessageToDict(dsl_utils.get_value(42)))
        self.assertDictEqual(
            json_format.MessageToDict(
                pipeline_spec_pb2.Value(double_value=12.2)),
            json_format.MessageToDict(dsl_utils.get_value(12.2)))
        self.assertDictEqual(
            json_format.MessageToDict(
                pipeline_spec_pb2.Value(string_value='hello world')),
            json_format.MessageToDict(dsl_utils.get_value('hello world')))
        with self.assertRaisesRegex(TypeError, 'Got unexpected type'):
            dsl_utils.get_value(_DummyClass())


if __name__ == '__main__':
    unittest.main()
