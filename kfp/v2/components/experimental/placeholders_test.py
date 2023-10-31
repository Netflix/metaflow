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
"""Tests for kfp.v2.components.experimental.placeholders."""

import unittest

from kfp.v2.components.experimental import placeholders


class PlaceholdersTest(unittest.TestCase):

    def test_input_artifact_uri_placeholder(self):
        self.assertEqual(
            "{{$.inputs.artifacts['input1'].uri}}",
            placeholders.input_artifact_uri_placeholder('input1'),
        )

    def test_output_artifact_uri_placeholder(self):
        self.assertEqual(
            "{{$.outputs.artifacts['output1'].uri}}",
            placeholders.output_artifact_uri_placeholder('output1'),
        )

    def test_input_artifact_path_placeholder(self):
        self.assertEqual(
            "{{$.inputs.artifacts['input1'].path}}",
            placeholders.input_artifact_path_placeholder('input1'),
        )

    def test_output_artifact_path_placeholder(self):
        self.assertEqual(
            "{{$.outputs.artifacts['output1'].path}}",
            placeholders.output_artifact_path_placeholder('output1'),
        )

    def test_input_parameter_placeholder(self):
        self.assertEqual(
            "{{$.inputs.parameters['input1']}}",
            placeholders.input_parameter_placeholder('input1'),
        )

    def test_output_parameter_path_placeholder(self):
        self.assertEqual(
            "{{$.outputs.parameters['output1'].output_file}}",
            placeholders.output_parameter_path_placeholder('output1'),
        )

    def test_executor_input_placeholder(self):
        self.assertEqual(
            "{{{{$}}}}",
            placeholders.executor_input_placeholder(),
        )


if __name__ == '__main__':
    unittest.main()
