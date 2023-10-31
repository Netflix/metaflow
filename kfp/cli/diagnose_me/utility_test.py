# Lint as: python3
# Copyright 2019 The Kubeflow Authors. All Rights Reserved.
#
# Licensed under the Apache License,Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for diagnose_me.utility."""

import unittest
from kfp.cli.diagnose_me import utility


class UtilityTest(unittest.TestCase):

    def test_parse_raw_input_json(self):
        """Testing json stdout is correctly parsed."""
        response = utility.ExecutorResponse()
        response._stdout = '{"key":"value"}'
        response._parse_raw_input()

        self.assertEqual(response._json, '{"key":"value"}')
        self.assertEqual(response._parsed_output, {'key': 'value'})

    def test_parse_raw_input_text(self):
        """Testing non-json stdout is correctly parsed."""
        response = utility.ExecutorResponse()
        response._stdout = 'non-json string'
        response._parse_raw_input()

        self.assertEqual(response._json, '"non-json string"')
        self.assertEqual(response._parsed_output, 'non-json string')


if __name__ == '__main__':
    unittest.main()
