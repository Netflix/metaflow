# Copyright 2020 The Kubeflow Authors
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
"""Tests for kfp.dsl.serialization_utils module."""
import unittest

from kfp.dsl import serialization_utils

_DICT_DATA = {
    'int1': 1,
    'str1': 'helloworld',
    'float1': 1.11,
    'none1': None,
    'dict1': {
        'int2': 2,
        'list2': ['inside the list', None, 42]
    }
}

_EXPECTED_YAML_LITERAL = """\
dict1:
  int2: 2
  list2:
  - inside the list
  -
  - 42
float1: 1.11
int1: 1
none1:
str1: helloworld
"""


class SerializationUtilsTest(unittest.TestCase):

    def testDumps(self):
        self.assertEqual(_EXPECTED_YAML_LITERAL,
                         serialization_utils.yaml_dump(_DICT_DATA))
