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
"""Tests for kfp.components.type_annoation_utils."""

import unittest
from absl.testing import parameterized
from typing import Any, Dict, List, Optional

from kfp.components import type_annotation_utils


class TypeAnnotationUtilsTest(parameterized.TestCase):

    @parameterized.parameters(
        {
            'original_annotation': str,
            'expected_annotation': str,
        },
        {
            'original_annotation': 'MyCustomType',
            'expected_annotation': 'MyCustomType',
        },
        {
            'original_annotation': List[int],
            'expected_annotation': List[int],
        },
        {
            'original_annotation': Optional[str],
            'expected_annotation': str,
        },
        {
            'original_annotation': Optional[Dict[str, float]],
            'expected_annotation': Dict[str, float],
        },
        {
            'original_annotation': Optional[List[Dict[str, Any]]],
            'expected_annotation': List[Dict[str, Any]],
        },
    )
    def test_maybe_strip_optional_from_annotation(self, original_annotation,
                                                  expected_annotation):
        self.assertEqual(
            expected_annotation,
            type_annotation_utils.maybe_strip_optional_from_annotation(
                original_annotation))

    @parameterized.parameters(
        {
            'original_type_name': 'str',
            'expected_type_name': 'str',
        },
        {
            'original_type_name': 'typing.List[int]',
            'expected_type_name': 'List',
        },
        {
            'original_type_name': 'List[int]',
            'expected_type_name': 'List',
        },
        {
            'original_type_name': 'Dict[str, str]',
            'expected_type_name': 'Dict',
        },
        {
            'original_type_name': 'List[Dict[str, str]]',
            'expected_type_name': 'List',
        },
    )
    def test_get_short_type_name(self, original_type_name, expected_type_name):
        self.assertEqual(
            expected_type_name,
            type_annotation_utils.get_short_type_name(original_type_name))


if __name__ == '__main__':
    unittest.main()
