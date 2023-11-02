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
"""Tests for kfp.v2.components.types.type_annotations."""

import unittest
from typing import Any, Dict, List, Optional

from absl.testing import parameterized
from kfp.v2.components.types import type_annotations
from kfp.v2.components.types.artifact_types import Model
from kfp.v2.components.types.type_annotations import (Input, InputAnnotation,
                                                      InputPath, Output,
                                                      OutputAnnotation,
                                                      OutputPath)


class AnnotationsTest(parameterized.TestCase):

    def test_is_artifact_annotation(self):
        self.assertTrue(type_annotations.is_artifact_annotation(Input[Model]))
        self.assertTrue(type_annotations.is_artifact_annotation(Output[Model]))
        self.assertTrue(
            type_annotations.is_artifact_annotation(Output['MyArtifact']))

        self.assertFalse(type_annotations.is_artifact_annotation(Model))
        self.assertFalse(type_annotations.is_artifact_annotation(int))
        self.assertFalse(type_annotations.is_artifact_annotation('Dataset'))
        self.assertFalse(type_annotations.is_artifact_annotation(List[str]))
        self.assertFalse(type_annotations.is_artifact_annotation(Optional[str]))

    def test_is_input_artifact(self):
        self.assertTrue(type_annotations.is_input_artifact(Input[Model]))
        self.assertTrue(type_annotations.is_input_artifact(Input))

        self.assertFalse(type_annotations.is_input_artifact(Output[Model]))
        self.assertFalse(type_annotations.is_input_artifact(Output))

    def test_is_output_artifact(self):
        self.assertTrue(type_annotations.is_output_artifact(Output[Model]))
        self.assertTrue(type_annotations.is_output_artifact(Output))

        self.assertFalse(type_annotations.is_output_artifact(Input[Model]))
        self.assertFalse(type_annotations.is_output_artifact(Input))

    def test_get_io_artifact_class(self):
        self.assertEqual(
            type_annotations.get_io_artifact_class(Output[Model]), Model)

        self.assertEqual(type_annotations.get_io_artifact_class(Input), None)
        self.assertEqual(type_annotations.get_io_artifact_class(Output), None)
        self.assertEqual(type_annotations.get_io_artifact_class(Model), None)
        self.assertEqual(type_annotations.get_io_artifact_class(str), None)

    def test_get_io_artifact_annotation(self):
        self.assertEqual(
            type_annotations.get_io_artifact_annotation(Output[Model]),
            OutputAnnotation)
        self.assertEqual(
            type_annotations.get_io_artifact_annotation(Input[Model]),
            InputAnnotation)
        self.assertEqual(
            type_annotations.get_io_artifact_annotation(Input), InputAnnotation)
        self.assertEqual(
            type_annotations.get_io_artifact_annotation(Output),
            OutputAnnotation)

        self.assertEqual(
            type_annotations.get_io_artifact_annotation(Model), None)
        self.assertEqual(type_annotations.get_io_artifact_annotation(str), None)

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
        {
            'original_annotation': Input[Model],
            'expected_annotation': Input[Model],
        },
        {
            'original_annotation': InputPath('Model'),
            'expected_annotation': InputPath('Model'),
        },
        {
            'original_annotation': OutputPath(Model),
            'expected_annotation': OutputPath(Model),
        },
    )
    def test_maybe_strip_optional_from_annotation(self, original_annotation,
                                                  expected_annotation):
        self.assertEqual(
            expected_annotation,
            type_annotations.maybe_strip_optional_from_annotation(
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
            type_annotations.get_short_type_name(original_type_name))


if __name__ == '__main__':
    unittest.main()
