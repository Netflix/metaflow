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
"""Tests for kfp.containers._component_builder module."""
import os
import shutil
import tempfile
import unittest
from unittest import mock

from kfp.containers import _component_builder
from kfp.containers import _container_builder
from kfp import components

_TEST_TARGET_IMAGE = 'gcr.io/my-project/my-image'
_TEST_STAGING_LOCATION = 'gs://my-project/tmp'


def test_function(test_param: str,
                  test_artifact: components.InputArtifact('Dataset'),
                  test_output: components.OutputArtifact('Model')):
    pass


class ComponentBuilderTest(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp_dir = tempfile.mkdtemp()
        self._old_dir = os.getcwd()
        with open(
                os.path.join(
                    os.path.dirname(__file__), 'testdata',
                    'expected_component.yaml'), 'r') as f:
            self._expected_component_yaml = f.read()
        os.chdir(self._tmp_dir)
        self.addCleanup(os.chdir, self._old_dir)
        self.addCleanup(shutil.rmtree, self._tmp_dir)

    @mock.patch.object(
        _container_builder.ContainerBuilder,
        'build',
        return_value='gcr.io/my-project/my-image:123456',
        autospec=True)
    def testBuildV2PythonComponent(self, mock_build):
        self.maxDiff = 2400
        _component_builder.build_python_component(
            component_func=test_function,
            target_image=_TEST_TARGET_IMAGE,
            staging_gcs_path=_TEST_STAGING_LOCATION,
            target_component_file='component.yaml',
            is_v2=True)

        with open('component.yaml', 'r') as f:
            actual_component_yaml = f.read()

        self.assertEquals(actual_component_yaml, self._expected_component_yaml)
