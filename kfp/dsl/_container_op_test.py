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
"""Tests for kfp.v2.dsl.container_op."""
import unittest

from kfp.dsl import _container_op
from kfp.pipeline_spec import pipeline_spec_pb2

from google.protobuf import text_format
from google.protobuf import json_format

_EXPECTED_CONTAINER_WITH_RESOURCE = """
resources {
  cpu_limit: 1.0
  memory_limit: 1.0
  accelerator {
    type: 'NVIDIA_TESLA_K80'
    count: 1
  }
}
"""

# Shorthand for PipelineContainerSpec
_PipelineContainerSpec = pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec


class ContainerOpTest(unittest.TestCase):

    def test_chained_call_resource_setter(self):
        task = _container_op.ContainerOp(name='test_task', image='python:3.7')
        task.container_spec = _PipelineContainerSpec()
        (task.set_cpu_limit('1').set_memory_limit(
            '1G').add_node_selector_constraint(
                'cloud.google.com/gke-accelerator',
                'nvidia-tesla-k80').set_gpu_limit(1))

        expected_container_spec = text_format.Parse(
            _EXPECTED_CONTAINER_WITH_RESOURCE, _PipelineContainerSpec())

        self.assertDictEqual(
            json_format.MessageToDict(task.container_spec),
            json_format.MessageToDict(expected_container_spec))


if __name__ == '__main__':
    unittest.main()
