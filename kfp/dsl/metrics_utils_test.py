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
"""Tests for kfp.dsl.metrics_utils."""

import os
import unittest
import yaml
import json
import jsonschema

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from kfp.dsl import metrics_utils

from google.protobuf import json_format


class MetricsUtilsTest(unittest.TestCase):

    def test_confusion_matrix(self):
        conf_matrix = metrics_utils.ConfusionMatrix()
        conf_matrix.set_categories(['dog', 'cat', 'horses'])
        conf_matrix.log_row('dog', [2, 6, 0])
        conf_matrix.log_cell('cat', 'dog', 3)
        with open(
                os.path.join(
                    os.path.dirname(__file__), 'test_data',
                    'expected_confusion_matrix.json')) as json_file:
            expected_json = json.load(json_file)
            self.assertEqual(expected_json, conf_matrix.get_metrics())

    def test_bulkload_confusion_matrix(self):
        conf_matrix = metrics_utils.ConfusionMatrix()
        conf_matrix.load_matrix(['dog', 'cat', 'horses'],
                                [[2, 6, 0], [3, 5, 6], [5, 7, 8]])

        with open(
                os.path.join(
                    os.path.dirname(__file__), 'test_data',
                    'expected_bulk_loaded_confusion_matrix.json')) as json_file:
            expected_json = json.load(json_file)
            self.assertEqual(expected_json, conf_matrix.get_metrics())

    def test_confidence_metrics(self):
        confid_metrics = metrics_utils.ConfidenceMetrics()
        confid_metrics.confidenceThreshold = 24.3
        confid_metrics.recall = 24.5
        confid_metrics.falsePositiveRate = 98.4
        expected_dict = {
            'confidenceThreshold': 24.3,
            'recall': 24.5,
            'falsePositiveRate': 98.4
        }
        self.assertEqual(expected_dict, confid_metrics.get_metrics())


if __name__ == '__main__':
    unittest.main()
