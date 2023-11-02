# Copyright 2021 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for kfp.v2.google.client._cloud_function_create_pipeline_job."""

import datetime
import json
import os
import unittest

from kfp.v2.google.client import _cloud_function_templated_http_request


class CloudFunctionCreatePipelineJobTest(unittest.TestCase):

    def test_preprocess(self):
        test_data_path = os.path.join(
            os.path.dirname(__file__),
            'testdata',
        )
        function_request_path = os.path.join(
            test_data_path,
            'pipeline1_request_body.json',
        )
        expected_pipeline_request_path = os.path.join(
            test_data_path,
            'pipeline1_request_body_final.json',
        )

        with open(function_request_path, 'rb') as f:
            function_request = f.read()

        with open(expected_pipeline_request_path, 'r') as f:
            expected_pipeline_request = json.load(f)

        (_, _, resolved_request_body
        ) = _cloud_function_templated_http_request._preprocess_request_body(
            function_request, time=datetime.datetime(2020, 8, 1, 12, 34))
        actual_pipeline_request = json.loads(resolved_request_body)
        self.assertEqual(actual_pipeline_request, expected_pipeline_request)


if __name__ == '__main__':
    unittest.main()
