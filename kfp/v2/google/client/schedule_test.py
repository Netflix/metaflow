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
"""Tests for kfp.v2.google.client.schedule."""

import base64
import hashlib
import json
import os
import unittest
from unittest import mock

from kfp.v2.google.client import schedule


class ScheduleTest(unittest.TestCase):

    def test_create_from_pipeline_file(self):
        test_data_path = os.path.join(os.path.dirname(__file__), 'testdata')
        pipeline_path = os.path.join(test_data_path, 'pipeline1.json')
        pipeline_request_body_path = os.path.join(
            test_data_path, 'pipeline1_request_body.json')

        project_id = 'project-id'
        location = 'us-central1'
        function_url = ('https://{}-{}.cloudfunctions.net/' +
                        'templated_http_request-v1').format(
                            location, project_id)
        with mock.patch(
                'kfp.v2.google.client.schedule._enable_required_apis',
                return_value=None,
        ), mock.patch(
                'kfp.v2.google.client.schedule._get_proxy_cloud_function_endpoint',
                return_value=function_url,
        ), mock.patch(
                'kfp.v2.google.client.schedule._create_scheduler_job',
                spec=True) as create_scheduler_job_mock:
            schedule.create_from_pipeline_file(
                pipeline_path=pipeline_path,
                schedule='46 * * * *',
                project_id=project_id,
                region=location,
                time_zone='America/Los_Angeles',
                parameter_values={'name_param': 'World'},
                pipeline_root='gs://my-project/pipeline_root/tmp/',
            )

            with open(pipeline_request_body_path, 'rb') as f:
                expected_body_dict = json.load(f)
            expected_body_json = json.dumps(expected_body_dict)
            expected_body_data = expected_body_json.encode('utf-8')
            expected_body_data_hash = hashlib.sha256(
                expected_body_data).hexdigest()[0:8]

            create_scheduler_job_mock.assert_called_with(
                project_location_path='projects/{}/locations/{}'.format(
                    project_id, location),
                job_body={
                    'name':
                        'projects/{}/locations/{}/jobs/pipeline_my-pipeline_{}_46-a-a-a-a'
                        .format(project_id, location, expected_body_data_hash),
                    'schedule':
                        '46 * * * *',
                    'time_zone':
                        'America/Los_Angeles',
                    'http_target': {
                        'http_method':
                            'POST',
                        'uri':
                            function_url,
                        'body':
                            base64.b64encode(expected_body_data).decode('utf-8'
                                                                       ),
                        'oidc_token': {
                            'service_account_email':
                                'project-id@appspot.gserviceaccount.com',
                        },
                    },
                },
            )

    def test_create_schedule_when_cloud_function_already_exists(self):
        test_data_path = os.path.join(os.path.dirname(__file__), 'testdata')
        pipeline_path = os.path.join(test_data_path, 'pipeline1.json')

        project_id = 'project-id'
        location = 'us-central1'
        function_url = ('https://{}-{}.cloudfunctions.net/' +
                        'templated_http_request-v1').format(
                            location, project_id)

        def mock_get_cloud_functions_api():
            functions_api = mock.Mock()

            def function_get(name):
                del name
                request_mock = mock.Mock()
                request_mock.execute.return_value = {
                    'httpsTrigger': {
                        'url': function_url
                    }
                }
                return request_mock

            functions_api.get = function_get
            return functions_api

        with mock.patch(
                'kfp.v2.google.client.schedule._enable_required_apis',
                return_value=None,
        ), mock.patch(
                'kfp.v2.google.client.schedule._get_cloud_functions_api',
                new=mock_get_cloud_functions_api,
        ), mock.patch(
                'kfp.v2.google.client.schedule._create_scheduler_job',
                spec=True) as create_scheduler_job_mock:
            schedule.create_from_pipeline_file(
                pipeline_path=pipeline_path,
                schedule='46 * * * *',
                project_id=project_id,
                region=location,
                time_zone='America/Los_Angeles',
            )
            create_scheduler_job_mock.assert_called_once()
            actual_job_body = create_scheduler_job_mock.call_args[1]['job_body']
            self.assertEqual(actual_job_body['http_target']['uri'],
                             function_url)


if __name__ == '__main__':
    unittest.main()
