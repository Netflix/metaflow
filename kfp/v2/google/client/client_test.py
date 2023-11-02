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
"""Tests for kfp.v2.google.client.client."""

import datetime
import json
import os
import unittest
from typing import Any, Dict
from unittest import mock

from googleapiclient import discovery, http
from kfp.v2.google.client import client, client_utils

# Mock response for get job request.
_EXPECTED_GET_RESPONSE = 'good job spec'
_GET_RESPONSES = {
    'projects/test-project/locations/us-central1/'
    'pipelineJobs/test-job':
        _EXPECTED_GET_RESPONSE
}


def _load_test_data(filename: str) -> Dict[Any, Any]:
    """Test helper function that loads json from testdata."""
    file_path = os.path.join(os.path.dirname(__file__), 'testdata', filename)
    with open(file_path) as f:
        return json.load(f)


# Mock the Python API client classes for testing purpose.
class _MockClient(object):
    """Mocks Python Google API client."""

    def projects(self):  # pylint: disable=invalid-name
        return _MockProjectsResource()


class _MockProjectsResource(object):
    """Mocks API Resource returned by projects()."""

    def locations(self):  # pylint: disable=invalid-name
        return _MockLocationResource()


class _MockLocationResource(object):
    """Mocks API Resource returned by locations()."""

    def pipelineJobs(self):  # pylint: disable=invalid-name
        return _MockPipelineJobsResource()


class _MockPipelineJobsResource(object):
    """Mocks API Resource returned by pipelineJobs()."""

    class _MockGetRequest(http.HttpRequest):

        def __init__(self, name: str):
            self._name = name

        def execute(self):
            return _GET_RESPONSES.get(self._name)

    def get(self, name: str):  # pylint: disable=invalid-name
        """Mocks get job request."""
        return self._MockGetRequest(name=name)


class ClientTest(unittest.TestCase):

    def test_client_init_with_defaults(self):
        api_client = client.AIPlatformClient(
            project_id='test-project', region='us-central1')

        self.assertEqual(api_client._project_id, 'test-project')
        self.assertEqual(api_client._region, 'us-central1')
        self.assertEqual(api_client._endpoint,
                         'us-central1-aiplatform.googleapis.com')

    @mock.patch.object(client, '_get_current_time', autospec=True)
    @mock.patch.object(client_utils, 'load_json', autospec=True)
    def test_create_run_from_pipeline_job(self, mock_load_json,
                                          mock_get_current_time):
        mock_load_json.return_value = _load_test_data('pipeline_job.json')
        mock_get_current_time.return_value = datetime.date(2020, 10, 28)

        api_client = client.AIPlatformClient(
            project_id='test-project', region='us-central1')
        with mock.patch.object(
                api_client, '_submit_job', autospec=True) as mock_submit:
            api_client.create_run_from_job_spec(
                job_spec_path='path/to/pipeline_job.json')

            golden = _load_test_data('pipeline_job.json')
            mock_submit.assert_called_once_with(
                job_spec=golden, job_id='sample-test-pipeline-20201028000000')

    @mock.patch.object(client, '_get_current_time', autospec=True)
    @mock.patch.object(client_utils, 'load_json', autospec=True)
    def test_job_id_parameters_override(self, mock_load_json,
                                        mock_get_current_time):
        mock_load_json.return_value = _load_test_data('pipeline_job.json')
        mock_get_current_time.return_value = datetime.date(2020, 10, 28)

        api_client = client.AIPlatformClient(
            project_id='test-project', region='us-central1')
        with mock.patch.object(
                api_client, '_submit_job', autospec=True) as mock_submit:
            api_client.create_run_from_job_spec(
                job_spec_path='path/to/pipeline_job.json',
                job_id='my-new-id',
                pipeline_root='gs://bucket/new-blob',
                parameter_values={
                    'text': 'Hello test!',
                    'list': [1, 2, 3],
                })

            golden = _load_test_data('pipeline_job.json')
            golden['name'] = ('projects/test-project/locations/us-central1/'
                              'pipelineJobs/my-new-id')
            golden['displayName'] = 'my-new-id'
            golden['runtimeConfig'][
                'gcsOutputDirectory'] = 'gs://bucket/new-blob'
            golden['runtimeConfig']['parameters']['text'] = {
                'stringValue': 'Hello test!'
            }
            golden['runtimeConfig']['parameters']['list'] = {
                'stringValue': '[1, 2, 3]'
            }
            mock_submit.assert_called_once_with(
                job_spec=golden, job_id='my-new-id')

    @mock.patch.object(client, '_get_current_time', autospec=True)
    @mock.patch.object(client_utils, 'load_json', autospec=True)
    def test_advanced_settings(self, mock_load_json, mock_get_current_time):
        mock_load_json.return_value = _load_test_data('pipeline_job.json')
        mock_get_current_time.return_value = datetime.date(2020, 10, 28)

        api_client = client.AIPlatformClient(
            project_id='test-project', region='us-central1')
        with mock.patch.object(
                api_client, '_submit_job', autospec=True) as mock_submit:
            api_client.create_run_from_job_spec(
                job_spec_path='path/to/pipeline_job.json',
                cmek='custom-key',
                service_account='custom-sa',
                network='custom-network')

            golden = _load_test_data('pipeline_job.json')
            golden['encryptionSpec'] = {'kmsKeyName': 'custom-key'}
            golden['serviceAccount'] = 'custom-sa'
            golden['network'] = 'custom-network'
            mock_submit.assert_called_once_with(
                job_spec=golden, job_id='sample-test-pipeline-20201028000000')

    def test_extract_job_id_success(self):

        job_name = 'projects/123/locations/us-central1/pipelineJobs/0123456789'
        self.assertEqual('0123456789', client._extract_job_id(job_name))
        self.assertIsNone(client._extract_job_id('invalid name'))

    @mock.patch.object(client, '_get_current_time', autospec=True)
    @mock.patch.object(client, '_get_gcp_credential', autospec=True)
    @mock.patch.object(discovery, 'build_from_document', autospec=True)
    def test_get_job_success(self, mock_build_client, mock_get_gcp_credential,
                             mock_get_current_time):
        mock_get_current_time.return_value = datetime.date(2020, 10, 28)
        mock_build_client.return_value = _MockClient()
        api_client = client.AIPlatformClient(
            project_id='test-project', region='us-central1')

        self.assertEqual(_EXPECTED_GET_RESPONSE,
                         api_client.get_job(job_id='test-job'))
        mock_get_gcp_credential.assert_called_once()

    @mock.patch.object(client, '_get_current_time', autospec=True)
    @mock.patch.object(client_utils, 'load_json', autospec=True)
    def test_disable_caching(self, mock_load_json, mock_get_current_time):
        mock_load_json.return_value = _load_test_data('pipeline_job.json')
        mock_get_current_time.return_value = datetime.date(2020, 10, 28)

        api_client = client.AIPlatformClient(
            project_id='test-project', region='us-central1')
        with mock.patch.object(
                api_client, '_submit_job', autospec=True) as mock_submit:
            api_client.create_run_from_job_spec(
                job_spec_path='path/to/pipeline_job.json', enable_caching=False)

            golden = _load_test_data('pipeline_job.json')
            golden = json.loads(
                json.dumps(golden).replace('"enableCache": true',
                                           '"enableCache": false'))
            mock_submit.assert_called_once_with(
                job_spec=golden, job_id='sample-test-pipeline-20201028000000')

    @mock.patch.object(client, '_get_current_time', autospec=True)
    @mock.patch.object(client_utils, 'load_json', autospec=True)
    def test_setting_labels(self, mock_load_json, mock_get_current_time):
        mock_load_json.return_value = _load_test_data('pipeline_job.json')
        mock_get_current_time.return_value = datetime.date(2020, 10, 28)

        user_labels = {'label1': 'value1', 'label2': 'value2'}
        api_client = client.AIPlatformClient(
            project_id='test-project', region='us-central1')
        with mock.patch.object(
                api_client, '_submit_job', autospec=True) as mock_submit:
            api_client.create_run_from_job_spec(
                job_spec_path='path/to/pipeline_job.json', labels=user_labels)

            golden = _load_test_data('pipeline_job.json')
            golden['labels'] = user_labels
            mock_submit.assert_called_once_with(
                job_spec=golden, job_id='sample-test-pipeline-20201028000000')


if __name__ == '__main__':
    unittest.main()
