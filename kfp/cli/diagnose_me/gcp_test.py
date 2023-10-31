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
"""Tests for diagnose_me.gcp."""

from typing import Text
import unittest
from unittest import mock
from kfp.cli.diagnose_me import gcp
from kfp.cli.diagnose_me import utility


class GoogleCloudTest(unittest.TestCase):

    @mock.patch.object(gcp, 'execute_gcloud_command', autospec=True)
    def test_project_configuration_gcloud(self, mock_execute_gcloud_command):
        """Tests gcloud commands."""
        gcp.get_gcp_configuration(gcp.Commands.GET_APIS)
        mock_execute_gcloud_command.assert_called_once_with(
            ['services', 'list'], project_id=None, human_readable=False)

    @mock.patch.object(gcp, 'execute_gsutil_command', autospec=True)
    def test_project_configuration_gsutil(self, mock_execute_gsutil_command):
        """Test Gsutil commands."""
        gcp.get_gcp_configuration(gcp.Commands.GET_STORAGE_BUCKETS)
        mock_execute_gsutil_command.assert_called_once_with(['ls'],
                                                            project_id=None)

    def test_Commands(self):
        """Verify commands are formaated properly."""
        for command in gcp.Commands:
            self.assertIsInstance(gcp._command_string[command], Text)
            self.assertNotIn('\t', gcp._command_string[command])
            self.assertNotIn('\n', gcp._command_string[command])

    @mock.patch.object(utility, 'ExecutorResponse', autospec=True)
    def test_execute_gsutil_command(self, mock_executor_response):
        """Test execute_gsutil_command."""
        gcp.execute_gsutil_command(
            [gcp._command_string[gcp.Commands.GET_STORAGE_BUCKETS]])
        mock_executor_response().execute_command.assert_called_once_with(
            ['gsutil', 'ls'])

        gcp.execute_gsutil_command(
            [gcp._command_string[gcp.Commands.GET_STORAGE_BUCKETS]],
            project_id='test_project')
        mock_executor_response().execute_command.assert_called_with(
            ['gsutil', 'ls', '-p', 'test_project'])

    @mock.patch.object(utility, 'ExecutorResponse', autospec=True)
    def test_execute_gcloud_command(self, mock_executor_response):
        """Test execute_gcloud_command."""
        gcp.execute_gcloud_command(
            gcp._command_string[gcp.Commands.GET_APIS].split(' '))
        mock_executor_response().execute_command.assert_called_once_with(
            ['gcloud', 'services', 'list', '--format', 'json'])

        gcp.execute_gcloud_command(
            gcp._command_string[gcp.Commands.GET_APIS].split(' '),
            project_id='test_project')
        # verify project id is added correctly
        mock_executor_response().execute_command.assert_called_with([
            'gcloud', 'services', 'list', '--format', 'json', '--project',
            'test_project'
        ])
        # verify human_readable removes json fromat flag
        gcp.execute_gcloud_command(
            gcp._command_string[gcp.Commands.GET_APIS].split(' '),
            project_id='test_project',
            human_readable=True)
        mock_executor_response().execute_command.assert_called_with(
            ['gcloud', 'services', 'list', '--project', 'test_project'])


if __name__ == '__main__':
    unittest.main()
