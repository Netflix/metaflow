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
"""Tests for diagnose_me.kubernetes_cluster."""

from typing import Text
import unittest
from unittest import mock
from kfp.cli.diagnose_me import kubernetes_cluster as dkc
from kfp.cli.diagnose_me import utility


class KubernetesClusterTest(unittest.TestCase):

    @mock.patch.object(dkc, 'execute_kubectl_command', autospec=True)
    def test_project_configuration_gcloud(self, mock_execute_kubectl_command):
        """Tests gcloud commands."""
        dkc.get_kubectl_configuration(dkc.Commands.GET_PODS)
        mock_execute_kubectl_command.assert_called_once_with(
            ['get', 'pods', '--all-namespaces'], human_readable=False)

        dkc.get_kubectl_configuration(dkc.Commands.GET_CONFIGURED_CONTEXT)
        mock_execute_kubectl_command.assert_called_with(['config', 'view'],
                                                        human_readable=False)

        dkc.get_kubectl_configuration(dkc.Commands.GET_KUBECTL_VERSION)
        mock_execute_kubectl_command.assert_called_with(['version'],
                                                        human_readable=False)

        dkc.get_kubectl_configuration(
            dkc.Commands.GET_PODS, kubernetes_context='test_context')
        mock_execute_kubectl_command.assert_called_with(
            ['get', 'pods', '--context', 'test_context', '--all-namespaces'],
            human_readable=False)

        dkc.get_kubectl_configuration(
            dkc.Commands.GET_PODS, kubernetes_context='test_context')
        mock_execute_kubectl_command.assert_called_with(
            ['get', 'pods', '--context', 'test_context', '--all-namespaces'],
            human_readable=False)

    def test_Commands(self):
        """Verify commands are formaated properly."""
        for command in dkc.Commands:
            self.assertIsInstance(dkc._command_string[command], Text)
            self.assertNotIn('\t', dkc._command_string[command])
            self.assertNotIn('\n', dkc._command_string[command])

    @mock.patch.object(utility, 'ExecutorResponse', autospec=True)
    def test_execute_kubectl_command(self, mock_executor_response):
        """Test execute_gsutil_command."""
        dkc.execute_kubectl_command(
            [dkc._command_string[dkc.Commands.GET_KUBECTL_VERSION]])
        mock_executor_response().execute_command.assert_called_once_with(
            ['kubectl', 'version', '-o', 'json'])

        dkc.execute_kubectl_command(
            [dkc._command_string[dkc.Commands.GET_KUBECTL_VERSION]],
            human_readable=True)
        mock_executor_response().execute_command.assert_called_with(
            ['kubectl', 'version'])


if __name__ == '__main__':
    unittest.main()
