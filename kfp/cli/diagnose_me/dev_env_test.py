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
"""Integration tests for diagnose_me.dev_env."""

from typing import Text
import unittest
from unittest import mock
from kfp.cli.diagnose_me import dev_env
from kfp.cli.diagnose_me import utility


class DevEnvTest(unittest.TestCase):

    def test_Commands(self):
        """Verify commands are formaated properly."""
        for command in dev_env.Commands:
            self.assertIsInstance(dev_env._command_string[command], Text)
            self.assertNotIn('\t', dev_env._command_string[command])
            self.assertNotIn('\n', dev_env._command_string[command])

    @mock.patch.object(utility, 'ExecutorResponse', autospec=True)
    def test_dev_env_configuration(self, mock_executor_response):
        """Tests dev_env command execution."""
        dev_env.get_dev_env_configuration(dev_env.Commands.PIP3_LIST)
        mock_executor_response().execute_command.assert_called_with(
            ['pip3', 'list', '--format', 'json'])

    @mock.patch.object(utility, 'ExecutorResponse', autospec=True)
    def test_dev_env_configuration_human_readable(self, mock_executor_response):
        """Tests dev_env command execution."""
        dev_env.get_dev_env_configuration(
            dev_env.Commands.PIP3_LIST, human_readable=True)
        mock_executor_response().execute_command.assert_called_with(
            ['pip3', 'list'])

    @mock.patch.object(utility, 'ExecutorResponse', autospec=True)
    def test_dev_env_configuration_version(self, mock_executor_response):
        """Tests dev_env command execution."""
        # human readable = false should not set format flag for version calls
        dev_env.get_dev_env_configuration(
            dev_env.Commands.PIP3_VERSION, human_readable=False)
        mock_executor_response().execute_command.assert_called_with(
            ['pip3', '-V'])
        dev_env.get_dev_env_configuration(
            dev_env.Commands.PYHYON3_PIP_VERSION, human_readable=False)
        mock_executor_response().execute_command.assert_called_with(
            ['python3', '-m', 'pip', '-V'])


if __name__ == '__main__':
    unittest.main()
