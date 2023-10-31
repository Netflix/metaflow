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
"""Functions for diagnostic data collection from development development."""

import enum
from kfp.cli.diagnose_me import utility


class Commands(enum.Enum):
    """Enum for gcloud and gsutil commands."""
    PIP3_LIST = 1
    PYTHON3_PIP_LIST = 2
    PIP3_VERSION = 3
    PYHYON3_PIP_VERSION = 4
    WHICH_PYHYON3 = 5
    WHICH_PIP3 = 6


_command_string = {
    Commands.PIP3_LIST: 'pip3 list',
    Commands.PYTHON3_PIP_LIST: 'python3 -m pip list',
    Commands.PIP3_VERSION: 'pip3 -V',
    Commands.PYHYON3_PIP_VERSION: 'python3 -m pip -V',
    Commands.WHICH_PYHYON3: 'which python3',
    Commands.WHICH_PIP3: 'which pip3',
}


def get_dev_env_configuration(
        configuration: Commands,
        human_readable: bool = False) -> utility.ExecutorResponse:
    """Captures the specified environment configuration.

    Captures the developement environment configuration including PIP version and
    Phython version as specifeid by configuration

    Args:
      configuration: Commands for specific information to be retrieved
        - PIP3LIST: captures pip3 freeze results
        - PYTHON3PIPLIST: captuers python3 -m pip freeze results
        - PIP3VERSION: captuers pip3 -V results
        - PYHYON3PIPVERSION: captuers python3 -m pip -V results
      human_readable: If true all output will be in human readable form insted of
        Json.

    Returns:
      A utility.ExecutorResponse with the output results for the specified
      command.
    """
    command_list = _command_string[configuration].split(' ')
    if not human_readable and configuration not in (
            Commands.PIP3_VERSION,
            Commands.PYHYON3_PIP_VERSION,
            Commands.WHICH_PYHYON3,
            Commands.WHICH_PIP3,
    ):
        command_list.extend(['--format', 'json'])

    return utility.ExecutorResponse().execute_command(command_list)
