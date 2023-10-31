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
"""Functions for collecting GCP related environment configurations."""

import enum
from typing import List, Text, Optional
from kfp.cli.diagnose_me import utility


class Commands(enum.Enum):
    """Enum for gcloud and gsutil commands."""
    GET_APIS = 1
    GET_CONTAINER_CLUSTERS = 2
    GET_CONTAINER_IMAGES = 3
    GET_DISKS = 4
    GET_GCLOUD_DEFAULT = 5
    GET_NETWORKS = 6
    GET_QUOTAS = 7
    GET_SCOPES = 8
    GET_SERVICE_ACCOUNTS = 9
    GET_STORAGE_BUCKETS = 10
    GET_GCLOUD_VERSION = 11
    GET_AUTH_LIST = 12


_command_string = {
    Commands.GET_APIS: 'services list',
    Commands.GET_CONTAINER_CLUSTERS: 'container clusters list',
    Commands.GET_CONTAINER_IMAGES: 'container images list',
    Commands.GET_DISKS: 'compute disks list',
    Commands.GET_GCLOUD_DEFAULT: 'config list --all',
    Commands.GET_NETWORKS: 'compute networks list',
    Commands.GET_QUOTAS: 'compute regions list',
    Commands.GET_SCOPES: 'compute instances list',
    Commands.GET_SERVICE_ACCOUNTS: 'iam service-accounts list',
    Commands.GET_STORAGE_BUCKETS: 'ls',
    Commands.GET_GCLOUD_VERSION: 'version',
    Commands.GET_AUTH_LIST: 'auth list',
}


def execute_gcloud_command(
        gcloud_command_list: List[Text],
        project_id: Optional[Text] = None,
        human_readable: Optional[bool] = False) -> utility.ExecutorResponse:
    """Function for invoking gcloud command.

    Args:
      gcloud_command_list: a command string list to be past to gcloud example
        format is ['config', 'list', '--all']
      project_id: specificies the project to run the commands against if not
        provided provided will use gcloud default project if one is configured
        otherwise will return an error message.
      human_readable: If false sets parameter --format json for all calls,
        otherwie output will be in human readable format.

    Returns:
      utility.ExecutorResponse with outputs from stdout,stderr and execution code.
    """
    command_list = ['gcloud']
    command_list.extend(gcloud_command_list)
    if not human_readable:
        command_list.extend(['--format', 'json'])

    if project_id is not None:
        command_list.extend(['--project', project_id])

    return utility.ExecutorResponse().execute_command(command_list)


def execute_gsutil_command(
        gsutil_command_list: List[Text],
        project_id: Optional[Text] = None) -> utility.ExecutorResponse:
    """Function for invoking gsutil command.

    This function takes in a gsutil parameter list and returns the results as a
    list of dictionaries.
    Args:
      gsutil_command_list: a command string list to be past to gsutil example
        format is ['config', 'list', '--all']
      project_id: specific project to check the QUOTASs for,if no project id is
        provided will use gcloud default project if one is configured otherwise
        will return an erro massage.

    Returns:
      utility.ExecutorResponse with outputs from stdout,stderr and execution code.
    """
    command_list = ['gsutil']
    command_list.extend(gsutil_command_list)

    if project_id is not None:
        command_list.extend(['-p', project_id])

    return utility.ExecutorResponse().execute_command(command_list)


def get_gcp_configuration(
        configuration: Commands,
        project_id: Optional[Text] = None,
        human_readable: Optional[bool] = False) -> utility.ExecutorResponse:
    """Captures the specified environment configuration.

    Captures the environment configuration for the specified setting such as
    NETWORKSing configuration, project QUOTASs, etc.

    Args:
      configuration: Commands for specific information to be retrieved
        - APIS:  Captures a complete list of enabled APISs and their configuration
          details under the specified project.
        - CONTAINER_CLUSTERS: List all visible k8 clusters under the project.
        - CONTAINER_IMAGES: List of all container images under the project
          container repo.
        - DISKS: List of storage allocated by the project including notebook
          instances as well as k8 pds with corresponding state.
        - GCLOUD_DEFAULT:  Environment default configuration for gcloud
        - NETWORKS: List all NETWORKSs and their configuration under the project.
        - QUOTAS:  Captures a complete list of  QUOTASs for project per
          region,returns the results as a list of dictionaries.
        - SCOPES: list of SCOPESs for each compute resources in the project.
        - SERVICE_ACCOUNTS: List of all service accounts that are enabled under
          this project.
        - STORAGE_BUCKETS: list of buckets and corresponding access information.
      project_id: specific project to check the QUOTASs for,if no project id is
        provided will use gcloud default project if one is configured otherwise
        will return an error message.
      human_readable: If true all output will be in human readable form insted of
        Json.

    Returns:
      A utility.ExecutorResponse with the output results for the specified
      command.
    """
    # storage bucket call requires execute_gsutil_command
    if configuration is Commands.GET_STORAGE_BUCKETS:
        return execute_gsutil_command(
            [_command_string[Commands.GET_STORAGE_BUCKETS]], project_id)

    # For all other cases can execute the command directly
    return execute_gcloud_command(_command_string[configuration].split(' '),
                                  project_id, human_readable)
