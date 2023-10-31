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
"""Functions for collecting diagnostic information on Kubernetes cluster."""

import enum
from typing import List, Text
from kfp.cli.diagnose_me import utility


class Commands(enum.Enum):
    """Enum for kubernetes commands."""
    GET_CONFIGURED_CONTEXT = 1
    GET_PODS = 2
    GET_PVCS = 3
    GET_PVS = 4
    GET_SECRETS = 5
    GET_SERVICES = 6
    GET_KUBECTL_VERSION = 7
    GET_CONFIG_MAPS = 8


_command_string = {
    Commands.GET_CONFIGURED_CONTEXT: 'config view',
    Commands.GET_PODS: 'get pods',
    Commands.GET_PVCS: 'get pvc',
    Commands.GET_PVS: 'get pv',
    Commands.GET_SECRETS: 'get secrets',
    Commands.GET_SERVICES: 'get services',
    Commands.GET_KUBECTL_VERSION: 'version',
    Commands.GET_CONFIG_MAPS: 'get configmaps',
}


def execute_kubectl_command(
        kubectl_command_list: List[Text],
        human_readable: bool = False) -> utility.ExecutorResponse:
    """Invokes  the kubectl command.

    Args:
      kubectl_command_list: a command string list to be past to kubectl example
        format is ['config', 'view']
      human_readable: If false sets parameter -o json for all calls, otherwie
        output will be in human readable format.

    Returns:
      utility.ExecutorResponse with outputs from stdout,stderr and execution code.
    """
    command_list = ['kubectl']
    command_list.extend(kubectl_command_list)
    if not human_readable:
        command_list.extend(['-o', 'json'])

    return utility.ExecutorResponse().execute_command(command_list)


def get_kubectl_configuration(
        configuration: Commands,
        kubernetes_context: Text = None,
        namespace: Text = None,
        human_readable: bool = False) -> utility.ExecutorResponse:
    """Captures the specified environment configuration.

    Captures the environment state for the specified setting such as current
    context, active pods, etc and returns it in as a dictionary format. if no
    context is specified the system will use the current_context or error out of
    none is specified.

    Args:
      configuration:
        - K8_CONFIGURED_CONTEXT: returns all k8 configuration available in the
          current env including current_context.
        - PODS: returns all pods and their status details.
        - PVCS: returns all PersistentVolumeClaim and their status details.
        - SECRETS: returns all accessible k8 secrests.
        - PVS: returns all PersistentVolume and their status details.
        - SERVICES: returns all services and their status details.
      kubernetes_context: Context to use to retrieve cluster specific commands, if
        set to None calls will rely on current_context configured.
      namespace: default name space to be used for the commaand, if not specifeid
        --all-namespaces will be used.
      human_readable: If true all output will be in human readable form insted of
        Json.

    Returns:
      A list of dictionaries matching gcloud / gsutil output for the specified
      configuration,or an error message if any occurs during execution.
    """

    if configuration in (Commands.GET_CONFIGURED_CONTEXT,
                         Commands.GET_KUBECTL_VERSION):
        return execute_kubectl_command(
            (_command_string[configuration]).split(' '), human_readable)

    execution_command = _command_string[configuration].split(' ')
    if kubernetes_context:
        execution_command.extend(['--context', kubernetes_context])
    if namespace:
        execution_command.extend(['--namespace', namespace])
    else:
        execution_command.extend(['--all-namespaces'])

    return execute_kubectl_command(execution_command, human_readable)


def _get_kfp_runtime() -> Text:
    """Captures the current version of kpf in k8 cluster.

    Returns:
      Returns the run-time version of kfp in as a string.
    """
    # TODO(chavoshi) needs to be implemented.
    raise NotImplementedError
