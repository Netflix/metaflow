# Copyright 2019 The Kubeflow Authors
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

import random
import string


def use_secret(secret_name: str,
               secret_volume_mount_path: str,
               env_variable: str = None,
               secret_file_path_in_volume: str = None):
    """An operator that configures the container to use a secret.

       This assumes that the secret is created and availabel in the k8s cluster.

    Keyword Arguments:
        secret_name {String} -- [Required] The k8s secret name.
        secret_volume_mount_path {String} -- [Required] The path to the secret that is mounted.
        env_variable {String} -- Env variable pointing to the mounted secret file. Requires both the env_variable and secret_file_path_in_volume to be defined.
                                 The value is the path to the secret.
        secret_file_path_in_volume {String} -- The path to the secret in the volume. This will be the value of env_variable.
                                 Both env_variable and secret_file_path_in_volume needs to be set if any env variable should be created.

    Raises:
        ValueError: If not the necessary variables (secret_name, volume_name", secret_volume_mount_path) are supplied.
                    Or only one of  env_variable and secret_file_path_in_volume are supplied

    Returns:
        [ContainerOperator] -- Returns the container operator after it has been modified.
    """

    secret_name = str(secret_name)
    if '{{' in secret_name:
        volume_name = ''.join(
            random.choices(string.ascii_lowercase + string.digits,
                           k=10)) + "_volume"
    else:
        volume_name = secret_name
    for param, param_name in zip([secret_name, secret_volume_mount_path],
                                 ["secret_name", "secret_volume_mount_path"]):
        if param == "":
            raise ValueError("The '{}' must not be empty".format(param_name))
    if bool(env_variable) != bool(secret_file_path_in_volume):
        raise ValueError(
            "Both {} and {} needs to be supplied together or not at all".format(
                env_variable, secret_file_path_in_volume))

    def _use_secret(task):
        import os
        from kubernetes import client as k8s_client
        task = task.add_volume(
            k8s_client.V1Volume(
                name=volume_name,
                secret=k8s_client.V1SecretVolumeSource(
                    secret_name=secret_name))).add_volume_mount(
                        k8s_client.V1VolumeMount(
                            name=volume_name,
                            mount_path=secret_volume_mount_path))
        if env_variable:
            task.container.add_env_variable(
                k8s_client.V1EnvVar(
                    name=env_variable,
                    value=os.path.join(secret_volume_mount_path,
                                       secret_file_path_in_volume),
                ))
        return task

    return _use_secret
