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


def use_azure_secret(secret_name='azcreds'):
    """An operator that configures the container to use Azure user credentials.

    The azcreds secret is created as part of the kubeflow deployment that
    stores the client ID and secrets for the kubeflow azure service principal.

    With this service principal, the container has a range of Azure APIs to access to.
    """

    def _use_azure_secret(task):
        from kubernetes import client as k8s_client
        (task.container.add_env_variable(
            k8s_client.V1EnvVar(
                name='AZ_SUBSCRIPTION_ID',
                value_from=k8s_client.V1EnvVarSource(
                    secret_key_ref=k8s_client.V1SecretKeySelector(
                        name=secret_name, key='AZ_SUBSCRIPTION_ID')))
        ).add_env_variable(
            k8s_client.V1EnvVar(
                name='AZ_TENANT_ID',
                value_from=k8s_client.V1EnvVarSource(
                    secret_key_ref=k8s_client.V1SecretKeySelector(
                        name=secret_name, key='AZ_TENANT_ID')))
        ).add_env_variable(
            k8s_client.V1EnvVar(
                name='AZ_CLIENT_ID',
                value_from=k8s_client.V1EnvVarSource(
                    secret_key_ref=k8s_client.V1SecretKeySelector(
                        name=secret_name, key='AZ_CLIENT_ID'))))
         .add_env_variable(
             k8s_client.V1EnvVar(
                 name='AZ_CLIENT_SECRET',
                 value_from=k8s_client.V1EnvVarSource(
                     secret_key_ref=k8s_client.V1SecretKeySelector(
                         name=secret_name, key='AZ_CLIENT_SECRET')))))
        return task

    return _use_azure_secret
