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


def use_aws_secret(secret_name='aws-secret',
                   aws_access_key_id_name='AWS_ACCESS_KEY_ID',
                   aws_secret_access_key_name='AWS_SECRET_ACCESS_KEY',
                   aws_region=None):
    """An operator that configures the container to use AWS credentials.

    AWS doesn't create secret along with kubeflow deployment and it requires users
    to manually create credential secret with proper permissions.

    ::

        apiVersion: v1
        kind: Secret
        metadata:
          name: aws-secret
        type: Opaque
        data:
          AWS_ACCESS_KEY_ID: BASE64_YOUR_AWS_ACCESS_KEY_ID
          AWS_SECRET_ACCESS_KEY: BASE64_YOUR_AWS_SECRET_ACCESS_KEY
    """

    def _use_aws_secret(task):
        from kubernetes import client as k8s_client
        task.container \
            .add_env_variable(
                k8s_client.V1EnvVar(
                    name='AWS_ACCESS_KEY_ID',
                    value_from=k8s_client.V1EnvVarSource(
                        secret_key_ref=k8s_client.V1SecretKeySelector(
                            name=secret_name,
                            key=aws_access_key_id_name
                        )
                    )
                )
            ) \
            .add_env_variable(
                k8s_client.V1EnvVar(
                    name='AWS_SECRET_ACCESS_KEY',
                    value_from=k8s_client.V1EnvVarSource(
                        secret_key_ref=k8s_client.V1SecretKeySelector(
                            name=secret_name,
                            key=aws_secret_access_key_name
                        )
                    )
                )
            )

        if aws_region:
            task.container \
                .add_env_variable(
                    k8s_client.V1EnvVar(
                        name='AWS_REGION',
                        value=aws_region
                    )
                )
        return task

    return _use_aws_secret
