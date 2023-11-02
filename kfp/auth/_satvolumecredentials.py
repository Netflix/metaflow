# Copyright 2021 Arrikto Inc.
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

import os
import logging

from kubernetes.client import configuration

from kfp import auth


class ServiceAccountTokenVolumeCredentials(auth.TokenCredentialsBase):
    """Audience-bound ServiceAccountToken in the local filesystem.

    This is a credentials interface for audience-bound ServiceAccountTokens
    found in the local filesystem, that get refreshed by the kubelet.

    The constructor of the class expects a filesystem path.
    If not provided, it uses the path stored in the environment variable
    defined in ``auth.KF_PIPELINES_SA_TOKEN_ENV``.
    If the environment variable is also empty, it falls back to the path
    specified in ``auth.KF_PIPELINES_SA_TOKEN_PATH``.

    This method of authentication is meant for use inside a Kubernetes cluster.

    Relevant documentation:
    https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#service-account-token-volume-projection
    """

    def __init__(self, path=None):
        self._token_path = (
            path or os.getenv(auth.KF_PIPELINES_SA_TOKEN_ENV) or
            auth.KF_PIPELINES_SA_TOKEN_PATH)

    def _get_token(self):
        token = None
        try:
            token = auth.read_token_from_file(self._token_path)
        except OSError as e:
            logging.error("Failed to read a token from file '%s' (%s).",
                          self._token_path, str(e))
            raise
        return token

    def refresh_api_key_hook(self, config: configuration.Configuration):
        """Refresh the api key.

        This is a helper function for registering token refresh with swagger
        generated clients.

        Args:
            config (kubernetes.client.configuration.Configuration):
                The configuration object that the client uses.

                The Configuration object of the kubernetes client's is the same
                with kfp_server_api.configuration.Configuration.
        """
        config.api_key["authorization"] = self._get_token()
