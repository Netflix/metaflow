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

from ._tokencredentialsbase import TokenCredentialsBase, read_token_from_file
from ._satvolumecredentials import ServiceAccountTokenVolumeCredentials

KF_PIPELINES_SA_TOKEN_ENV = "KF_PIPELINES_SA_TOKEN_PATH"
KF_PIPELINES_SA_TOKEN_PATH = "/var/run/secrets/kubeflow/pipelines/token"
