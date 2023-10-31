# Copyright 2021 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module for AIPlatformPipelines API client utils."""

import json
from typing import Any, Dict

from google.cloud import storage


def load_json(path: str) -> Dict[str, Any]:
    """Loads data from a JSON document.

    Args:
      path: The path of the JSON document. It can be a local path or a GS URI.

    Returns:
      A deserialized Dict object representing the JSON document.
    """

    if path.startswith('gs://'):
        return _load_json_from_gs_uri(path)
    else:
        return _load_json_from_local_file(path)


def _load_json_from_gs_uri(uri: str) -> Dict[str, Any]:
    """Loads data from a JSON document referenced by a GS URI.

    Args:
      uri: The GCS URI of the JSON document.

    Returns:
      A deserialized Dict object representing the JSON document.

    Raises:
      google.cloud.exceptions.NotFound: If the blob is not found.
      json.decoder.JSONDecodeError: On JSON parsing problems.
      ValueError: If uri is not a valid gs URI.
    """
    storage_client = storage.Client()
    blob = storage.Blob.from_string(uri, storage_client)
    return json.loads(blob.download_as_bytes())


def _load_json_from_local_file(file_path: str) -> Dict[str, Any]:
    """Loads data from a JSON local file.

    Args:
      file_path: The local file path of the JSON document.

    Returns:
      A deserialized Dict object representing the JSON document.

    Raises:
      json.decoder.JSONDecodeError: On JSON parsing problems.
    """
    with open(file_path) as f:
        return json.load(f)
