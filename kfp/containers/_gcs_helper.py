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

import os
import pathlib
import tempfile


class GCSHelper(object):
    """GCSHelper manages the connection with the GCS storage."""

    @staticmethod
    def get_blob_from_gcs_uri(gcs_path):
        """
    Args:
      gcs_path (str) : gcs blob path
    Returns:
      gcs_blob: gcs blob object(https://github.com/googleapis/google-cloud-python/blob/5c9bb42cb3c9250131cfeef6e0bafe8f4b7c139f/storage/google/cloud/storage/blob.py#L105)
    """
        from google.cloud import storage
        pure_path = pathlib.PurePath(gcs_path)
        gcs_bucket = pure_path.parts[1]
        gcs_blob = '/'.join(pure_path.parts[2:])
        client = storage.Client()
        bucket = client.get_bucket(gcs_bucket)
        blob = bucket.blob(gcs_blob)
        return blob

    @staticmethod
    def upload_gcs_file(local_path, gcs_path):
        """
    Args:
      local_path (str): local file path
      gcs_path (str) : gcs blob path
    """
        blob = GCSHelper.get_blob_from_gcs_uri(gcs_path)
        blob.upload_from_filename(local_path)

    @staticmethod
    def write_to_gcs_path(path: str, content: str) -> None:
        """Writes serialized content to a GCS location.

        Args:
          path: GCS path to write to.
          content: The content to be written.
        """
        fd, temp_path = tempfile.mkstemp()
        try:
            with os.fdopen(fd, 'w') as tmp:
                tmp.write(content)

            if not GCSHelper.get_blob_from_gcs_uri(path):
                pure_path = pathlib.PurePath(path)
                gcs_bucket = pure_path.parts[1]
                GCSHelper.create_gcs_bucket_if_not_exist(gcs_bucket)

            GCSHelper.upload_gcs_file(temp_path, path)
        finally:
            os.remove(temp_path)

    @staticmethod
    def remove_gcs_blob(gcs_path):
        """
    Args:
      gcs_path (str) : gcs blob path
    """
        blob = GCSHelper.get_blob_from_gcs_uri(gcs_path)
        blob.delete()

    @staticmethod
    def download_gcs_blob(local_path, gcs_path):
        """
    Args:
      local_path (str): local file path
      gcs_path (str) : gcs blob path
    """
        blob = GCSHelper.get_blob_from_gcs_uri(gcs_path)
        blob.download_to_filename(local_path)

    @staticmethod
    def read_from_gcs_path(gcs_path: str) -> str:
        """Reads the content of a file hosted on GCS."""
        fd, temp_path = tempfile.mkstemp()
        try:
            GCSHelper.download_gcs_blob(temp_path, gcs_path)
            with os.fdopen(fd, 'r') as tmp:
                result = tmp.read()
        finally:
            os.remove(temp_path)
        return result

    @staticmethod
    def create_gcs_bucket_if_not_exist(gcs_bucket):
        """
    Args:
      gcs_bucket (str) : gcs bucket name
    """
        from google.cloud import storage
        from google.cloud.exceptions import NotFound
        client = storage.Client()
        try:
            client.get_bucket(gcs_bucket)
        except NotFound:
            client.create_bucket(gcs_bucket)
