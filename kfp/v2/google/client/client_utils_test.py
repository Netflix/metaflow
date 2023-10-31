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
"""Tests for kfp.v2.google.client.client_utils."""

import json
import unittest
from unittest import mock

from google.cloud import storage

from kfp.v2.google.client import client_utils


class ClientUtilsTest(unittest.TestCase):

    @mock.patch.object(storage, 'Client', autospec=True)
    @mock.patch.object(storage.Blob, 'download_as_bytes', autospec=True)
    def test_load_json_from_gs_uri(self, mock_download_as_bytes,
                                   unused_storage_client):
        mock_download_as_bytes.return_value = b'{"key":"value"}'
        self.assertEqual({'key': 'value'},
                         client_utils.load_json('gs://bucket/path/to/blob'))

    @mock.patch('builtins.open', mock.mock_open(read_data='{"key":"value"}'))
    def test_load_json_from_local_file(self):
        self.assertEqual({'key': 'value'},
                         client_utils.load_json('/path/to/file'))

    @mock.patch.object(storage, 'Client', autospec=True)
    def test_load_json_from_gs_uri_with_non_gs_uri_should_fail(
            self, unused_storage_client):
        with self.assertRaisesRegex(ValueError, 'URI scheme must be gs'):
            client_utils._load_json_from_gs_uri(
                'https://storage.google.com/bucket/blob')

    @mock.patch.object(storage, 'Client', autospec=True)
    @mock.patch.object(storage.Blob, 'download_as_bytes', autospec=True)
    def test_load_json_from_gs_uri_with_invalid_json_should_fail(
            self, mock_download_as_bytes, unused_storage_client):
        mock_download_as_bytes.return_value = b'invalid-json'
        with self.assertRaises(json.decoder.JSONDecodeError):
            client_utils._load_json_from_gs_uri('gs://bucket/path/to/blob')

    @mock.patch('builtins.open', mock.mock_open(read_data='invalid-json'))
    def test_load_json_from_local_file_with_invalid_json_should_fail(self):
        with self.assertRaises(json.decoder.JSONDecodeError):
            client_utils._load_json_from_local_file('/path/to/file')


if __name__ == '__main__':
    unittest.main()
