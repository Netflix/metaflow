import sys
import unittest
from unittest.mock import patch, MagicMock

# Ensure we can import metaflow
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from metaflow.plugins.metadata_providers.request_provider import (
    DefaultRequestProvider,
    TracingRequestProvider,
    MetaflowServiceRequestProvider,
)
import requests
from metaflow.plugins.metadata_providers.service import (
    _load_request_provider,
    ServiceMetadataProvider,
)
from metaflow.exception import MetaflowException


class DummyProvider:
    def request(self, method, url, base_headers, json=None):
        pass

    def close(self):
        pass


class TestServiceRequestProvider(unittest.TestCase):
    def test_load_default_provider(self):
        provider = _load_request_provider("default")
        self.assertIsInstance(provider, DefaultRequestProvider)

    @patch("importlib.import_module")
    def test_load_custom_provider(self, mock_import_module):
        mock_module = MagicMock()
        mock_module.DummyProvider = DummyProvider
        mock_import_module.return_value = mock_module

        provider = _load_request_provider("test_module.DummyProvider")
        self.assertIsInstance(provider, DummyProvider)
        mock_import_module.assert_called_once_with("test_module")

    @patch("importlib.import_module")
    def test_load_custom_provider_error(self, mock_import_module):
        mock_import_module.side_effect = ImportError("Module not found")
        with self.assertRaises(MetaflowException) as context:
            _load_request_provider("nonexistent.module.Class")
        self.assertIn(
            "Failed to load custom service request provider", str(context.exception)
        )

    @patch(
        "metaflow.plugins.metadata_providers.service.SERVICE_REQUEST_PROVIDER",
        "default",
    )
    def test_service_metadata_get_provider_singleton(self):
        # Reset provider cache before test, restore after to avoid cross-test pollution
        ServiceMetadataProvider._provider = None
        self.addCleanup(setattr, ServiceMetadataProvider, "_provider", None)

        provider1 = ServiceMetadataProvider._get_provider()
        provider2 = ServiceMetadataProvider._get_provider()

        self.assertIsInstance(provider1, DefaultRequestProvider)
        self.assertIs(provider1, provider2)

    @patch("requests.Session.request")
    def test_default_request_provider_delegation(self, mock_session_request):
        mock_response = MagicMock()
        mock_session_request.return_value = mock_response

        provider = DefaultRequestProvider()

        headers = {"Authorization": "Bearer test"}
        res = provider.request("GET", "http://test.com", headers, json={"data": 1})

        mock_session_request.assert_called_once_with(
            method="GET", url="http://test.com", headers=headers, json={"data": 1}
        )
        self.assertEqual(res, mock_response)

    @patch("sys.stderr.write")
    @patch("requests.Session.request")
    def test_tracing_request_provider_success(
        self, mock_session_request, mock_stderr_write
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session_request.return_value = mock_response

        provider = TracingRequestProvider()

        headers = {"Authorization": "Bearer test"}
        res = provider.request("GET", "http://test.com", headers, json={"data": 1})

        mock_session_request.assert_called_once_with(
            method="GET", url="http://test.com", headers=headers, json={"data": 1}
        )
        self.assertEqual(res, mock_response)
        
        mock_stderr_write.assert_called_once()
        log_message = mock_stderr_write.call_args[0][0]
        self.assertRegex(
            log_message, r"\[METAFLOW_TRACE\] GET    http://test\.com -> 200 \(\d+\.\d+ms\)\n"
        )

    @patch("sys.stderr.write")
    @patch("requests.Session.request")
    def test_tracing_request_provider_error(
        self, mock_session_request, mock_stderr_write
    ):
        mock_session_request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        provider = TracingRequestProvider()

        headers = {"Authorization": "Bearer test"}
        with self.assertRaises(requests.exceptions.ConnectionError):
            provider.request("POST", "http://test.com", headers, json={"data": 1})

        mock_session_request.assert_called_once_with(
            method="POST", url="http://test.com", headers=headers, json={"data": 1}
        )
        
        mock_stderr_write.assert_called_once()
        log_message = mock_stderr_write.call_args[0][0]
        self.assertRegex(
            log_message,
            r"\[METAFLOW_TRACE\] POST   http://test\.com -> ERROR: ConnectionError \(\d+\.\d+ms\)\n",
        )

if __name__ == "__main__":
    unittest.main()
