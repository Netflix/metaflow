import sys
import types

import pytest
import requests

from metaflow.exception import MetaflowException
from metaflow.plugins.aws import aws_client
from metaflow.plugins.aws.aws_client import Boto3ClientProvider


@pytest.fixture(autouse=True)
def reset_sandbox_cache():
    aws_client.cached_aws_sandbox_creds = None
    yield
    aws_client.cached_aws_sandbox_creds = None


@pytest.fixture(autouse=True)
def mock_aws_sdk_modules(monkeypatch):
    class DummyClientError(Exception):
        pass

    class DummyConfig(object):
        def __init__(self, **kwargs):
            self.retries = kwargs.get("retries")

    class DummySession(object):
        def __init__(self, **kwargs):
            pass

        def client(self, module, **client_params):
            return {"module": module, "client_params": client_params}

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.session = types.SimpleNamespace(Session=DummySession)

    botocore_mod = types.ModuleType("botocore")
    botocore_exceptions = types.ModuleType("botocore.exceptions")
    botocore_exceptions.ClientError = DummyClientError
    botocore_config = types.ModuleType("botocore.config")
    botocore_config.Config = DummyConfig

    monkeypatch.setitem(sys.modules, "boto3", boto3_mod)
    monkeypatch.setitem(sys.modules, "botocore", botocore_mod)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exceptions)
    monkeypatch.setitem(sys.modules, "botocore.config", botocore_config)


def _configure_sandbox(monkeypatch, endpoint):
    import metaflow.metaflow_config as config

    monkeypatch.setattr(config, "AWS_SANDBOX_ENABLED", True)
    monkeypatch.setattr(config, "AWS_SANDBOX_STS_ENDPOINT_URL", endpoint)
    monkeypatch.setattr(config, "AWS_SANDBOX_API_KEY", "test-api-key")


def test_sandbox_sts_connection_error_raises_metaflow_exception(monkeypatch):
    endpoint = "http://sandbox-sts-connection-error.local"
    _configure_sandbox(monkeypatch, endpoint)

    def raise_connection_error(*args, **kwargs):
        raise requests.exceptions.ConnectionError("connection refused")

    monkeypatch.setattr(requests, "get", raise_connection_error)

    with pytest.raises(MetaflowException) as exc:
        Boto3ClientProvider.get_client("s3")
    msg = str(exc.value)
    assert endpoint in msg
    assert "Failed to connect to AWS sandbox STS endpoint" in msg
    msg = str(exc.value)
    assert endpoint in msg
    assert "Timed out while fetching AWS sandbox STS credentials" in msg
def test_sandbox_sts_http_error_raises_metaflow_exception(monkeypatch):
    endpoint = "http://sandbox-sts-http-error.local"
    _configure_sandbox(monkeypatch, endpoint)

    def raise_http_error(*args, **kwargs):
        mock_resp = requests.models.Response()
        mock_resp.status_code = 500
        raise requests.exceptions.HTTPError(response=mock_resp)

    monkeypatch.setattr(requests, "get", raise_http_error)

    with pytest.raises(MetaflowException) as exc:
        Boto3ClientProvider.get_client("s3")
    msg = str(exc.value)
    assert endpoint in msg
    assert "Received an invalid response from AWS sandbox STS endpoint" in msg


def test_sandbox_sts_generic_request_exception_raises_metaflow_exception(monkeypatch):
    endpoint = "http://sandbox-sts-generic-error.local"
    _configure_sandbox(monkeypatch, endpoint)

    def raise_request_exception(*args, **kwargs):
        raise requests.exceptions.RequestException("some other error")

    monkeypatch.setattr(requests, "get", raise_request_exception)

    with pytest.raises(MetaflowException) as exc:
        Boto3ClientProvider.get_client("s3")
    msg = str(exc.value)
    assert endpoint in msg
    assert "Failed while requesting AWS sandbox STS credentials from endpoint" in msg
