from unittest.mock import MagicMock, patch

import pytest
import requests

import metaflow.metaflow_config as metaflow_config
from metaflow.exception import MetaflowException
from metaflow.plugins.aws import aws_client


class DummySession(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def client(self, module, **client_params):
        return {"module": module, "client_params": client_params, "creds": self.kwargs}


@pytest.fixture(autouse=True)
def reset_cached_creds():
    aws_client.cached_aws_sandbox_creds = None
    yield
    aws_client.cached_aws_sandbox_creds = None


def _enable_sandbox(monkeypatch):
    monkeypatch.setattr(metaflow_config, "AWS_SANDBOX_ENABLED", True)
    monkeypatch.setattr(
        metaflow_config, "AWS_SANDBOX_STS_ENDPOINT_URL", "http://sandbox.local"
    )
    monkeypatch.setattr(metaflow_config, "AWS_SANDBOX_API_KEY", "test-api-key")


def _fake_sandbox_creds():
    return {
        "aws_access_key_id": "test_key",
        "aws_secret_access_key": "test_secret",
        "aws_session_token": "test_token",
        "region_name": "us-east-1",
    }


def test_sandbox_sts_request_uses_timeout_tuple(monkeypatch):
    _enable_sandbox(monkeypatch)
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = _fake_sandbox_creds()

    with patch("requests.get", return_value=response) as mock_get:
        with patch("boto3.session.Session", DummySession):
            client = aws_client.Boto3ClientProvider.get_client("s3")

    assert client["module"] == "s3"
    assert mock_get.call_count == 1
    call_kwargs = mock_get.call_args.kwargs
    assert call_kwargs["timeout"] == (5, 30)
    assert call_kwargs["headers"] == {"x-api-key": "test-api-key"}
    assert mock_get.call_args.args[0] == "http://sandbox.local/auth/token"


def test_sandbox_timeout_raises_metaflowexception(monkeypatch):
    _enable_sandbox(monkeypatch)

    with patch("requests.get", side_effect=requests.exceptions.Timeout("timed out")):
        with pytest.raises(MetaflowException) as exc:
            aws_client.Boto3ClientProvider.get_client("s3")

    assert "Timed out connecting to AWS sandbox STS endpoint" in str(exc.value)
    assert "http://sandbox.local" in str(exc.value)


def test_sandbox_connection_error_raises_metaflowexception(monkeypatch):
    _enable_sandbox(monkeypatch)

    with patch(
        "requests.get",
        side_effect=requests.exceptions.ConnectionError("network unreachable"),
    ):
        with pytest.raises(MetaflowException) as exc:
            aws_client.Boto3ClientProvider.get_client("s3")

    assert "Could not connect to AWS sandbox STS endpoint" in str(exc.value)
    assert "http://sandbox.local" in str(exc.value)


def test_sandbox_http_error_still_wrapped(monkeypatch):
    _enable_sandbox(monkeypatch)
    response = MagicMock()
    response.raise_for_status.side_effect = requests.exceptions.HTTPError("403")

    with patch("requests.get", return_value=response):
        with pytest.raises(MetaflowException) as exc:
            aws_client.Boto3ClientProvider.get_client("s3")

    assert "HTTPError" in str(exc.value)


def test_cached_sandbox_creds_skip_network_call(monkeypatch):
    _enable_sandbox(monkeypatch)
    aws_client.cached_aws_sandbox_creds = _fake_sandbox_creds()

    with patch("requests.get") as mock_get:
        with patch("boto3.session.Session", DummySession):
            client = aws_client.Boto3ClientProvider.get_client("s3")

    assert client["creds"]["aws_access_key_id"] == "test_key"
    mock_get.assert_not_called()
