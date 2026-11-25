import socket
import time

import pytest

from metaflow.plugins.metadata_providers import service
from metaflow.plugins.metadata_providers.service import ServiceMetadataProvider


@pytest.fixture
def silent_server():
    # Accepts connections but never replies.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(5)
        yield "http://127.0.0.1:%d" % sock.getsockname()[1]


@pytest.fixture
def provider_url(monkeypatch):
    def _set(url):
        monkeypatch.setattr(ServiceMetadataProvider, "_INFO", url)

    return _set


def test_request_to_unresponsive_service_times_out(
    monkeypatch, provider_url, silent_server
):
    monkeypatch.setattr(service, "SERVICE_REQUEST_TIMEOUT", 0.2)
    monkeypatch.setattr(service, "SERVICE_RETRY_COUNT", 1)
    provider_url(silent_server)

    start = time.time()
    with pytest.raises(Exception):
        ServiceMetadataProvider._request(None, "flows/x", "GET")

    assert time.time() - start < 5


@pytest.mark.parametrize(
    "call",
    [
        lambda: ServiceMetadataProvider._request(None, "flows/x", "GET"),
        lambda: ServiceMetadataProvider._version(None),
        lambda: ServiceMetadataProvider.compute_info("http://service"),
    ],
    ids=["request", "version", "compute_info"],
)
def test_keyboard_interrupt_is_not_retried(mocker, monkeypatch, provider_url, call):
    session = mocker.Mock()
    session.get.side_effect = KeyboardInterrupt()
    monkeypatch.setattr(ServiceMetadataProvider, "_session", session)
    sleep = mocker.patch.object(service.time, "sleep")
    provider_url("http://service")

    with pytest.raises(KeyboardInterrupt):
        call()

    assert session.get.call_count == 1
    sleep.assert_not_called()
