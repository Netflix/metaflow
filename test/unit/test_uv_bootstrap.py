import pytest
from unittest.mock import patch

from metaflow.plugins.uv.bootstrap import (
    _UV_BASE_URL,
    _UV_TARGET_MAP,
    _get_uv_download_url,
)
from metaflow.metaflow_config import UV_VERSION

_SYS = "metaflow.plugins.uv.bootstrap.platform.system"
_MACH = "metaflow.plugins.uv.bootstrap.platform.machine"


@pytest.fixture
def url():
    def _build(version, target):
        return f"{_UV_BASE_URL}/{version}/uv-{target}.tar.gz"

    return _build


@patch(_MACH, return_value="x86_64")
@patch(_SYS, return_value="Linux")
def test_linux_x86_64(_sys, _mach, url):
    assert _get_uv_download_url() == url(UV_VERSION, "x86_64-unknown-linux-gnu")


@patch(_MACH, return_value="aarch64")
@patch(_SYS, return_value="Linux")
def test_linux_aarch64(_sys, _mach, url):
    assert _get_uv_download_url() == url(UV_VERSION, "aarch64-unknown-linux-gnu")


@patch(_MACH, return_value="arm64")
@patch(_SYS, return_value="Linux")
def test_linux_arm64_alias(_sys, _mach, url):
    assert _get_uv_download_url() == url(UV_VERSION, "aarch64-unknown-linux-gnu")


@patch(_MACH, return_value="amd64")
@patch(_SYS, return_value="Linux")
def test_linux_amd64_alias(_sys, _mach, url):
    assert _get_uv_download_url() == url(UV_VERSION, "x86_64-unknown-linux-gnu")


@patch(_MACH, return_value="arm64")
@patch(_SYS, return_value="Darwin")
def test_darwin_arm64(_sys, _mach, url):
    assert _get_uv_download_url() == url(UV_VERSION, "aarch64-apple-darwin")


@patch(_MACH, return_value="x86_64")
@patch(_SYS, return_value="Darwin")
def test_darwin_x86_64(_sys, _mach, url):
    assert _get_uv_download_url() == url(UV_VERSION, "x86_64-apple-darwin")


@patch(_MACH, return_value="x86_64")
@patch(_SYS, return_value="Linux")
def test_custom_version_argument(_sys, _mach, url):
    assert _get_uv_download_url(version="0.5.0") == url(
        "0.5.0", "x86_64-unknown-linux-gnu"
    )


@patch(_MACH, return_value="riscv64")
@patch(_SYS, return_value="Linux")
def test_unsupported_architecture_raises(_sys, _mach):
    with pytest.raises(RuntimeError, match="linux/riscv64"):
        _get_uv_download_url()


@patch(_MACH, return_value="x86_64")
@patch(_SYS, return_value="Windows")
def test_unsupported_os_raises(_sys, _mach):
    with pytest.raises(RuntimeError, match="windows/x86_64"):
        _get_uv_download_url()


def test_target_map_completeness(url):
    for (system, machine), target in _UV_TARGET_MAP.items():
        with patch(_SYS, return_value=system.capitalize()):
            with patch(_MACH, return_value=machine):
                result = _get_uv_download_url()
                assert target in result
                assert result.startswith(_UV_BASE_URL)
                assert result.endswith(".tar.gz")
