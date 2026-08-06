import pytest

from metaflow.metaflow_config import UV_VERSION
from metaflow.plugins.uv.bootstrap import (
    _UV_BASE_URL,
    _UV_TARGET_MAP,
    _get_uv_download_url,
)

_SYSTEM = "metaflow.plugins.uv.bootstrap.platform.system"
_MACHINE = "metaflow.plugins.uv.bootstrap.platform.machine"


_PLATFORM_CASES = [
    ("Linux", "x86_64", "x86_64-unknown-linux-gnu"),
    ("Linux", "amd64", "x86_64-unknown-linux-gnu"),
    ("Linux", "aarch64", "aarch64-unknown-linux-gnu"),
    ("Linux", "arm64", "aarch64-unknown-linux-gnu"),
    ("Darwin", "x86_64", "x86_64-apple-darwin"),
    ("Darwin", "amd64", "x86_64-apple-darwin"),
    ("Darwin", "arm64", "aarch64-apple-darwin"),
    ("Darwin", "aarch64", "aarch64-apple-darwin"),
]


def _expected_url(version, target):
    return f"{_UV_BASE_URL}/{version}/uv-{target}.tar.gz"


def test_platform_cases_cover_target_map():
    tested = {
        (system.lower(), machine.lower()) for system, machine, _ in _PLATFORM_CASES
    }
    assert tested == set(_UV_TARGET_MAP.keys())


@pytest.mark.parametrize(
    "system, machine, target",
    _PLATFORM_CASES,
    ids=[f"{system}-{machine}" for system, machine, _ in _PLATFORM_CASES],
)
def test_platform_resolves_to_expected_target(mocker, system, machine, target):
    mocker.patch(_SYSTEM, return_value=system)
    mocker.patch(_MACHINE, return_value=machine)
    assert _get_uv_download_url() == _expected_url(UV_VERSION, target)


def test_version_argument(mocker):
    mocker.patch(_MACHINE, return_value="x86_64")
    mocker.patch(_SYSTEM, return_value="Linux")
    assert _get_uv_download_url(version="0.5.0") == _expected_url(
        "0.5.0", "x86_64-unknown-linux-gnu"
    )


def test_unsupported_architecture_raises(mocker):
    mocker.patch(_MACHINE, return_value="riscv64")
    mocker.patch(_SYSTEM, return_value="Linux")
    with pytest.raises(RuntimeError, match="linux/riscv64"):
        _get_uv_download_url()


def test_unsupported_os_raises(mocker):
    mocker.patch(_MACHINE, return_value="x86_64")
    mocker.patch(_SYSTEM, return_value="Windows")
    with pytest.raises(RuntimeError, match="windows/x86_64"):
        _get_uv_download_url()
