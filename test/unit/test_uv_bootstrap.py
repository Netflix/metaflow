import os
import sys
import types

import pytest

if not hasattr(os, "O_NONBLOCK"):
    os.O_NONBLOCK = 0

if "fcntl" not in sys.modules:
    fake_fcntl = types.ModuleType("fcntl")
    fake_fcntl.F_SETFL = 0
    fake_fcntl.fcntl = lambda *args, **kwargs: None
    sys.modules["fcntl"] = fake_fcntl

from metaflow.plugins.uv.bootstrap import build_uv_sync_cmd, get_uv_download_info


def test_get_uv_download_info_linux_x86_64():
    info = get_uv_download_info(version="0.9.21", system="Linux", machine="x86_64")

    assert info["archive_type"] == "tar.gz"
    assert info["executable_name"] == "uv"
    assert info["url"].endswith(
        "/0.9.21/uv-x86_64-unknown-linux-gnu.tar.gz"
    )


def test_get_uv_download_info_darwin_arm64():
    info = get_uv_download_info(version="0.9.21", system="Darwin", machine="arm64")

    assert info["archive_type"] == "tar.gz"
    assert info["executable_name"] == "uv"
    assert info["url"].endswith("/0.9.21/uv-aarch64-apple-darwin.tar.gz")


def test_get_uv_download_info_windows_amd64():
    info = get_uv_download_info(version="0.9.21", system="Windows", machine="AMD64")

    assert info["archive_type"] == "zip"
    assert info["executable_name"] == "uv.exe"
    assert info["url"].endswith("/0.9.21/uv-x86_64-pc-windows-msvc.zip")


def test_get_uv_download_info_raises_for_unsupported_platform():
    with pytest.raises(RuntimeError):
        get_uv_download_info(system="FreeBSD", machine="x86_64")


def test_build_uv_sync_cmd_uses_portable_command_chain():
    cmd = build_uv_sync_cmd(
        dependencies=["boto3", "requests"],
        skip_packages=["metaflow", "my-extension"],
    )

    assert "set -e;" not in cmd
    assert "uv sync --frozen --no-dev" in cmd
    assert "--no-install-package metaflow" in cmd
    assert "--no-install-package my-extension" in cmd
    assert "&& uv pip install boto3 requests --strict" in cmd
