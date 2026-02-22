import os
import sys
import types

import pytest

if sys.platform == "win32" and "fcntl" not in sys.modules:
    fcntl_stub = types.ModuleType("fcntl")
    fcntl_stub.F_SETFL = 0
    fcntl_stub.fcntl = lambda *args, **kwargs: 0
    sys.modules["fcntl"] = fcntl_stub
if sys.platform == "win32" and not hasattr(os, "O_NONBLOCK"):
    os.O_NONBLOCK = 0

from metaflow.plugins.kubernetes.kubernetes import KubernetesException
from metaflow.plugins.kubernetes.kubernetes_cli import (
    _apply_debug_settings,
    _build_step_entrypoint,
)


def test_build_step_entrypoint_without_debug():
    assert (
        _build_step_entrypoint(
            executable="python3",
            flow_filename="flow.py",
            debug=False,
            debug_port=5678,
            debug_listen_host="0.0.0.0",
        )
        == "python3 -u flow.py"
    )


def test_build_step_entrypoint_with_debug():
    assert (
        _build_step_entrypoint(
            executable="python3",
            flow_filename="flow.py",
            debug=True,
            debug_port=5678,
            debug_listen_host="0.0.0.0",
        )
        == "python3 -u -m debugpy --listen 0.0.0.0:5678 --wait-for-client flow.py"
    )


def test_build_step_entrypoint_quotes_debug_host():
    assert (
        _build_step_entrypoint(
            executable="python3",
            flow_filename="flow.py",
            debug=True,
            debug_port=5678,
            debug_listen_host="0.0.0.0;rm -rf /",
        )
        == "python3 -u -m debugpy --listen '0.0.0.0;rm -rf /':5678 --wait-for-client flow.py"
    )


def test_apply_debug_settings_without_debug():
    assert _apply_debug_settings(
        debug=False,
        debug_port=5678,
        debug_listen_host="0.0.0.0",
        port=1234,
        num_parallel=None,
    ) == (1234, None)


def test_apply_debug_settings_sets_default_port():
    assert _apply_debug_settings(
        debug=True,
        debug_port=5678,
        debug_listen_host="0.0.0.0",
        port=None,
        num_parallel=None,
    ) == (5678, 5678)


def test_apply_debug_settings_accepts_matching_explicit_port():
    assert _apply_debug_settings(
        debug=True,
        debug_port=5678,
        debug_listen_host="0.0.0.0",
        port=5678,
        num_parallel=None,
    ) == (5678, 5678)


@pytest.mark.parametrize("debug_port", [None, 0, -1, 65536, "abc"])
def test_apply_debug_settings_rejects_invalid_port(debug_port):
    with pytest.raises(KubernetesException):
        _apply_debug_settings(
            debug=True,
            debug_port=debug_port,
            debug_listen_host="0.0.0.0",
            port=None,
            num_parallel=None,
        )


def test_apply_debug_settings_rejects_conflicting_explicit_port():
    with pytest.raises(KubernetesException):
        _apply_debug_settings(
            debug=True,
            debug_port=5678,
            debug_listen_host="0.0.0.0",
            port=9999,
            num_parallel=None,
        )


@pytest.mark.parametrize("port", [0, 65536, "abc"])
def test_apply_debug_settings_rejects_invalid_explicit_port(port):
    with pytest.raises(KubernetesException):
        _apply_debug_settings(
            debug=True,
            debug_port=5678,
            debug_listen_host="0.0.0.0",
            port=port,
            num_parallel=None,
        )


@pytest.mark.parametrize("debug_listen_host", [None, ""])
def test_apply_debug_settings_rejects_invalid_host(debug_listen_host):
    with pytest.raises(KubernetesException):
        _apply_debug_settings(
            debug=True,
            debug_port=5678,
            debug_listen_host=debug_listen_host,
            port=None,
            num_parallel=None,
        )


def test_apply_debug_settings_rejects_parallel_debug():
    with pytest.raises(KubernetesException):
        _apply_debug_settings(
            debug=True,
            debug_port=5678,
            debug_listen_host="0.0.0.0",
            port=None,
            num_parallel=2,
        )
