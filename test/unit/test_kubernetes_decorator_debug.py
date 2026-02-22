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
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator


class _DummyFlowDatastore(object):
    TYPE = "s3"


class _DummyDeco(object):
    def __init__(self, name):
        self.name = name
        self.attributes = {}


class _DummyCLIArgs(object):
    def __init__(self):
        self.commands = []
        self.command_args = []
        self.command_options = {}
        self.entrypoint = [None]


def _make_decorator(attributes):
    deco = KubernetesDecorator(attributes=attributes)
    deco.external_init()
    return deco


def _run_step_init(deco, decos, logger=None):
    if logger is None:
        logger = lambda *args, **kwargs: None
    deco.step_init(
        flow=None,
        graph=None,
        step="debug_step",
        decos=decos,
        environment=None,
        flow_datastore=_DummyFlowDatastore(),
        logger=logger,
    )


def test_kubernetes_decorator_rejects_parallel_debug():
    deco = _make_decorator({"debug": True, "debug_port": 5678})
    with pytest.raises(KubernetesException, match="@parallel"):
        _run_step_init(deco, [deco, _DummyDeco("parallel")])


def test_kubernetes_decorator_rejects_debug_port_mismatch():
    deco = _make_decorator({"debug": True, "debug_port": 5678, "port": 8080})
    with pytest.raises(KubernetesException, match="port must match debug_port"):
        _run_step_init(deco, [deco])


def test_kubernetes_decorator_rejects_invalid_explicit_port_in_debug_mode():
    deco = _make_decorator({"debug": True, "debug_port": 5678, "port": "abc"})
    with pytest.raises(KubernetesException, match="Invalid port value"):
        _run_step_init(deco, [deco])


def test_kubernetes_decorator_accepts_matching_explicit_port():
    deco = _make_decorator({"debug": True, "debug_port": 5678, "port": 5678})
    _run_step_init(deco, [deco])
    assert deco.attributes["port"] == 5678


def test_kubernetes_decorator_warns_for_retry_with_debug():
    logs = []
    deco = _make_decorator({"debug": True, "debug_port": 5678})
    _run_step_init(
        deco,
        [deco, _DummyDeco("retry")],
        logger=lambda msg, *args, **kwargs: logs.append(msg),
    )
    assert any("@retry is active" in msg for msg in logs)


def test_runtime_step_cli_omits_debug_connection_args_when_debug_disabled():
    deco = _make_decorator({"debug": False})
    cli_args = _DummyCLIArgs()
    deco.runtime_step_cli(
        cli_args=cli_args,
        retry_count=0,
        max_user_code_retries=0,
        ubf_context=None,
    )
    assert "debug_port" not in cli_args.command_options
    assert "debug_listen_host" not in cli_args.command_options


def test_runtime_step_cli_keeps_debug_connection_args_when_debug_enabled():
    deco = _make_decorator(
        {"debug": True, "debug_port": 5678, "debug_listen_host": "0.0.0.0"}
    )
    cli_args = _DummyCLIArgs()
    deco.runtime_step_cli(
        cli_args=cli_args,
        retry_count=0,
        max_user_code_retries=0,
        ubf_context=None,
    )
    assert cli_args.command_options["debug_port"] == 5678
    assert cli_args.command_options["debug_listen_host"] == "0.0.0.0"
