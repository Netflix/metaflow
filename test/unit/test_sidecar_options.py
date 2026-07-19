import json
import subprocess
from types import SimpleNamespace

import pytest

from metaflow._vendor.click.testing import CliRunner
from metaflow.mflog.save_logs_periodically import SaveLogsPeriodicallySidecar
from metaflow.mflog.mflog import parse
from metaflow.sidecar import Sidecar
from metaflow.sidecar.sidecar_subprocess import SidecarSubProcess
from metaflow.sidecar.sidecar_worker import (
    deserialize_options,
    instantiate_worker,
    main as sidecar_worker_main,
)


def test_sidecar_passes_detached_options_to_subprocess(mocker):
    options = {"enabled": True, "nested": {"value": 1}}
    subprocess = mocker.patch("metaflow.sidecar.sidecar.SidecarSubProcess")
    sidecar = Sidecar("heartbeat", options=options)
    options["nested"]["value"] = 2

    sidecar.start()

    subprocess.assert_called_once_with(
        "heartbeat", options={"enabled": True, "nested": {"value": 1}}
    )


@pytest.mark.parametrize(
    "options",
    ["enabled", {"callback": lambda: None}],
    ids=["not-a-dictionary", "not-json-serializable"],
)
def test_sidecar_rejects_invalid_options(options):
    with pytest.raises(TypeError, match="Sidecar options"):
        Sidecar("heartbeat", options=options)


def test_subprocess_serializes_options_on_command_line(mocker):
    options = {
        "enable_tracing": True,
        "streams": ["stdout", "stderr"],
        "limits": {"retries": 3, "interval": 0.25},
        "message": 'value with spaces and "quotes"',
    }
    sidecar = SidecarSubProcess.__new__(SidecarSubProcess)
    sidecar._worker_type = "save_logs_periodically"
    sidecar._options = options
    sidecar._logger = mocker.Mock()
    start_subprocess = mocker.patch.object(
        sidecar, "_start_subprocess", return_value=None
    )

    sidecar.start()

    command = start_subprocess.call_args[0][0]
    assert command[-1] == (
        '{"enable_tracing":true,"limits":{"interval":0.25,"retries":3},'
        '"message":"value with spaces and \\"quotes\\"",'
        '"streams":["stdout","stderr"]}'
    )
    assert json.loads(command[-1]) == options


def test_worker_main_initializes_worker_from_command_line(mocker):
    options = {"enable_tracing": True, "nested": {"values": [1, 2, 3]}}

    class Worker(object):
        def __init__(self, options=None):
            self.options = options

    provider = SimpleNamespace(get_worker=lambda: Worker)
    mocker.patch.dict(
        "metaflow.sidecar.sidecar_worker.SIDECARS", {"test_worker": provider}
    )
    process_messages = mocker.patch("metaflow.sidecar.sidecar_worker.process_messages")

    result = CliRunner().invoke(
        sidecar_worker_main, ["test_worker", json.dumps(options)]
    )

    assert result.exit_code == 0, result.output
    worker_type, worker = process_messages.call_args[0]
    assert worker_type == "test_worker"
    assert worker.options == options


def test_worker_receives_deserialized_options():
    class Worker(object):
        def __init__(self, options=None):
            self.options = options

    provider = SimpleNamespace(get_worker=lambda: Worker)

    worker = instantiate_worker(
        provider, deserialize_options('{"enable_tracing": true}')
    )

    assert worker.options == {"enable_tracing": True}


def test_worker_without_options_uses_legacy_constructor():
    class Worker(object):
        def __init__(self):
            self.started = True

    provider = SimpleNamespace(get_worker=lambda: Worker)

    worker = instantiate_worker(provider, {})

    assert worker.started is True


@pytest.mark.parametrize(
    "options",
    [{"enable_tracing": "yes"}, {"unsupported": True}],
    ids=["invalid-type", "unknown-option"],
)
def test_log_publisher_rejects_invalid_options(options):
    with pytest.raises((TypeError, ValueError)):
        SaveLogsPeriodicallySidecar(options=options)


def test_log_publisher_traces_file_sizes_to_uploader_stdout(
    monkeypatch, mocker, tmp_path
):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    uploader_stdout = tmp_path / "periodical_uploader_stdout"
    save_logs_stdout = tmp_path / "save_logs_process_stdout"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))
    monkeypatch.setenv("PERIODICAL_UPLOADER_STDOUT", str(uploader_stdout))
    monkeypatch.setenv("SAVE_LOGS_PROCESS_STDOUT", str(save_logs_stdout))
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._enable_tracing = True
    publisher.is_alive = True
    mocker.patch(
        "metaflow.mflog.save_logs_periodically.time.sleep",
        side_effect=lambda _: setattr(publisher, "is_alive", False),
    )
    mocker.patch("metaflow.mflog.save_logs_periodically.time.time", return_value=100)
    upload = mocker.patch(
        "metaflow.mflog.save_logs_periodically.subprocess.call", return_value=0
    )

    publisher._update_loop()

    records = [
        parse(line)
        for line in uploader_stdout.read_bytes().splitlines(keepends=True)
        if line.startswith(b"[MFLOG|")
    ]
    messages = [record.msg.decode() for record in records]
    assert len(messages) == 2
    assert "file=%s previous_size=0 current_size=4 delta=4" % stdout in messages[0]
    assert "file=%s previous_size=0 current_size=4 delta=4" % stderr in messages[1]
    assert all("elapsed_seconds=0.000" in message for message in messages)
    upload.assert_called_once()
    assert upload.call_args.args[0] == ["python", "-m", "metaflow.mflog.save_logs"]
    assert upload.call_args.kwargs["stdout"].name == str(save_logs_stdout)
    assert upload.call_args.kwargs["stderr"] is subprocess.STDOUT
    process_records = [
        parse(line)
        for line in save_logs_stdout.read_bytes().splitlines(keepends=True)
        if line.startswith(b"[MFLOG|")
    ]
    process_messages = [record.msg.decode() for record in process_records]
    assert "[save_logs_process] process_start" in process_messages[0]
    assert "[save_logs_process] process_exit return_code=0" in process_messages[1]


def test_log_publisher_does_not_redirect_save_logs_without_tracing(mocker):
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._enable_tracing = False
    upload = mocker.patch("metaflow.mflog.save_logs_periodically.subprocess.call")

    publisher._call_save_logs()

    upload.assert_called_once_with(["python", "-m", "metaflow.mflog.save_logs"])
