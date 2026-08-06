import json
import subprocess
from threading import Event
from types import SimpleNamespace

import pytest

from metaflow._vendor.click.testing import CliRunner
import metaflow.mflog.save_logs_periodically as save_logs_periodically
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
        "enable_debug_logs": True,
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
        '{"enable_debug_logs":true,"limits":{"interval":0.25,"retries":3},'
        '"message":"value with spaces and \\"quotes\\"",'
        '"streams":["stdout","stderr"]}'
    )
    assert json.loads(command[-1]) == options


def test_worker_main_initializes_worker_from_command_line(mocker):
    options = {"enable_debug_logs": True, "nested": {"values": [1, 2, 3]}}

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
        provider, deserialize_options('{"enable_debug_logs": true}')
    )

    assert worker.options == {"enable_debug_logs": True}


def test_worker_without_options_uses_legacy_constructor():
    class Worker(object):
        def __init__(self):
            self.started = True

    provider = SimpleNamespace(get_worker=lambda: Worker)

    worker = instantiate_worker(provider, {})

    assert worker.started is True


@pytest.mark.parametrize(
    "options",
    [{"enable_debug_logs": "yes"}, {"unsupported": True}],
    ids=["invalid-type", "unknown-option"],
)
def test_log_publisher_rejects_invalid_options(options):
    with pytest.raises((TypeError, ValueError)):
        SaveLogsPeriodicallySidecar(options=options)


def test_debug_fault_mode_requires_debug_log_transfer(monkeypatch):
    monkeypatch.setenv(
        save_logs_periodically.FAULT_MODE_ENV_VAR,
        save_logs_periodically.FAULT_UPLOAD_FAILURE,
    )
    monkeypatch.delenv(save_logs_periodically.DEBUG_LOG_TRANSFER_ENV_VAR, raising=False)

    assert save_logs_periodically._debug_fault_mode() is None

    monkeypatch.setenv(save_logs_periodically.DEBUG_LOG_TRANSFER_ENV_VAR, "1")

    assert (
        save_logs_periodically._debug_fault_mode()
        == save_logs_periodically.FAULT_UPLOAD_FAILURE
    )


def test_debug_fault_exits_publisher_process(mocker):
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._fault_mode = save_logs_periodically.FAULT_PROCESS_EXIT
    publisher._fault_triggered = Event()
    publisher.is_alive = True
    mocker.patch("metaflow.mflog.save_logs_periodically.time.sleep")
    write_marker = mocker.patch(
        "metaflow.mflog.save_logs_periodically._write_fault_marker"
    )
    write_log = mocker.patch("metaflow.mflog.save_logs_periodically._write_uploader_log")
    exit_process = mocker.patch("metaflow.mflog.save_logs_periodically.os._exit")

    publisher._inject_fault()

    write_marker.assert_called_once_with(save_logs_periodically.FAULT_PROCESS_EXIT)
    write_log.assert_called_once()
    assert publisher._fault_triggered.is_set()
    exit_process.assert_called_once_with(save_logs_periodically.FAULT_EXIT_CODE)


def test_debug_upload_failure_raises_from_call_save_logs(mocker):
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._fault_mode = save_logs_periodically.FAULT_UPLOAD_FAILURE
    publisher._fault_triggered = Event()
    publisher._fault_triggered.set()
    publisher._enable_debug_logs = False
    upload = mocker.patch("metaflow.mflog.save_logs_periodically.subprocess.call")
    write_log = mocker.patch("metaflow.mflog.save_logs_periodically._write_uploader_log")

    with pytest.raises(RuntimeError, match="Injected periodic log upload failure"):
        publisher._call_save_logs()

    upload.assert_not_called()
    write_log.assert_called_once_with(
        "[save_logs_periodically] simulated upload_failure raising RuntimeError"
    )


def test_upload_fault_activates_before_next_upload(mocker):
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._fault_mode = save_logs_periodically.FAULT_UPLOAD_FAILURE
    publisher._fault_triggered = Event()
    publisher._fault_delay_seconds = 15
    activate = mocker.patch.object(
        publisher,
        "_activate_fault",
        side_effect=lambda: publisher._fault_triggered.set(),
    )
    mocker.patch("metaflow.mflog.save_logs_periodically.time.time", return_value=115)

    publisher._activate_upload_fault_if_due(start_time=100)

    activate.assert_called_once_with()
    assert publisher._fault_triggered.is_set()


def test_log_publisher_silently_catches_upload_exception(monkeypatch, mocker, tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    uploader_log = tmp_path / "periodical_uploader_log"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))
    monkeypatch.setenv("PERIODICAL_UPLOADER_LOG_PATH", str(uploader_log))
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._enable_debug_logs = True
    publisher._fault_mode = save_logs_periodically.FAULT_UPLOAD_FAILURE
    publisher._fault_triggered = Event()
    publisher._fault_triggered.set()
    publisher.is_alive = True
    mocker.patch(
        "metaflow.mflog.save_logs_periodically.time.sleep",
        side_effect=lambda _: setattr(publisher, "is_alive", False),
    )
    mocker.patch("metaflow.mflog.save_logs_periodically.time.time", return_value=100)

    publisher._update_loop()

    messages = [
        parse(line).msg.decode()
        for line in uploader_log.read_bytes().splitlines(keepends=True)
        if line.startswith(b"[MFLOG|")
    ]
    assert any("simulated upload_failure raising RuntimeError" in m for m in messages)
    assert not any("upload_exception" in m for m in messages)


def test_log_publisher_writes_debug_logs_to_uploader_log(monkeypatch, mocker, tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    uploader_log = tmp_path / "periodical_uploader_log"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))
    monkeypatch.setenv("PERIODICAL_UPLOADER_LOG_PATH", str(uploader_log))
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._enable_debug_logs = True
    publisher.is_alive = True
    mocker.patch(
        "metaflow.mflog.save_logs_periodically.time.sleep",
        side_effect=lambda _: setattr(publisher, "is_alive", False),
    )
    mocker.patch("metaflow.mflog.save_logs_periodically.time.time", return_value=100)
    process = SimpleNamespace(
        communicate=mocker.Mock(return_value=(b"child stdout\n", b"child stderr\n")),
        returncode=0,
    )
    upload = mocker.patch(
        "metaflow.mflog.save_logs_periodically.subprocess.Popen",
        return_value=process,
    )

    publisher._update_loop()

    records = [
        parse(line)
        for line in uploader_log.read_bytes().splitlines(keepends=True)
        if line.startswith(b"[MFLOG|")
    ]
    messages = [record.msg.decode() for record in records]
    assert len(messages) == 4
    assert "file=%s previous_size=0 current_size=4 delta=4" % stdout in messages[0]
    assert "file=%s previous_size=0 current_size=4 delta=4" % stderr in messages[1]
    assert all("elapsed_seconds=0.000" in message for message in messages[:2])
    assert "[save_logs stdout] child stdout" in messages[2]
    assert "[save_logs stderr] child stderr" in messages[3]
    upload.assert_called_once_with(
        ["python", "-m", "metaflow.mflog.save_logs"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.communicate.assert_called_once_with()


def test_log_publisher_does_not_redirect_save_logs_without_debug_logs(mocker):
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._enable_debug_logs = False
    upload = mocker.patch("metaflow.mflog.save_logs_periodically.subprocess.call")

    publisher._call_save_logs()

    upload.assert_called_once_with(["python", "-m", "metaflow.mflog.save_logs"])
