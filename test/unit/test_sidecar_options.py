import json
from types import SimpleNamespace

import pytest

from metaflow.mflog.save_logs_periodically import SaveLogsPeriodicallySidecar
from metaflow.mflog.mflog import parse
from metaflow.sidecar import Sidecar
from metaflow.sidecar.sidecar_subprocess import SidecarSubProcess
from metaflow.sidecar.sidecar_worker import deserialize_options, instantiate_worker


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
    sidecar = SidecarSubProcess.__new__(SidecarSubProcess)
    sidecar._worker_type = "save_logs_periodically"
    sidecar._options = {"enable_tracing": True}
    sidecar._logger = mocker.Mock()
    start_subprocess = mocker.patch.object(
        sidecar, "_start_subprocess", return_value=None
    )

    sidecar.start()

    command = start_subprocess.call_args.args[0]
    assert json.loads(command[-1]) == {"enable_tracing": True}


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


def test_log_publisher_traces_file_sizes_to_task_stderr(monkeypatch, mocker, tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._enable_tracing = True
    publisher.is_alive = True
    mocker.patch(
        "metaflow.mflog.save_logs_periodically.time.sleep",
        side_effect=lambda _: setattr(publisher, "is_alive", False),
    )
    mocker.patch("metaflow.mflog.save_logs_periodically.time.time", return_value=100)
    upload = mocker.patch("metaflow.mflog.save_logs_periodically.subprocess.call")

    publisher._update_loop()

    records = [
        parse(line)
        for line in stderr.read_bytes().splitlines(keepends=True)
        if line.startswith(b"[MFLOG|")
    ]
    messages = [record.msg.decode() for record in records]
    assert len(messages) == 2
    assert "file=%s previous_size=0 current_size=4 delta=4" % stdout in messages[0]
    assert "file=%s previous_size=0 current_size=4 delta=4" % stderr in messages[1]
    assert all("elapsed_seconds=0.000" in message for message in messages)
    upload.assert_called_once()
