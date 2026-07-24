import json
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

import metaflow.mflog.save_logs as save_logs_module
import metaflow.mflog.save_logs_periodically as publisher_module
from metaflow.mflog.mflog import parse


def test_log_transfer_config_values_are_exportable(tmp_path):
    trace_path = tmp_path / "trace"
    env = os.environ.copy()
    env["METAFLOW_DEBUG_LOG_TRANSFER"] = "1"
    env["METAFLOW_LOG_TRANSFER_TRACE_PATH"] = str(trace_path)
    script = """
import json
from metaflow.metaflow_config import DEBUG_LOG_TRANSFER, LOG_TRANSFER_TRACE_PATH
from metaflow.metaflow_config_funcs import config_values

print(json.dumps({
    "enabled": DEBUG_LOG_TRANSFER,
    "path": LOG_TRANSFER_TRACE_PATH,
    "exported": dict(config_values()),
}))
"""

    result = json.loads(
        subprocess.check_output([sys.executable, "-c", script], env=env, text=True)
    )

    assert result["enabled"] is True
    assert result["path"] == str(trace_path)
    assert result["exported"]["METAFLOW_DEBUG_LOG_TRANSFER"] == "True"
    assert result["exported"]["METAFLOW_LOG_TRANSFER_TRACE_PATH"] == str(trace_path)


@pytest.fixture
def save_logs_context(monkeypatch, mocker, tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MF_PATHSPEC", "Flow/1/step/task")
    monkeypatch.setenv("MF_ATTEMPT", "0")
    monkeypatch.setenv("MF_DATASTORE", "test")
    monkeypatch.setenv("MF_DATASTORE_ROOT", str(tmp_path))
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))

    task_datastore = mocker.Mock()
    flow_datastore = mocker.Mock()
    flow_datastore.get_task_datastore.return_value = task_datastore
    mocker.patch.object(
        save_logs_module,
        "DATASTORES",
        [SimpleNamespace(TYPE="test")],
    )
    mocker.patch.object(
        save_logs_module,
        "FlowDataStore",
        return_value=flow_datastore,
    )
    return task_datastore


@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled"])
def test_publisher_reads_tracing_config(monkeypatch, mocker, enabled):
    monkeypatch.setattr(publisher_module, "DEBUG_LOG_TRANSFER", enabled)
    thread = mocker.patch.object(publisher_module, "Thread")

    publisher = publisher_module.SaveLogsPeriodicallySidecar()

    assert publisher._enable_tracing is enabled
    assert thread.call_args.kwargs["target"].__self__ is publisher
    thread.return_value.start.assert_called_once_with()


def test_publisher_traces_file_sizes_and_child_output(monkeypatch, mocker, tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    trace = tmp_path / "periodical_uploader_stdout"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))
    monkeypatch.setattr(publisher_module, "LOG_TRANSFER_TRACE_PATH", str(trace))

    publisher = publisher_module.SaveLogsPeriodicallySidecar.__new__(
        publisher_module.SaveLogsPeriodicallySidecar
    )
    publisher._enable_tracing = True
    publisher.is_alive = True
    mocker.patch.object(
        publisher_module.time,
        "sleep",
        side_effect=lambda _: setattr(publisher, "is_alive", False),
    )
    mocker.patch.object(publisher_module.time, "time", return_value=100)
    process = SimpleNamespace(
        communicate=mocker.Mock(return_value=(b"child stdout\n", b"child stderr\n")),
        returncode=0,
    )
    popen = mocker.patch.object(
        publisher_module.subprocess,
        "Popen",
        return_value=process,
    )

    publisher._update_loop()

    records = [
        parse(line)
        for line in trace.read_bytes().splitlines(keepends=True)
        if line.startswith(b"[MFLOG|")
    ]
    messages = [record.msg.decode() for record in records]
    assert len(messages) == 4
    assert "file=%s previous_size=0 current_size=4 delta=4" % stdout in messages[0]
    assert "file=%s previous_size=0 current_size=4 delta=4" % stderr in messages[1]
    assert "[save_logs stdout] child stdout" in messages[2]
    assert "[save_logs stderr] child stderr" in messages[3]
    popen.assert_called_once_with(
        publisher_module.BASH_SAVE_LOGS_ARGS,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_publisher_uses_original_upload_path_when_tracing_is_disabled(mocker):
    publisher = publisher_module.SaveLogsPeriodicallySidecar.__new__(
        publisher_module.SaveLogsPeriodicallySidecar
    )
    publisher._enable_tracing = False
    call = mocker.patch.object(publisher_module.subprocess, "call")

    publisher._call_save_logs()

    call.assert_called_once_with(publisher_module.BASH_SAVE_LOGS_ARGS)


@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled"])
def test_save_logs_diagnostics_follow_config(
    monkeypatch, save_logs_context, capsys, enabled
):
    monkeypatch.setattr(save_logs_module, "DEBUG_LOG_TRANSFER", enabled)

    save_logs_module.save_logs()

    save_logs_context.save_logs.assert_called_once()
    captured = capsys.readouterr()
    if enabled:
        assert "[save_logs] upload_start" in captured.out
        assert "[save_logs] upload_success" in captured.out
    else:
        assert captured == ("", "")


def test_save_logs_traces_upload_failure(monkeypatch, save_logs_context, capsys):
    monkeypatch.setattr(save_logs_module, "DEBUG_LOG_TRANSFER", True)
    save_logs_context.save_logs.side_effect = RuntimeError("upload failed")

    save_logs_module.save_logs()

    captured = capsys.readouterr()
    assert "[save_logs] upload_start" in captured.out
    assert "[save_logs] upload_failure" in captured.err
    assert "upload failed" in captured.err
