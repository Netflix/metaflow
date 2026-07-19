from types import SimpleNamespace

import metaflow.mflog.save_logs as save_logs_module
from metaflow.mflog.mflog import parse


def _configure_save_logs(monkeypatch, mocker, tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    process_stdout = tmp_path / "save_logs_process_stdout"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MF_PATHSPEC", "Flow/1/step/task")
    monkeypatch.setenv("MF_ATTEMPT", "0")
    monkeypatch.setenv("MF_DATASTORE", "test")
    monkeypatch.setenv("MF_DATASTORE_ROOT", str(tmp_path / "datastore"))
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))
    monkeypatch.setenv("SAVE_LOGS_PROCESS_STDOUT", str(process_stdout))

    task_datastore = mocker.Mock()
    flow_datastore = mocker.Mock()
    flow_datastore.get_task_datastore.return_value = task_datastore
    mocker.patch.object(save_logs_module, "DATASTORES", [SimpleNamespace(TYPE="test")])
    mocker.patch.object(save_logs_module, "FlowDataStore", return_value=flow_datastore)
    return process_stdout, task_datastore


def _messages(path):
    return [
        parse(line).msg.decode() for line in path.read_bytes().splitlines(keepends=True)
    ]


def test_save_logs_records_upload_start_and_success(monkeypatch, mocker, tmp_path):
    process_stdout, task_datastore = _configure_save_logs(monkeypatch, mocker, tmp_path)

    save_logs_module.save_logs()

    messages = _messages(process_stdout)
    assert "[save_logs] upload_start datastore=test" in messages[0]
    assert "[save_logs] upload_success datastore=test" in messages[1]
    task_datastore.save_logs.assert_called_once()


def test_save_logs_records_upload_failure(monkeypatch, mocker, tmp_path):
    process_stdout, task_datastore = _configure_save_logs(monkeypatch, mocker, tmp_path)
    task_datastore.save_logs.side_effect = RuntimeError("upload failed")

    save_logs_module.save_logs()

    messages = _messages(process_stdout)
    assert "[save_logs] upload_start datastore=test" in messages[0]
    assert "[save_logs] upload_failure datastore=test" in messages[1]
    assert "RuntimeError('upload failed')" in messages[1]
