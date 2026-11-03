from types import SimpleNamespace

import metaflow.mflog.save_logs as save_logs_module


class S3ApiFailure(Exception):
    pass


def _configure_save_logs(monkeypatch, mocker, tmp_path, datastore_type="test"):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MF_PATHSPEC", "Flow/1/step/task")
    monkeypatch.setenv("MF_ATTEMPT", "0")
    monkeypatch.setenv("MF_DATASTORE", datastore_type)
    monkeypatch.setenv("MF_DATASTORE_ROOT", str(tmp_path / "datastore"))
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))

    task_datastore = mocker.Mock()
    flow_datastore = mocker.Mock()
    flow_datastore.get_task_datastore.return_value = task_datastore
    mocker.patch.object(
        save_logs_module, "DATASTORES", [SimpleNamespace(TYPE=datastore_type)]
    )
    mocker.patch.object(save_logs_module, "FlowDataStore", return_value=flow_datastore)
    return task_datastore


def test_save_logs_prints_upload_start_and_success(
    monkeypatch, mocker, tmp_path, capsys
):
    task_datastore = _configure_save_logs(monkeypatch, mocker, tmp_path)

    save_logs_module.save_logs()

    captured = capsys.readouterr()
    messages = captured.out.splitlines()
    assert "[save_logs] upload_start datastore=test" in messages[0]
    assert "[save_logs] upload_success datastore=test" in messages[1]
    assert captured.err == ""
    task_datastore.save_logs.assert_called_once()


def test_save_logs_prints_upload_failure_to_stderr(
    monkeypatch, mocker, tmp_path, capsys
):
    task_datastore = _configure_save_logs(monkeypatch, mocker, tmp_path)
    task_datastore.save_logs.side_effect = RuntimeError("upload failed")

    save_logs_module.save_logs()

    captured = capsys.readouterr()
    assert "[save_logs] upload_start datastore=test" in captured.out
    assert "[save_logs] upload_failure datastore=test" in captured.err
    assert "RuntimeError('upload failed')" in captured.err


def test_save_logs_prints_s3_api_failure_to_stderr(
    monkeypatch, mocker, tmp_path, capsys
):
    task_datastore = _configure_save_logs(
        monkeypatch, mocker, tmp_path, datastore_type="s3"
    )
    task_datastore.save_logs.side_effect = S3ApiFailure(
        "PutObject failed with AccessDenied"
    )

    save_logs_module.save_logs()

    captured = capsys.readouterr()
    assert "[save_logs] upload_start datastore=s3" in captured.out
    assert "[save_logs] upload_failure datastore=s3" in captured.err
    assert "S3ApiFailure('PutObject failed with AccessDenied')" in captured.err
    assert "upload_success" not in captured.out


def test_save_logs_prints_crash_to_stderr(monkeypatch, mocker, tmp_path, capsys):
    task_datastore = _configure_save_logs(monkeypatch, mocker, tmp_path)
    task_datastore.save_logs.side_effect = SystemExit("upload worker crashed")

    save_logs_module.save_logs()

    captured = capsys.readouterr()
    assert "[save_logs] upload_start datastore=test" in captured.out
    assert "[save_logs] upload_failure datastore=test" in captured.err
    assert "SystemExit('upload worker crashed')" in captured.err
    assert "upload_success" not in captured.out
