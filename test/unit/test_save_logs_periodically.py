from types import SimpleNamespace

import metaflow.mflog.save_logs_periodically as save_logs_periodically_module
from metaflow.mflog import BASH_SAVE_LOGS_ARGS
from metaflow.mflog.mflog import parse
from metaflow.mflog.save_logs_periodically import SaveLogsPeriodicallySidecar


def _new_sidecar(enable_tracing):
    sidecar = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    sidecar._enable_tracing = enable_tracing
    return sidecar


def _read_uploader_messages(path):
    if not path.exists():
        return []
    return [
        parse(line).msg.decode()
        for line in path.read_bytes().splitlines(keepends=True)
        if line.startswith(b"[MFLOG|")
    ]


def test_init_uses_options_instead_of_environment(monkeypatch, mocker, tmp_path):
    mocker.patch.object(save_logs_periodically_module, "Thread")
    monkeypatch.setenv("PERIODICAL_UPLOADER_STDOUT", str(tmp_path / "trace"))

    sidecar = SaveLogsPeriodicallySidecar()

    assert sidecar._enable_tracing is False


def test_init_enables_tracing_from_options(mocker):
    mocker.patch.object(save_logs_periodically_module, "Thread")

    sidecar = SaveLogsPeriodicallySidecar(options={"enable_tracing": True})

    assert sidecar._enable_tracing is True


def test_call_save_logs_without_tracing_uses_plain_subprocess_call(mocker):
    sidecar = _new_sidecar(False)
    call = mocker.patch(
        "metaflow.mflog.save_logs_periodically.subprocess.call", return_value=0
    )

    returncode = sidecar._call_save_logs()

    assert returncode == 0
    call.assert_called_once_with(BASH_SAVE_LOGS_ARGS)


def test_call_save_logs_captures_upload_failure_diagnostics(
    monkeypatch, mocker, tmp_path
):
    uploader_stdout = tmp_path / "periodical_uploader_stdout"
    monkeypatch.setenv("PERIODICAL_UPLOADER_STDOUT", str(uploader_stdout))
    sidecar = _new_sidecar(True)
    process = SimpleNamespace(
        communicate=mocker.Mock(
            return_value=(
                b"[save_logs] upload_start datastore=s3 files=[]\n",
                b"[save_logs] upload_failure datastore=s3 files=[] "
                b"error=S3ApiFailure('PutObject failed') "
                b"elapsed_seconds=0.001\n",
            )
        ),
        returncode=0,
    )
    popen = mocker.patch(
        "metaflow.mflog.save_logs_periodically.subprocess.Popen",
        return_value=process,
    )

    returncode = sidecar._call_save_logs()

    assert returncode == 0
    messages = _read_uploader_messages(uploader_stdout)
    assert messages == [
        "[save_logs stdout] [save_logs] upload_start datastore=s3 files=[]",
        "[save_logs stderr] [save_logs] upload_failure datastore=s3 files=[] "
        "error=S3ApiFailure('PutObject failed') elapsed_seconds=0.001",
    ]
    popen.assert_called_once_with(
        BASH_SAVE_LOGS_ARGS,
        stdout=save_logs_periodically_module.subprocess.PIPE,
        stderr=save_logs_periodically_module.subprocess.PIPE,
    )
    process.communicate.assert_called_once_with()


def test_call_save_logs_confirms_absence_of_logs_when_child_crashes(
    monkeypatch, mocker, tmp_path
):
    uploader_stdout = tmp_path / "periodical_uploader_stdout"
    monkeypatch.setenv("PERIODICAL_UPLOADER_STDOUT", str(uploader_stdout))
    sidecar = _new_sidecar(True)
    process = SimpleNamespace(
        communicate=mocker.Mock(return_value=(b"", b"")),
        returncode=-9,
    )
    mocker.patch(
        "metaflow.mflog.save_logs_periodically.subprocess.Popen",
        return_value=process,
    )

    returncode = sidecar._call_save_logs()

    assert returncode == -9
    assert _read_uploader_messages(uploader_stdout) == []
    process.communicate.assert_called_once_with()
