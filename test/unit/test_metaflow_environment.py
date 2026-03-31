import os
import shutil
import stat
import subprocess
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

from metaflow.metaflow_environment import MetaflowEnvironment


def test_get_install_dependencies_cmd_bootstraps_pip():
    env = MetaflowEnvironment(None)

    cmd = env._get_install_dependencies_cmd("s3")

    assert 'if [ -z "$METAFLOW_SKIP_INSTALL_DEPENDENCIES" ]; then' in cmd
    assert "python -m pip --version >/dev/null 2>&1" in cmd
    assert "python -m ensurepip --upgrade >/dev/null 2>&1 || true;" in cmd
    assert "python -m pip install -qqq --no-compile --no-cache-dir" in cmd
    assert "boto3 requests" in cmd


def _write_fake_python(tmp_path):
    fake_python = tmp_path / "python"
    fake_python.write_text(
        """#!/bin/sh
set -eu
state_file="${FAKE_PYTHON_STATE_FILE:?}"
log_file="${FAKE_PYTHON_LOG_FILE:?}"
if [ "$1" = "-m" ] && [ "$2" = "pip" ] && [ "$3" = "--version" ]; then
    if [ -f "$state_file" ]; then
        echo "pip_version" >> "$log_file"
        exit 0
    fi
    exit 1
fi
if [ "$1" = "-m" ] && [ "$2" = "ensurepip" ] && [ "$3" = "--upgrade" ]; then
    echo "ensurepip" >> "$log_file"
    touch "$state_file"
    exit 0
fi
if [ "$1" = "-m" ] && [ "$2" = "pip" ] && [ "$3" = "install" ]; then
    echo "pip_install:$*" >> "$log_file"
    if [ ! -f "$state_file" ]; then
        echo "pip missing" >&2
        exit 1
    fi
    exit 0
fi
echo "unexpected:$*" >> "$log_file"
exit 0
""",
        encoding="utf-8",
    )
    fake_python.chmod(fake_python.stat().st_mode | stat.S_IEXEC)
    return fake_python


@pytest.mark.skipif(sys.platform == "win32", reason="requires a POSIX shell")
def test_get_install_dependencies_cmd_installs_pip_if_missing(tmp_path):
    env = MetaflowEnvironment(None)
    cmd = env._get_install_dependencies_cmd("s3")
    _write_fake_python(tmp_path)
    state_file = tmp_path / "state"
    log_file = tmp_path / "log"
    log_file.write_text("", encoding="utf-8")
    bash = shutil.which("bash")

    assert bash is not None

    exec_env = os.environ.copy()
    exec_env["PATH"] = "{}:{}".format(tmp_path, exec_env.get("PATH", ""))
    exec_env["FAKE_PYTHON_STATE_FILE"] = str(state_file)
    exec_env["FAKE_PYTHON_LOG_FILE"] = str(log_file)

    subprocess.run([bash, "-lc", cmd], check=True, env=exec_env)

    log_lines = log_file.read_text(encoding="utf-8").splitlines()
    assert log_lines[0] == "ensurepip"
    assert log_lines[1] == "pip_version"
    assert "pip_install:-m pip install -qqq --no-compile --no-cache-dir --disable-pip-version-check boto3 requests" in log_lines[2]


@pytest.mark.skipif(sys.platform == "win32", reason="requires a POSIX shell")
def test_get_install_dependencies_cmd_respects_skip_flag(tmp_path):
    env = MetaflowEnvironment(None)
    cmd = env._get_install_dependencies_cmd("s3")
    _write_fake_python(tmp_path)
    log_file = tmp_path / "log"
    log_file.write_text("", encoding="utf-8")
    bash = shutil.which("bash")

    assert bash is not None

    exec_env = os.environ.copy()
    exec_env["PATH"] = "{}:{}".format(tmp_path, exec_env.get("PATH", ""))
    exec_env["FAKE_PYTHON_STATE_FILE"] = str(tmp_path / "state")
    exec_env["FAKE_PYTHON_LOG_FILE"] = str(log_file)
    exec_env["METAFLOW_SKIP_INSTALL_DEPENDENCIES"] = "1"

    subprocess.run([bash, "-lc", cmd], check=True, env=exec_env)

    assert log_file.read_text(encoding="utf-8") == ""
