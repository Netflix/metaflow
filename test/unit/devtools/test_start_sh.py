"""Regression tests for metaflow-dev start.sh sudo/tunnel ordering (issue #2605)."""

import os
import signal
import stat
import subprocess
import time

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAKEFILE = os.path.join(REPO_ROOT, "devtools", "Makefile")


def _write_exec(path, body):
    with open(path, "w") as f:
        f.write(body)
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)


def _generate_start_sh(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    events = tmp_path / "events.log"
    tiltfile = tmp_path / "Tiltfile"
    tiltfile.write_text("# test stub\n")
    start_sh_dir = tmp_path / "devtools-state"
    start_sh_dir.mkdir()

    _write_exec(
        str(bindir / "sudo"),
        r"""#!/usr/bin/env bash
set -e
EVENTS="${MOCK_EVENTS:?}"
if [ "${MOCK_SUDO_FAIL:-}" = "1" ] && [ "$1" = "-v" ]; then
  echo "sudo-v-fail" >> "$EVENTS"
  exit 1
fi
if [ "$1" = "-v" ]; then
  echo "sudo-v-start" >> "$EVENTS"
  # Stay in-process long enough that a backgrounded sudo -v would let the
  # next command start first (the bug in issue #2605).
  sleep 0.2
  echo "sudo-v-done" >> "$EVENTS"
  exit 0
fi
echo "sudo-other $*" >> "$EVENTS"
exit 0
""",
    )
    _write_exec(
        str(bindir / "minikube"),
        r"""#!/usr/bin/env bash
set -e
EVENTS="${MOCK_EVENTS:?}"
echo "minikube $*" >> "$EVENTS"
if [ "$1" = "docker-env" ]; then
  echo "true"
  exit 0
fi
if [ "$1" = "tunnel" ]; then
  echo "tunnel-start" >> "$EVENTS"
  echo $$ > "${TUNNEL_PIDFILE:?}"
  sudo route -n add dummy 127.0.0.1 >/dev/null 2>&1 || sudo true
  if [ "${TUNNEL_HOLD:-}" = "1" ]; then
    trap 'exit 0' TERM INT
    while true; do sleep 1; done
  fi
  exit 0
fi
exit 0
""",
    )
    _write_exec(
        str(bindir / "tilt"),
        r"""#!/usr/bin/env bash
set -e
EVENTS="${MOCK_EVENTS:?}"
echo "tilt-start" >> "$EVENTS"
echo "tilt $*" >> "$EVENTS"
if [ "${TILT_HOLD:-}" = "1" ]; then
  trap 'exit 0' TERM INT
  while true; do sleep 1; done
fi
exit 0
""",
    )
    _write_exec(
        str(bindir / "pick_services.sh"),
        r"""#!/usr/bin/env bash
echo "pick-services-called" >> "${MOCK_EVENTS:?}"
exit 1
""",
    )

    subprocess.check_call(
        [
            "make",
            "-f",
            MAKEFILE,
            "generate-start-sh",
            "DEVTOOLS_DIR=%s" % start_sh_dir,
            "MINIKUBE=%s" % (bindir / "minikube"),
            "MINIKUBE_DIR=%s" % bindir,
            "TILT_DIR=%s" % bindir,
            "HELM_DIR=%s" % bindir,
            "TILTFILE=%s" % tiltfile,
            "PICK_SERVICES=%s" % (bindir / "pick_services.sh"),
        ]
    )
    start_sh = start_sh_dir / "start.sh"
    assert start_sh.is_file()
    return start_sh, events, bindir


def _event_lines(events_path):
    if not events_path.exists():
        return []
    with open(str(events_path)) as f:
        return [line.strip() for line in f if line.strip()]


def _run_start_sh(start_sh, events, bindir, extra_env=None):
    env = os.environ.copy()
    env["PATH"] = "%s:%s" % (bindir, env.get("PATH", ""))
    env["MOCK_EVENTS"] = str(events)
    env["TUNNEL_PIDFILE"] = str(events.parent / "tunnel.pid")
    env["SERVICES_OVERRIDE"] = "minio"
    if extra_env:
        env.update(extra_env)
    return subprocess.Popen(
        ["bash", str(start_sh)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def test_sudo_preflight_runs_to_completion_before_tunnel_is_started(tmp_path):
    start_sh, events, bindir = _generate_start_sh(tmp_path)
    syntax = subprocess.run(["bash", "-n", str(start_sh)])
    assert syntax.returncode == 0

    proc = _run_start_sh(start_sh, events, bindir)
    out, _ = proc.communicate(timeout=15)
    assert proc.returncode == 0, out.decode("utf-8", "replace")

    names = [
        e
        for e in _event_lines(events)
        if e in ("sudo-v-start", "sudo-v-done", "tunnel-start", "tilt-start")
    ]
    assert names == [
        "sudo-v-start",
        "sudo-v-done",
        "tunnel-start",
        "tilt-start",
    ]
    assert not any(e == "pick-services-called" for e in _event_lines(events))
    tilt_line = [e for e in _event_lines(events) if e.startswith("tilt ")]
    assert tilt_line, _event_lines(events)
    assert "-f" in tilt_line[0]
    assert "up" in tilt_line[0]


def test_sudo_preflight_failure_does_not_start_tunnel_or_tilt(tmp_path):
    start_sh, events, bindir = _generate_start_sh(tmp_path)
    proc = _run_start_sh(start_sh, events, bindir, extra_env={"MOCK_SUDO_FAIL": "1"})
    out, _ = proc.communicate(timeout=10)
    assert proc.returncode != 0
    combined = out.decode("utf-8", "replace")
    assert "Failed to obtain sudo privileges" in combined
    names = _event_lines(events)
    assert "sudo-v-fail" in names
    assert "tunnel-start" not in names
    assert "tilt-start" not in names


def test_tunnel_stays_backgrounded_until_cleanup(tmp_path):
    start_sh, events, bindir = _generate_start_sh(tmp_path)
    proc = _run_start_sh(
        start_sh,
        events,
        bindir,
        extra_env={"TUNNEL_HOLD": "1", "TILT_HOLD": "1"},
    )
    tunnel_pid = None
    try:
        tunnel_pidfile = events.parent / "tunnel.pid"
        deadline = time.time() + 8
        while time.time() < deadline:
            if (
                tunnel_pidfile.exists()
                and "tilt-start" in _event_lines(events)
                and "sudo-v-done" in _event_lines(events)
            ):
                break
            time.sleep(0.05)
        else:
            if proc.poll() is not None:
                out = proc.communicate()[0]
                pytest.fail(
                    "start.sh exited early: %s\n%s"
                    % (proc.returncode, out.decode("utf-8", "replace"))
                )
            pytest.fail("timed out waiting for tunnel+tilt: %s" % _event_lines(events))

        names = _event_lines(events)
        assert names.index("sudo-v-done") < names.index("tunnel-start")
        # tunnel and tilt now race - both should start, order doesn't matter
        assert "tunnel-start" in names
        assert "tilt-start" in names

        tunnel_pid = int(tunnel_pidfile.read_text().strip())
        os.kill(tunnel_pid, 0)
        os.kill(proc.pid, 0)

        # Match a session interrupt: signal the start.sh process group.
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)
            pytest.fail("start.sh did not exit after SIGTERM to its process group")

        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                os.kill(tunnel_pid, 0)
            except OSError:
                break
            time.sleep(0.05)
        else:
            pytest.fail("tunnel process %s still running after cleanup" % tunnel_pid)
    finally:
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except OSError:
                pass
            proc.wait(timeout=5)
        if tunnel_pid:
            try:
                os.kill(tunnel_pid, signal.SIGKILL)
            except OSError:
                pass


def test_generated_script_does_not_background_sudo_preflight(tmp_path):
    start_sh, _, _ = _generate_start_sh(tmp_path)
    with open(str(start_sh)) as f:
        lines = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]

    sudo_lines = [i for i, line in enumerate(lines) if "sudo -v" in line]
    assert sudo_lines, "expected a foreground sudo -v preflight"
    for i in sudo_lines:
        assert not lines[i].endswith("&"), lines[i]

    tunnel_bg = [
        i
        for i, line in enumerate(lines)
        if "tunnel" in line.split() and line.endswith("&")
    ]
    assert tunnel_bg, "expected minikube tunnel to remain a background job"
    assert min(sudo_lines) < min(tunnel_bg)

    tilt_lines = [i for i, line in enumerate(lines) if "tilt up" in line]
    assert tilt_lines
    assert not lines[tilt_lines[0]].endswith("&")
    assert min(tunnel_bg) < tilt_lines[0]
    assert any(line == "wait" for line in lines)
    # Check for EXIT trap that kills background jobs (jobs -p or kill 0)
    assert any(
        "trap " in line and ("jobs -p" in line or "kill 0" in line) and "EXIT" in line
        for line in lines
    )
    assert not any("kill -0" in line for line in lines)
    assert not any("_tunnel_pid" in line for line in lines)
