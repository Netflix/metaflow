import base64
import importlib.metadata
import re
import time
from collections import namedtuple

import pytest

from metaflow.plugins.aws.aws_utils import compute_resource_attributes
from metaflow.plugins.pypi.conda_decorator import CondaStepDecorator
from metaflow.plugins.tenki import tenki as tenki_mod
from metaflow.plugins.tenki import tenki_cli, tenki_client
from metaflow.plugins.tenki.tenki import (
    Tenki,
    _sanitize_name,
    _tag,
    is_permanent_launch_error,
)
from metaflow.plugins.tenki.tenki_client import (
    TenkiClient,
    TenkiException,
    TenkiKilledException,
)
from metaflow.plugins.tenki.tenki_decorator import TenkiDecorator

MockDeco = namedtuple("MockDeco", ["name", "attributes"])


class _DS(object):
    def __init__(self, type_):
        self.TYPE = type_


class _Env(object):
    def get_package_commands(self, url, ds, metadata):
        return ["download %s" % url]

    def bootstrap_commands(self, step_name, ds):
        return ["bootstrap"]


def _deco(**attrs):
    base = {"cpu": None, "memory": None, "image": None, "executable": None}
    base.update(attrs)
    return TenkiDecorator(attributes=base)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("MyFlow/run-1", "myflow-run-1"),
        ("Step_Name", "step-name"),
        ("---", "step"),
        ("A" * 80, "a" * 40),
    ],
)
def test_sanitize_name(raw, expected):
    assert _sanitize_name(raw) == expected


def test_resource_defaults_used_without_resources():
    deco = _deco()
    assert compute_resource_attributes([], deco, TenkiDecorator.resource_defaults) == {
        "cpu": "2",
        "memory": "4096",
    }


def test_resource_merge_takes_max_with_resources():
    deco = _deco(cpu="4")
    merged = compute_resource_attributes(
        [MockDeco("resources", {"cpu": "1", "memory": "8192"})],
        deco,
        TenkiDecorator.resource_defaults,
    )
    assert merged == {"cpu": "4.0", "memory": "8192"}


def test_step_init_rejects_local_datastore():
    deco = _deco()
    with pytest.raises(TenkiException):
        deco.step_init(
            None, None, "start", [], _Env(), _DS("local"), lambda *a, **k: None
        )


@pytest.mark.parametrize("ds_type", ["s3", "azure", "gs"])
def test_step_init_accepts_remote_datastores(ds_type):
    deco = _deco()
    # Should not raise, and should populate resource attributes.
    deco.step_init(None, None, "start", [], _Env(), _DS(ds_type), lambda *a, **k: None)
    assert deco.attributes["cpu"] == "2"
    assert deco.attributes["memory"] == "4096"


def test_step_init_rejects_parallel():
    deco = _deco()
    with pytest.raises(TenkiException):
        deco.step_init(
            None,
            None,
            "start",
            [MockDeco("parallel", {})],
            _Env(),
            _DS("s3"),
            lambda *a, **k: None,
        )


def test_task_finished_local_fallback_does_not_crash(monkeypatch):
    # In the @catch local-fallback path, task_pre_step never started the log
    # sidecar. task_finished must not raise (no METAFLOW_TENKI_WORKLOAD).
    monkeypatch.delenv("METAFLOW_TENKI_WORKLOAD", raising=False)
    deco = _deco()
    deco.task_finished("start", None, None, True, 0, 0)


def test_default_project_id_respects_workspace():
    Proj = namedtuple("Proj", ["id"])
    Ws = namedtuple("Ws", ["id", "projects"])
    Identity = namedtuple("Identity", ["workspaces"])

    class _RawClient:
        def who_am_i(self):
            return Identity(
                workspaces=(
                    Ws("ws-1", (Proj("proj-1a"),)),
                    Ws("ws-2", (Proj("proj-2a"),)),
                )
            )

    client = TenkiClient.__new__(TenkiClient)
    client._client = _RawClient()
    assert client.default_project_id() == "proj-1a"  # first workspace
    assert client.default_project_id("ws-2") == "proj-2a"  # requested workspace
    assert client.default_project_id("ws-missing") is None


def test_sdk_min_version_enforced(monkeypatch):
    def _set_version(v):
        monkeypatch.setattr(importlib.metadata, "version", lambda dist: v)

    # Too old -> clear error.
    _set_version("0.3.9")
    with pytest.raises(TenkiException, match="too old"):
        tenki_client._check_min_version()

    # At or above the minimum -> no error.
    _set_version("0.4.0")
    tenki_client._check_min_version()
    _set_version("1.2.0")
    tenki_client._check_min_version()

    # Undeterminable version must not block (graceful degrade).
    def _raise(dist):
        raise importlib.metadata.PackageNotFoundError(dist)

    monkeypatch.setattr(importlib.metadata, "version", _raise)
    tenki_client._check_min_version()


def test_tag_sanitization_fits_tenki_constraints():
    charset = re.compile(r"^[a-z0-9_:.-]{1,32}$")

    # Email user: has '@' and uppercase and overflows 32 chars -> normalized,
    # hashed, still valid and <= 32.
    email = "Alvaro.Deleglise@Luxor.Tech"
    t = _tag("metaflow-user", email)
    assert charset.match(t), t
    assert len(t) <= 32
    assert t.startswith("metaflow-user:")

    # Short clean values pass through unchanged (human-readable).
    assert _tag("metaflow-run", "1784731662078694") == "metaflow-run:1784731662078694"
    assert _tag("metaflow-user", "tester") == "metaflow-user:tester"

    # Deterministic: create and cleanup must agree on the same input.
    assert _tag("metaflow-user", email) == _tag("metaflow-user", email)

    # Distinct long values stay distinct (no truncation collision).
    a = "user-with-a-very-long-name-alice@example.com"
    b = "user-with-a-very-long-name-bob@example.com"
    assert _tag("metaflow-user", a) != _tag("metaflow-user", b)
    assert len(_tag("metaflow-user", a)) <= 32


def test_cli_matching_sandboxes_uses_configured_credentials(monkeypatch):
    # `tenki list` / `tenki kill` must build the client with the same config as
    # launch, so cleanup works when credentials come only from Metaflow config.
    captured = {}

    class _FakeClient:
        def __init__(self, api_key=None, base_url=None):
            captured["api_key"] = api_key
            captured["base_url"] = base_url

        def list_sandboxes(self, tags=None):
            captured["tags"] = tags
            return []

    monkeypatch.setattr(tenki_cli, "TenkiClient", _FakeClient)
    monkeypatch.setattr(tenki_cli, "TENKI_API_KEY", "cfg-key")
    monkeypatch.setattr(tenki_cli, "TENKI_BASE_URL", "https://cfg.example")

    tenki_cli._matching_sandboxes("MyFlow", "run-1", "tester")

    assert captured["api_key"] == "cfg-key"
    assert captured["base_url"] == "https://cfg.example"
    # Always flow-scoped (second tag) so cleanup can never cross flows.
    assert captured["tags"] == [
        "metaflow",
        "metaflow-flow:myflow",
        "metaflow-run:run-1",
        "metaflow-user:tester",
    ]


def test_cli_matching_sandboxes_is_always_flow_scoped(monkeypatch):
    # Even with no run-id/user, the query is scoped to the flow tag — never the
    # bare "metaflow" tag (which would match every user's sandboxes).
    captured = {}

    class _FakeClient:
        def __init__(self, api_key=None, base_url=None):
            pass

        def list_sandboxes(self, tags=None):
            captured["tags"] = tags
            return []

    monkeypatch.setattr(tenki_cli, "TenkiClient", _FakeClient)
    tenki_cli._matching_sandboxes("MyFlow", None, None)
    assert captured["tags"] == ["metaflow", "metaflow-flow:myflow"]


def test_resolve_scope_semantics(monkeypatch):
    from metaflow.exception import CommandException

    echo = lambda *a, **k: None

    # Mutually-exclusive flags.
    with pytest.raises(CommandException):
        tenki_cli._resolve_scope("F", None, "u", True, echo)
    with pytest.raises(CommandException):
        tenki_cli._resolve_scope("F", "r", None, True, echo)

    # --my-runs -> current user, run_id stays None (all my runs of this flow).
    monkeypatch.setattr(tenki_cli.util, "get_username", lambda: "me")
    assert tenki_cli._resolve_scope("F", None, None, True, echo) == (None, "me")

    # A user filter alone -> that user's runs, no latest-run fallback.
    assert tenki_cli._resolve_scope("F", None, "bob", False, echo) == (None, "bob")

    # No flags -> default to the latest run of this flow.
    monkeypatch.setattr(tenki_cli.util, "get_latest_run_id", lambda e, f: "run-9")
    assert tenki_cli._resolve_scope("F", None, None, False, echo) == ("run-9", None)

    # No flags and no previous run -> clear error.
    monkeypatch.setattr(tenki_cli.util, "get_latest_run_id", lambda e, f: None)
    with pytest.raises(CommandException):
        tenki_cli._resolve_scope("F", None, None, False, echo)


class _KillSandbox(object):
    """A sandbox whose teardown methods succeed or raise per `fails`.

    `fails` is a set of method names that raise; any other listed method
    succeeds. Only methods in `has` exist on the object.
    """

    def __init__(self, name, has=("terminate", "close"), fails=()):
        self.name = name
        self._fails = set(fails)
        self.calls = []
        for m in has:
            setattr(self, m, self._make(m))

    def _make(self, method):
        def _fn():
            self.calls.append(method)
            if method in self._fails:
                raise RuntimeError("boom-%s" % method)

        return _fn


def test_terminate_sandboxes_tries_all_methods_before_failing():
    echoes = []
    echo = echoes.append

    # terminate() fails but close() succeeds -> counted as killed, NO false alarm.
    recovered = _KillSandbox("a", has=("terminate", "close"), fails=("terminate",))
    # every method fails -> counted as failed, one failure line.
    dead = _KillSandbox("b", has=("terminate", "close"), fails=("terminate", "close"))

    killed, failed = tenki_cli._terminate_sandboxes([recovered, dead], echo)

    assert (killed, failed) == (1, 1)
    # The recovered sandbox tried terminate then close (no early break).
    assert recovered.calls == ["terminate", "close"]
    # No "Failed" line for the recovered one; exactly one for the dead one.
    assert "Terminated a." in echoes
    assert not any("Failed to terminate a" in m for m in echoes)
    assert any("Failed to terminate b" in m for m in echoes)


def test_terminate_sandboxes_all_succeed():
    echoes = []
    sbs = [_KillSandbox("a"), _KillSandbox("b")]
    killed, failed = tenki_cli._terminate_sandboxes(sbs, echoes.append)
    assert (killed, failed) == (2, 0)


def test_terminate_sandboxes_no_teardown_method():
    # Degenerate sandbox exposing no teardown method: reported as failed with a
    # clear reason, not a stray "None".
    echoes = []
    sb = _KillSandbox("a", has=())
    killed, failed = tenki_cli._terminate_sandboxes([sb], echoes.append)
    assert (killed, failed) == (0, 1)
    assert any("no teardown method" in m for m in echoes)


def _conda_interpreter_for(step_decorators):
    """Drive the real CondaStepDecorator.runtime_task_created and return the
    interpreter it selects for a step carrying `step_decorators`."""
    Deco = namedtuple("Deco", ["name"])
    Step = namedtuple("Step", ["name", "decorators"])

    class _Env:
        def interpreter(self, step_name):
            return "pypi-env-python"  # sentinel: the resolved-env interpreter

    deco = CondaStepDecorator.__new__(CondaStepDecorator)
    deco.disabled = False
    deco.environment = _Env()
    deco.step = "start"
    deco.flow = [Step("start", [Deco(n) for n in step_decorators])]
    deco.runtime_task_created(None, None, None, None, False, None)
    return deco.interpreter


def test_conda_does_not_hijack_tenki_trampoline_interpreter():
    # Bug #7 regression (behavioral): @pypi/@conda must set interpreter=None for
    # a @tenki step (like @batch/@kubernetes) so it does NOT swap the local
    # `tenki step` trampoline to the resolved pypi env — which would make the
    # trampoline fail to import datastore/metadata deps. Without the fix ("tenki"
    # absent from conda's remote-backend list) a @tenki step gets a non-None
    # interpreter (the hijack), so this fails without the fix and passes with it.
    assert _conda_interpreter_for(["tenki", "pypi"]) is None
    # Parity: @kubernetes behaves identically.
    assert _conda_interpreter_for(["kubernetes", "pypi"]) is None
    # A local step (no remote backend) still gets the resolved-env interpreter.
    assert _conda_interpreter_for(["pypi"]) == "pypi-env-python"


# ---------------------------------------------------------------------------
# Integration-style tests: exercise the full launch_job -> exec (in a thread)
# -> wait -> exit-code/exception handling -> cleanup path with the Tenki SDK
# stubbed out. Networking, credentials and the real SDK are NOT required.
# ---------------------------------------------------------------------------


# Mirror the real SDK: every error subclasses SandboxError and carries a
# `retryable` class flag (only UNAVAILABLE / rate-limit are retryable).
class _FakeSandboxError(Exception):
    retryable = False


class _FakeCommandTimeout(_FakeSandboxError):
    pass


class _FakeSessionNotFound(_FakeSandboxError):
    pass


class _FakePermissionDenied(_FakeSandboxError):
    pass


class _FakeMissingAuthToken(_FakeSandboxError):
    pass


class _FakeUnauthorized(_FakeSandboxError):
    pass


class _FakeQuotaExceeded(_FakeSandboxError):
    pass


class _FakeRegistryImageNotFound(_FakeSandboxError):
    pass


class _FakeRateLimited(_FakeSandboxError):
    retryable = True


_FAKE_EXC = {
    "SandboxError": _FakeSandboxError,
    "CommandTimeoutError": _FakeCommandTimeout,
    "SessionNotFoundError": _FakeSessionNotFound,
    "PermissionDeniedError": _FakePermissionDenied,
    "MissingAuthTokenError": _FakeMissingAuthToken,
}


class _FakeResult(object):
    def __init__(
        self,
        exit_code,
        stdout_text="",
        stderr_text="",
        signal=None,
        reason=None,
        errno=None,
    ):
        self.exit_code = exit_code
        self.stdout_text = stdout_text
        self.stderr_text = stderr_text
        self.signal = signal
        self.reason = reason
        self.errno = errno

    @property
    def ok(self):
        # Mirrors tenki_sandbox.models.CommandResult.ok.
        return self.exit_code == 0 and not self.signal


class _FakeSandbox(object):
    def __init__(self, behavior):
        # behavior is either a _FakeResult (returned) or an Exception (raised).
        self._behavior = behavior
        self.closed = False
        self.exec_calls = []

    def exec(self, *args, **kwargs):
        self.exec_calls.append((args, kwargs))
        if isinstance(self._behavior, Exception):
            raise self._behavior
        return self._behavior

    def close(self):
        self.closed = True


class _FakeClient(object):
    def __init__(self, sandbox):
        self._sandbox = sandbox

    def create_sandbox(self, **kwargs):
        self._sandbox.create_kwargs = kwargs
        return self._sandbox

    def exception(self, name):
        return _FAKE_EXC.get(name, ())

    def default_project_id(self, workspace_id=None):
        return "proj-test"

    def close(self):
        self.closed = True


def _run_task(monkeypatch, behavior, cpu="2", memory="4096", run_time_limit=120):
    """Drive a task through the runner with a stubbed SDK; return (echoes, sandbox)."""
    sandbox = _FakeSandbox(behavior)
    monkeypatch.setattr(tenki_mod, "TenkiClient", lambda **kwargs: _FakeClient(sandbox))
    # Stub out datastore-backed log tailing.
    monkeypatch.setattr(tenki_mod, "get_log_tailer", lambda loc, ds_type: None)

    def _fake_tail_logs(prefix, stdout_tail, stderr_tail, echo, has_log_updates):
        # Mimic the real loop: keep going while there are updates.
        while has_log_updates():
            time.sleep(0.005)

    monkeypatch.setattr(tenki_mod, "tail_logs", _fake_tail_logs)

    tenki = Tenki(datastore=_DS("s3"), metadata=None, environment=_Env())
    tenki.launch_job(
        flow_name="MyFlow",
        run_id="run-1",
        step_name="start",
        task_id="t-1",
        attempt="0",
        user="tester",
        code_package_metadata="meta",
        code_package_sha="sha",
        code_package_url="s3://pkg.tgz",
        code_package_ds="s3",
        step_cli="python flow.py step start",
        image=None,
        cpu=cpu,
        memory=memory,
        run_time_limit=run_time_limit,
        env={},
    )
    echoes = []
    tenki.wait(
        "s3://logs/stdout",
        "s3://logs/stderr",
        echo=lambda msg, stream="stderr", **k: echoes.append((stream, msg)),
    )
    return echoes, sandbox


def test_run_success(monkeypatch):
    echoes, sandbox = _run_task(monkeypatch, _FakeResult(0))
    # Sandbox created with outbound networking enabled and the right resources.
    assert sandbox.create_kwargs["allow_outbound"] is True
    assert sandbox.create_kwargs["cpu_cores"] == 2
    assert sandbox.create_kwargs["memory_mb"] == 4096
    assert sandbox.create_kwargs["project_id"] == "proj-test"
    # Sandbox is tagged with the flow (for flow-scoped `tenki list`/`kill`),
    # run, and user — all sanitized to Tenki's tag charset.
    assert sandbox.create_kwargs["tags"] == [
        "metaflow",
        "metaflow-flow:myflow",
        "metaflow-run:run-1",
        "metaflow-user:tester",
    ]
    # Server-side TTL backstop = run_time_limit (120) + grace.
    assert sandbox.create_kwargs["max_duration"] > 120
    # We must NOT set idle_timeout_minutes: a data-plane exec doesn't refresh
    # last_activity_at, so it could auto-pause an actively-running task.
    assert "idle_timeout_minutes" not in sandbox.create_kwargs
    # exec ran `bash -c <cmd>` with an env dict forwarded.
    args, kwargs = sandbox.exec_calls[0]
    assert args[0] == "bash" and args[1] == "-c"
    assert kwargs["env"]["METAFLOW_TENKI_WORKLOAD"] == "1"
    # The sandbox name is exposed to the workload for metadata bookkeeping.
    assert kwargs["env"]["METAFLOW_TENKI_SANDBOX_NAME"]
    assert "Task finished with exit code 0." in [m for _, m in echoes]
    # Disposable microVM is torn down.
    assert sandbox.closed is True


def test_runtime_shim_precedes_bootstrap(monkeypatch):
    # The command must provision a `python`/pip runtime before the standard
    # metaflow bootstrap, so it runs on images (incl. the Tenki default) that
    # ship only python3. See _RUNTIME_BOOTSTRAP_SHIM.
    _, sandbox = _run_task(monkeypatch, _FakeResult(0))
    cmd = sandbox.exec_calls[0][0][2]
    # Symlink python -> python3 when absent, on a writable PATH dir (non-root).
    assert "ln -sf $(command -v python3) $HOME/.local/bin/python" in cmd
    assert "export PATH=$HOME/.local/bin:$PATH" in cmd
    # Allow pip on a PEP-668 externally-managed interpreter.
    assert "PIP_BREAK_SYSTEM_PACKAGES=1" in cmd
    # The shim must come before the code-package download / step.
    assert cmd.index("$HOME/.local/bin/python") < cmd.index("flow.py step")
    # An image with neither python nor python3 fails with a clear error rather
    # than a confusing bootstrap failure later.
    assert "no python runtime" in cmd
    assert "command -v python3" in cmd


class _FlakyCleanupSandbox(object):
    """close() raises for the first `fail_times` calls, then succeeds."""

    def __init__(self, fail_times):
        self.id = "sb-flaky"
        self._fail_times = fail_times
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        if self.close_calls <= self._fail_times:
            raise RuntimeError("transient API error")


def _make_runner():
    return Tenki(datastore=_DS("s3"), metadata=None, environment=_Env())


def test_cleanup_retry_keeps_handles_silently(capsys):
    # A non-final failed teardown must NOT clear the sandbox handle (so a later
    # pass can retry) and must NOT close the client (the retry needs it). It
    # must stay silent so a transient failure the retry resolves is no false
    # alarm.
    sandbox = _FlakyCleanupSandbox(fail_times=1)
    client = _FakeClient(sandbox)
    tenki = _make_runner()
    tenki._sandbox = sandbox
    tenki._client = client

    tenki._cleanup()  # wait()'s finally pass (final=False)

    assert sandbox.close_calls == 1
    assert tenki._sandbox is sandbox  # handle retained for retry
    assert getattr(client, "closed", False) is False  # client kept open
    assert capsys.readouterr().err == ""  # no warning yet


def test_cleanup_retry_succeeds_and_releases_handles():
    # The retry (a second _cleanup) succeeds: handle cleared, client closed.
    sandbox = _FlakyCleanupSandbox(fail_times=1)
    client = _FakeClient(sandbox)
    tenki = _make_runner()
    tenki._sandbox = sandbox
    tenki._client = client

    tenki._cleanup()  # first pass fails silently
    tenki._cleanup(final=True)  # atexit retry succeeds

    assert sandbox.close_calls == 2
    assert tenki._sandbox is None
    assert client.closed is True


def test_cleanup_final_failure_warns_and_keeps_handle(capsys):
    # When even the final (atexit) pass fails, surface a warning and keep the
    # handle rather than silently leaking a billed sandbox.
    sandbox = _FlakyCleanupSandbox(fail_times=99)
    client = _FakeClient(sandbox)
    tenki = _make_runner()
    tenki._sandbox = sandbox
    tenki._client = client

    tenki._cleanup(final=True)

    assert tenki._sandbox is sandbox
    assert getattr(client, "closed", False) is False
    assert "could not terminate sandbox sb-flaky" in capsys.readouterr().err


def test_run_nonzero_is_retryable(monkeypatch):
    with pytest.raises(TenkiException, match="Use @retry to retry"):
        _run_task(monkeypatch, _FakeResult(1, stderr_text="boom"))


def test_run_oom(monkeypatch):
    with pytest.raises(TenkiException, match="ran out of memory"):
        _run_task(monkeypatch, _FakeResult(137))


def test_run_signalled_exit0_is_a_failure(monkeypatch):
    # exit_code 0 but killed by a signal is NOT success (result.ok is False);
    # the signal must be surfaced in the diagnostic.
    with pytest.raises(TenkiException, match="signal SIGTERM"):
        _run_task(monkeypatch, _FakeResult(0, signal="SIGTERM"))


def test_run_signalled_surfaces_signal_generically(monkeypatch):
    # A signalled task is a failure and the signal appears in the generic
    # diagnostic (we do not guess Tenki's signal-name format to special-case it).
    with pytest.raises(TenkiException, match="signal SIGKILL"):
        _run_task(monkeypatch, _FakeResult(0, signal="SIGKILL"))


def test_run_failure_includes_reason_and_errno(monkeypatch):
    # reason and errno from the SDK are surfaced in the failure diagnostic.
    with pytest.raises(TenkiException, match="reason exec-failed.*errno 8"):
        _run_task(monkeypatch, _FakeResult(126, reason="exec-failed", errno=8))


def test_run_segfault(monkeypatch):
    with pytest.raises(TenkiException, match="segmentation fault"):
        _run_task(monkeypatch, _FakeResult(139))


def test_timeout_is_retryable(monkeypatch):
    with pytest.raises(TenkiException, match="timed out"):
        _run_task(monkeypatch, _FakeCommandTimeout("deadline"))


def test_builtin_timeouterror_is_retryable(monkeypatch):
    # The real SDK raises a builtin TimeoutError for the client-side exec
    # deadline (not the SDK's CommandTimeoutError), so it must map to a
    # retryable timeout too, not the generic failure branch.
    with pytest.raises(TenkiException, match="timed out"):
        _run_task(monkeypatch, TimeoutError("timed out waiting for command"))


def test_session_lost_is_retryable(monkeypatch):
    with pytest.raises(TenkiException, match="disappeared"):
        _run_task(monkeypatch, _FakeSessionNotFound("gone"))


def test_permission_denied_is_not_retryable(monkeypatch):
    # Non-retryable failures surface as TenkiKilledException, which the CLI
    # maps to METAFLOW_EXIT_DISALLOW_RETRY.
    with pytest.raises(TenkiKilledException):
        _run_task(monkeypatch, _FakePermissionDenied("bad token"))


def test_is_permanent_launch_error_classification():
    # A client that resolves SDK exception classes like the real TenkiClient.
    client = _FakeClient(None)

    # Permanent -> DISALLOW_RETRY.
    assert is_permanent_launch_error(TenkiKilledException("x"), client) is True
    assert is_permanent_launch_error(TenkiException("too old"), client) is True
    assert is_permanent_launch_error(_FakePermissionDenied("no"), client) is True
    assert is_permanent_launch_error(_FakeMissingAuthToken("no"), client) is True
    # Non-retryable SDK errors we don't name explicitly must still be permanent
    # (the bug the SDK's `retryable` flag guards against): auth, quota, bad image.
    assert is_permanent_launch_error(_FakeUnauthorized("no"), client) is True
    assert is_permanent_launch_error(_FakeQuotaExceeded("no"), client) is True
    assert is_permanent_launch_error(_FakeRegistryImageNotFound("no"), client) is True
    # A generic SandboxError (unknown gRPC code) defaults to retryable=False.
    assert is_permanent_launch_error(_FakeSandboxError("blip"), client) is True
    # Unknown non-SDK error -> permanent (conservative default).
    assert is_permanent_launch_error(RuntimeError("???"), client) is True

    # Transient -> retryable (plain non-zero exit).
    assert is_permanent_launch_error(_FakeSessionNotFound("gone"), client) is False
    assert is_permanent_launch_error(_FakeCommandTimeout("slow"), client) is False
    assert is_permanent_launch_error(TimeoutError("deadline"), client) is False
    # SDK-declared retryable (UNAVAILABLE / rate-limit) is honored.
    assert is_permanent_launch_error(_FakeRateLimited("slow down"), client) is False
    unavailable = _FakeSandboxError("service unavailable")
    unavailable.retryable = True  # mirrors map_rpc_error's UNAVAILABLE mapping
    assert is_permanent_launch_error(unavailable, client) is False


def test_is_permanent_launch_error_without_client():
    # If the client failed to construct, SDK names resolve to () and never
    # match; our own signals and the unknown-default still classify correctly.
    assert is_permanent_launch_error(TenkiException("guard"), None) is True
    assert is_permanent_launch_error(TenkiKilledException("x"), None) is True
    assert is_permanent_launch_error(RuntimeError("???"), None) is True
    # A builtin TimeoutError is transient even with no client.
    assert is_permanent_launch_error(TimeoutError("x"), None) is False


def test_unknown_exit_code_is_retryable(monkeypatch):
    # A None exit code (command killed, SDK returned no status) must be a
    # retryable failure, not a silent success.
    with pytest.raises(TenkiException, match="unknown status"):
        _run_task(monkeypatch, _FakeResult(None))


def test_nonzero_surfaces_stderr(monkeypatch):
    # The captured stderr is echoed on a non-zero exit even before the raise.
    sandbox = _FakeSandbox(_FakeResult(1, stderr_text="boom-on-stderr"))
    monkeypatch.setattr(tenki_mod, "TenkiClient", lambda **kwargs: _FakeClient(sandbox))
    monkeypatch.setattr(tenki_mod, "get_log_tailer", lambda loc, ds_type: None)

    def _fake_tail_logs(prefix, stdout_tail, stderr_tail, echo, has_log_updates):
        while has_log_updates():
            time.sleep(0.005)

    monkeypatch.setattr(tenki_mod, "tail_logs", _fake_tail_logs)

    tenki = Tenki(datastore=_DS("s3"), metadata=None, environment=_Env())
    tenki.launch_job(
        flow_name="F",
        run_id="r",
        step_name="start",
        task_id="t",
        attempt="0",
        user="u",
        code_package_metadata="m",
        code_package_sha="s",
        code_package_url="s3://p",
        code_package_ds="s3",
        step_cli="python flow.py step start",
        cpu="1",
        memory="512",
        run_time_limit=60,
        env={},
    )
    msgs = []
    with pytest.raises(TenkiException):
        tenki.wait("a", "b", echo=lambda m, stream="stderr", **k: msgs.append(m))
    assert "boom-on-stderr" in msgs


def test_resource_float_strings(monkeypatch):
    # Resource values arrive as float-formatted strings when merged with
    # @resources; cpu_cores/memory_mb/max_duration/timeout must all parse.
    _, sandbox = _run_task(
        monkeypatch,
        _FakeResult(0),
        cpu="1.0",
        memory="4096.0",
        run_time_limit="300.0",
    )
    assert sandbox.create_kwargs["cpu_cores"] == 1
    assert sandbox.create_kwargs["memory_mb"] == 4096
    assert sandbox.create_kwargs["max_duration"] > 300
    assert sandbox.exec_calls[0][1]["timeout"] == 300


def test_env_vars_forward_only_the_datastores_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ak")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://minio:9000")
    monkeypatch.setenv("AZURE_CLIENT_ID", "cid")
    monkeypatch.setenv("AZURE_TENANT_ID", "tid")
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", "http://fake-gcs:4443")

    s3_env = Tenki(_DS("s3"), None, _Env())._env_vars("m", "s", "s3://p", "s3", "u", {})
    assert s3_env["AWS_ACCESS_KEY_ID"] == "ak"
    assert s3_env["AWS_ENDPOINT_URL_S3"] == "http://minio:9000"
    assert "AZURE_CLIENT_ID" not in s3_env
    assert "STORAGE_EMULATOR_HOST" not in s3_env

    az_env = Tenki(_DS("azure"), None, _Env())._env_vars(
        "m", "s", "az://p", "azure", "u", {}
    )
    assert az_env["AZURE_CLIENT_ID"] == "cid"
    assert az_env["AZURE_TENANT_ID"] == "tid"
    assert "AWS_ACCESS_KEY_ID" not in az_env

    gs_env = Tenki(_DS("gs"), None, _Env())._env_vars("m", "s", "gs://p", "gs", "u", {})
    assert gs_env["STORAGE_EMULATOR_HOST"] == "http://fake-gcs:4443"
    assert "AWS_ACCESS_KEY_ID" not in gs_env
    assert "AZURE_CLIENT_ID" not in gs_env


def test_env_vars_forward_secrets_otel_and_sse(monkeypatch):
    # Parity with @kubernetes: secret-backend prefixes, S3 server-side
    # encryption, and OTEL config must reach the sandbox.
    monkeypatch.setattr(tenki_mod, "AWS_SECRETS_MANAGER_DEFAULT_REGION", "us-west-2")
    monkeypatch.setattr(tenki_mod, "GCP_SECRET_MANAGER_PREFIX", "projects/p/secrets/")
    monkeypatch.setattr(tenki_mod, "AZURE_KEY_VAULT_PREFIX", "https://kv.example")
    monkeypatch.setattr(tenki_mod, "OTEL_ENDPOINT", "http://otel:4317")
    monkeypatch.setattr(tenki_mod, "OTEL_SERVICE_NAME", "my-svc")
    monkeypatch.setattr(tenki_mod, "S3_SERVER_SIDE_ENCRYPTION", "aws:kms")

    env = Tenki(_DS("s3"), None, _Env())._env_vars("m", "s", "s3://p", "s3", "u", {})
    assert env["METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION"] == "us-west-2"
    assert env["METAFLOW_GCP_SECRET_MANAGER_PREFIX"] == "projects/p/secrets/"
    assert env["METAFLOW_AZURE_KEY_VAULT_PREFIX"] == "https://kv.example"
    assert env["METAFLOW_OTEL_ENDPOINT"] == "http://otel:4317"
    assert env["METAFLOW_OTEL_SERVICE_NAME"] == "my-svc"
    assert env["METAFLOW_S3_SERVER_SIDE_ENCRYPTION"] == "aws:kms"


def test_env_vars_drop_unset_s3_encryption(monkeypatch):
    # Unset optional config must not be forwarded as a stringified None.
    monkeypatch.setattr(tenki_mod, "S3_SERVER_SIDE_ENCRYPTION", None)
    env = Tenki(_DS("s3"), None, _Env())._env_vars("m", "s", "s3://p", "s3", "u", {})
    assert "METAFLOW_S3_SERVER_SIDE_ENCRYPTION" not in env


def test_env_vars_materializes_gcp_credentials(monkeypatch, tmp_path):
    creds = tmp_path / "sa.json"
    creds.write_text('{"type":"service_account"}')
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(creds))

    tenki = Tenki(_DS("gs"), None, _Env())
    env = tenki._env_vars("m", "s", "gs://p", "gs", "u", {})
    blob = env["METAFLOW_GCP_CREDENTIALS_JSON_B64"]
    assert base64.b64decode(blob).decode() == '{"type":"service_account"}'

    # The command recreates the file and points GOOGLE_APPLICATION_CREDENTIALS at
    # it (only for gs).
    argv = tenki._command(
        flow_name="F",
        run_id="r",
        step_name="start",
        task_id="t",
        attempt="0",
        code_package_metadata="m",
        code_package_url="gs://p",
        step_cmds=["python flow.py step start"],
    )
    cmd = argv[2]
    assert "base64 -d" in cmd
    assert "GOOGLE_APPLICATION_CREDENTIALS=$HOME/mf_gcp_creds.json" in cmd


def test_command_has_no_gcp_prefix_for_s3():
    argv = Tenki(_DS("s3"), None, _Env())._command(
        flow_name="F",
        run_id="r",
        step_name="start",
        task_id="t",
        attempt="0",
        code_package_metadata="m",
        code_package_url="s3://p",
        step_cmds=["python flow.py step start"],
    )
    # The s3 command string is unchanged by the gs credential handling.
    assert "mf_gcp_creds.json" not in argv[2]


def test_command_contract():
    tenki = Tenki(datastore=_DS("s3"), metadata=None, environment=_Env())
    argv = tenki._command(
        flow_name="MyFlow",
        run_id="run-1",
        step_name="start",
        task_id="t-1",
        attempt="0",
        code_package_metadata="meta",
        code_package_url="s3://pkg.tgz",
        step_cmds=["python flow.py step start"],
    )
    # _command returns a `bash -c <cmd>` argv list (same transformation as
    # @kubernetes), so the \\"-escaped inner quotes are already resolved.
    assert argv[0] == "bash" and argv[1] == "-c"
    cmd = argv[2]
    # The optional init-script hook is prefixed (quotes resolved by the unwrap).
    assert cmd.startswith('${METAFLOW_INIT_SCRIPT:+eval "${METAFLOW_INIT_SCRIPT}"} &&')
    # Code package download, step command, and exit-code propagation are present.
    assert "download s3://pkg.tgz" in cmd
    assert "python flow.py step start" in cmd
    assert cmd.rstrip().endswith("exit $c")
