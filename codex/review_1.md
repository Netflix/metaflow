# Round 1 Review — `test/core/test_core_pytest.py` and `test/core/conftest.py`

## Context
- Repo: `/home/coder/metaflow` on branch `AIPMDM-888` (rebased on origin/master).
- HEAD: `a0bbbfb0 fixed greptile-apps suggestions`.
- Baseline: `pytest test/core/test_core_pytest.py --collect-only` collects **502 tests** in 0.27 s.
- North star (from PR author): "the most simple pytest-centric setup ... while ensuring use of APIs/methods like runner, tox environments etc". Keep tox + Runner + the FlowDefinition+graph generator — strip everything else.

> Note: `codex exec` deadlocked at the "writing the review file" phase (≥ 9 min, no output written, process consuming no CPU). Killed via SIGTERM. Reviewer self-conducted this round per the skill's blocker clause.

## Findings

### F1 — `_run_flow` is 350 LOC of subprocess plumbing that pretends to be in-process
**Severity:** critical
**File:** `test/core/test_core_pytest.py:100-449`

```python
def _run_flow(formatter, context, core_checks, env_base, executor):
    ...
    tempdir = tempfile.mkdtemp("_metaflow_test")
    try:
        os.chdir(tempdir)
        ...
        original_env = os.environ.copy()
        try:
            ...
            os.environ.clear()
            os.environ.update(env)
            ...
            if executor == "cli":
                called_processes.append(subprocess.run(run_cmd("run"), env=env, ...))
            elif executor == "api":
                ...
                runner = Runner("test_flow.py", show_output=False, env=env, **top_level_dict)
                ...
            elif executor == "scheduler":
                ...
                while time.time() < deadline:
                    ...
                    time.sleep(10)
            ...
        finally:
            ...
            os.environ.clear()
            os.environ.update(original_env)
```

This is `run_tests.run_test()` (a 643-line module) with:
- a single `try`/`except RuntimeError` added around `Runner.run()`,
- an early `return` after a failed resume,
- a single `pytest.fail()` at the bottom.

Everything else — tempdir, `os.chdir`, `os.environ.clear/update`, `_log` with `threading.Lock`, the api branch synthesizing a `subprocess.CompletedProcess` to keep downstream uniform — is unchanged from the pre-pytest harness. The PR's "in-process" win is real only for `check_results`. Flow execution is still a subprocess in all three executor paths.

**Suggested fix:**
Decompose into pytest fixtures and one tiny strategy:

```python
# In conftest.py
@pytest.fixture
def core_workdir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(_CORE_DIR)
    return tmp_path

@pytest.fixture
def core_env(monkeypatch, tmp_path):
    nonce = uuid.uuid4().hex
    for k, v in os.environ.items():
        if "{nonce}" in v:
            monkeypatch.setenv(k, v.replace("{nonce}", nonce))
    monkeypatch.setenv("PYTHONPATH", f"{_CORE_DIR}:{os.environ.get('PYTHONPATH', '.')}")
    monkeypatch.setenv("PYTHONIOENCODING", "utf_8")
    monkeypatch.setenv("LANG", "en_US.UTF-8")
    monkeypatch.setenv("LC_ALL", "en_US.UTF-8")
    monkeypatch.setenv("METAFLOW_CLICK_API_PROCESS_CONFIG", "0")
    monkeypatch.setenv("METAFLOW_TEST_PRINT_FLOW", "1")
    return monkeypatch
```

Then `_run_flow` shrinks to "write the flow, dispatch on executor, check results", and the boilerplate disappears entirely.

**Rationale:** `monkeypatch.setenv` and `tmp_path` are why pytest fixtures exist. Manual `os.environ.clear()` leaks state across tests if a worker is killed mid-test (xdist will SIGTERM workers on shutdown), and `tempfile.mkdtemp` + `shutil.rmtree` re-implements `tmp_path` poorly (no failure preservation, no name collision protection).

---

### F2 — The api executor still goes through Runner (which subprocesses), then synthesises a `subprocess.CompletedProcess` so downstream code looks uniform
**Severity:** major
**File:** `test/core/test_core_pytest.py:226-256`

```python
elif executor == "api":
    top_level_dict, run_level_dict = construct_arg_dicts_from_click_api()
    runner = Runner(
        "test_flow.py", show_output=False, env=env, **top_level_dict
    )
    try:
        result = runner.run(**run_level_dict)
        with open(result.command_obj.log_files["stdout"], encoding="utf-8") as f:
            stdout = f.read()
        with open(result.command_obj.log_files["stderr"], encoding="utf-8") as f:
            stderr = f.read()
        called_processes.append(
            subprocess.CompletedProcess(
                result.command_obj.command,
                result.command_obj.process.returncode,
                stdout,
                stderr,
            )
        )
    except RuntimeError as e:
        _log("api executor failed: %s" % e, formatter, context)
        called_processes.append(subprocess.CompletedProcess([], 1, b"", b""))
```

The `subprocess.CompletedProcess` synthesis exists only so the resume / log code below this block can ignore which executor ran. Cost: 30 LOC of branchy plumbing that obscures intent, plus two file reads we don't need (we never look at stdout/stderr unless the run failed and we print them).

**Suggested fix:** Define a tiny normalised result type and stop pretending api is a subprocess:

```python
RunResult = namedtuple("RunResult", "returncode stdout stderr")

def _run_via_api(runner, run_kwargs):
    try:
        r = runner.run(**run_kwargs)
        return RunResult(r.command_obj.process.returncode, "", "")
    except RuntimeError as e:
        return RunResult(1, "", str(e))
```

Defer reading log files until we actually need them in the failure path. (Most successful runs never look at stdout/stderr.)

**Rationale:** Don't pay for stdout/stderr reads on the success path. Don't pretend types match when they don't.

---

### F3 — `construct_arg_dict` + `construct_arg_dicts_from_click_api` (33 LOC of click-internals introspection)
**Severity:** major
**File:** `test/core/test_core_pytest.py:123-156`

```python
def construct_arg_dict(params_opts, cli_options):
    result_dict = {}
    has_value = False
    secondary_supplied = False
    for arg in cli_options:
        if "=" in arg:
            given_opt, val = arg.split("=", 1)
            has_value = True
        else:
            given_opt = arg
        for key, each_param in params_opts.items():
            py_type = click_to_python_types[type(each_param.type)]
            if given_opt in each_param.opts:
                secondary_supplied = False
            elif given_opt in each_param.secondary_opts:
                secondary_supplied = True
            else:
                continue
            value = val if has_value else (False if secondary_supplied else True)
            ...
```

This walks click's `Param.opts`, `secondary_opts`, `multiple`, and translates string CLI options like `--metadata=local` into Python kwargs `{"metadata": "local"}` for `Runner`. It exists because the PR (and its predecessor) want to share one `top_options` list between the cli executor (pass-through) and the api executor (kwargs).

**Suggested fix:** Either (a) **upgrade Runner** to take an `args=` list (it already does in master HEAD — `MetaflowAPI` supports raw click args), in which case both branches converge:

```python
runner = Runner("test_flow.py", show_output=False, env=env, args=context["top_options"])
result = runner.run(args=context["run_options"])
```

…or (b) move the click introspection into a tiny `metaflow_test/click_args.py` helper and unit-test it — currently it lives 200 lines deep inside `_run_flow` where its bugs are invisible.

**Rationale:** The translation layer is brittle (it relies on `each_param.opts`/`secondary_opts`), version-dependent (click version), and bypasses Runner's documented API. Pushing it into Runner upstream is a one-week task; a helper module is a one-hour task. Either beats inlining.

---

### F4 — `os.environ.clear() / .update(env)` in `_run_flow` is dangerous mutation of process state
**Severity:** major
**File:** `test/core/test_core_pytest.py:207-208, 443-444`

```python
os.environ.clear()
os.environ.update(env)
...
finally:
    if runner is not None:
        runner.cleanup()
    os.environ.clear()
    os.environ.update(original_env)
```

If the test is interrupted between the clear and the finally (e.g. xdist worker SIGTERM during shutdown), the next test in this worker inherits a corrupted environment. The `os.environ.copy()` snapshot taken at the top is ALSO subject to mutation by Metaflow's own code (e.g. `metaflow.config.set_global_config()` populates `os.environ`).

**Suggested fix:** Wholesale replace with `monkeypatch.setenv` / `monkeypatch.delenv` — pytest unwinds them on test finalize regardless of failure mode, and they never global-clear.

```python
def test_flow_triple(flow_triple, core_checks, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for k, v in _build_env(...).items():
        monkeypatch.setenv(k, v)
    ...
```

**Rationale:** This is one of the canonical reasons monkeypatch exists. Manual env clearing in pytest tests is a code smell.

---

### F5 — `_log` with `threading.Lock` + `click.echo` is a fossil from the pre-pytest multiprocessing.Pool design
**Severity:** minor
**File:** `test/core/test_core_pytest.py:58-76`

```python
_log_lock = threading.Lock()

def _log(msg, formatter=None, context=None, processes=None):
    with _log_lock:
        parts = []
        if formatter:
            parts.append(str(formatter))
        if context:
            parts.append("context '%s'" % context["name"])
        prefix = " / ".join(parts)
        line = ("[%s] %s" % (prefix, msg)) if prefix else msg
        click.echo(line)
        if processes:
            for p in processes:
                if p.stdout:
                    click.echo(p.stdout, nl=False)
                if p.stderr:
                    click.echo(p.stderr, nl=False)
```

In the original `run_tests.py`, this needed a lock because tests ran in a multiprocessing.Pool and stdout was shared. Pytest captures stdout per-test and xdist serialises worker output at boundaries, so the lock is unnecessary, and `click.echo` adds nothing over `print` once captures are involved. The `processes=` parameter is referenced once on failure (line 411) — could just inline an f-string.

**Suggested fix:** Replace with plain `pytest.fail(message)` carrying the captured failure context, or with `caplog` if you want structured logs.

**Rationale:** Less surface, fewer custom logging helpers in tests.

---

### F6 — Three places define the test "context": tox.ini → conftest.py → test_core_pytest.py
**Severity:** major
**File:** `test/core/tox.ini:46-181`, `test/core/conftest.py:99-110`, `test/core/test_core_pytest.py:79-97`

`tox.ini` per-env `setenv` writes a flat namespace of `METAFLOW_CORE_*` vars:

```ini
[testenv:core-local]
setenv =
    {[testenv]setenv}
    METAFLOW_DEFAULT_METADATA = local
    METAFLOW_CORE_MARKER = local
    METAFLOW_CORE_TOP_OPTIONS = --metadata=local --datastore=local --environment=local --event-logger=nullSidecarLogger --no-pylint --quiet
    METAFLOW_CORE_EXECUTORS = cli,api
    METAFLOW_CORE_DISABLED_TESTS = {[_disabled]local}
```

`conftest.py` reads five of those:
```python
marker_name = os.environ.get("METAFLOW_CORE_MARKER", "local")
executors = [e for e in os.environ.get("METAFLOW_CORE_EXECUTORS", "cli,api").split(",") if e]
disabled_tests = {t for t in os.environ.get("METAFLOW_CORE_DISABLED_TESTS", "").split(",") if t}
enabled_tests = {t for t in os.environ.get("METAFLOW_CORE_ENABLED_TESTS", "").split(",") if t}
disable_parallel = os.environ.get("METAFLOW_CORE_DISABLE_PARALLEL", "") == "1"
```

`test_core_pytest.py` reads three more in `_context_from_env()`:
```python
top_options = shlex.split(os.environ.get("METAFLOW_CORE_TOP_OPTIONS", ""))
ctx = {
    "name": os.environ.get("METAFLOW_CORE_MARKER", "local"),
    ...
}
scheduler = os.environ.get("METAFLOW_CORE_SCHEDULER", "")
```

Both `METAFLOW_CORE_MARKER` reads can disagree if anything sets it twice; both env-reading paths reimplement the same parse logic.

**Suggested fix:** One session-scoped fixture in `conftest.py` returns a typed `CoreContext` dataclass; everything else consumes the fixture, no direct os.environ reads in test code:

```python
@dataclass(frozen=True)
class CoreContext:
    marker: str
    executors: tuple[str, ...]
    top_options: tuple[str, ...]
    run_options: tuple[str, ...]
    scheduler: str | None
    scheduler_timeout: int
    disabled_tests: frozenset[str]
    enabled_tests: frozenset[str]
    disable_parallel: bool

@pytest.fixture(scope="session")
def core_context() -> CoreContext:
    g = os.environ.get
    return CoreContext(
        marker=g("METAFLOW_CORE_MARKER", "local"),
        executors=tuple(filter(None, g("METAFLOW_CORE_EXECUTORS", "cli,api").split(","))),
        top_options=tuple(shlex.split(g("METAFLOW_CORE_TOP_OPTIONS", ""))),
        run_options=("--max-workers=50", "--max-num-splits=10000",
                     "--tag=刺身 means sashimi", "--tag=multiple tags should be ok"),
        scheduler=g("METAFLOW_CORE_SCHEDULER") or None,
        scheduler_timeout=int(g("METAFLOW_CORE_SCHEDULER_TIMEOUT", "600")),
        disabled_tests=frozenset(filter(None, g("METAFLOW_CORE_DISABLED_TESTS", "").split(","))),
        enabled_tests=frozenset(filter(None, g("METAFLOW_CORE_ENABLED_TESTS", "").split(","))),
        disable_parallel=g("METAFLOW_CORE_DISABLE_PARALLEL", "") == "1",
    )
```

`pytest_generate_tests` and `_run_flow` both consume `core_context` instead of `os.environ.get(...)`.

**Rationale:** Single source of truth. Typo-resistant. Trivial to test in isolation.

---

### F7 — `_skip_api_executor` set at import time silently disables the api executor everywhere
**Severity:** minor
**File:** `test/core/test_core_pytest.py:42-47`

```python
_skip_api_executor = False
try:
    from metaflow import Runner
    from metaflow.runner.click_api import click_to_python_types, extract_all_params
except ImportError:
    _skip_api_executor = True
```

…and then `_skip_api_executor` is **never used** in the new code. The variable name is preserved from `run_tests.py` but the `if executor == "api" and skip_api_executor` skip check is gone — meaning if Runner can't be imported, the api branch will fail on `Runner(...)` at line 228 with NameError. (Confirmed by grep: no other uses.)

**Suggested fix:** Either delete the dead variable + the bare `except ImportError`, or replace with `pytest.importorskip("metaflow.runner")` inside the api branch so it skips loudly.

**Rationale:** Dead variable is dead. If Runner is genuinely optional, skip explicitly; otherwise import unconditionally.

---

### F8 — `_DEFAULT_RUN_OPTIONS` is a module-level global that no test can override
**Severity:** minor
**File:** `test/core/test_core_pytest.py:51-56`

```python
_SASHIMI = "刺身 means sashimi"

_DEFAULT_RUN_OPTIONS = [
    "--max-workers=50",
    "--max-num-splits=10000",
    "--tag=%s" % _SASHIMI,
    "--tag=multiple tags should be ok",
]
```

These are baked into every test run. A tag with a Japanese string and a tag with a space are intentional (test that the harness handles them) but `--max-workers=50` and `--max-num-splits=10000` are arbitrary numbers that would surprise anyone writing a single-test debug invocation. A future maintainer adding a test that wants different limits has nowhere to set them.

**Suggested fix:** Move into the `CoreContext` fixture so a `conftest.py` closer to a test directory (or a `pytest.ini` `addopts`) can override.

**Rationale:** Hidden globals are technical debt. Minor here; major if the suite grows.

---

### F9 — `pytest_generate_tests` does ~70 LoC of filtering with two custom CLI flags + four env-var lists
**Severity:** major
**File:** `test/core/conftest.py:86-151`

```python
def pytest_generate_tests(metafunc):
    if "flow_triple" not in metafunc.fixturenames:
        return
    ok_tests_raw = metafunc.config.getoption("--core-tests", default=None)
    ok_graphs_raw = metafunc.config.getoption("--core-graphs", default=None)
    ok_tests = {t.lower() for t in ok_tests_raw.split(",") if t} if ok_tests_raw else set()
    ok_graphs = {g.lower() for g in ok_graphs_raw.split(",") if g} if ok_graphs_raw else set()
    ...
    for graph in all_graphs:
        if ok_graphs and graph["name"].lower() not in ok_graphs:
            continue
        if disable_parallel and any("num_parallel" in node for node in graph["graph"].values()):
            continue
        for test in all_tests:
            test_name = test.__class__.__name__
            if ok_tests and test_name.lower() not in ok_tests:
                continue
            if test_name in disabled_tests:
                continue
            if enabled_tests and test_name not in enabled_tests:
                continue
            if not FlowFormatter(graph, test).valid:
                continue
            for executor in executors:
                ...
                params.append(pytest.param((graph, test, executor), marks=[mark], id=param_id))
    metafunc.parametrize("flow_triple", params)
```

Six independent filters, expressed imperatively, that together do what `pytest -k`, `pytest -m`, and `pytest.mark.skipif` do natively.

**Suggested fix:** Hand `pytest_collection_modifyitems` the same job. Parametrize with all combinations + the right markers, then drop unselected items at collection time:

```python
def pytest_collection_modifyitems(config, items):
    disabled = ...
    enabled = ...
    for item in items:
        graph_name, test_name, executor = item.callspec.params["flow_triple"][...]
        if disabled and test_name in disabled:
            item.add_marker(pytest.mark.skip(reason=f"disabled by tox env"))
        if enabled and test_name not in enabled:
            item.add_marker(pytest.mark.skip(reason="not in enabled list"))
```

Or even simpler: tag each FlowDefinition class with `pytest.mark.local`, `pytest.mark.cloud`, `pytest.mark.scheduler` markers via a class attribute, and let users invoke `pytest -m "local and not large_artifact"`.

**Rationale:** `pytest_generate_tests` is a sharp tool; using it to do skip-logic is overkill. The end result for the user — `tox -e core-local -- --core-tests BasicArtifactTest` — is the same.

---

### F10 — Drop `--core-tests` / `--core-graphs` in favor of pytest-native `-k`
**Severity:** minor
**File:** `test/core/conftest.py:73-83`

```python
def pytest_addoption(parser):
    parser.addoption("--core-tests", default=None,
                     help="Comma-separated test class names to run...")
    parser.addoption("--core-graphs", default=None,
                     help="Comma-separated graph names to run...")
```

The param ID format `local/single-linear-step/BasicArtifactTest/cli` makes both filters expressible with `-k`:

```bash
pytest -k "BasicArtifact"
pytest -k "single-linear-step and BasicArtifact"
```

**Suggested fix:** Delete both options; document `-k` usage in `TESTING.md`. Saves ~10 LOC of conftest + the imperative filter loop.

**Rationale:** Native pytest is cheaper than custom CLI options when the param ID is well-formed.

---

## Smaller items (nit / style)

- **conftest.py:14**: `if _CORE_DIR not in sys.path: sys.path.insert(0, _CORE_DIR)` — same code is in `test_core_pytest.py:39`. Pick one.
- **conftest.py:32-34**: `iter_tests` mutates `sys.path` as a side effect of iteration. Move to module top-level.
- **test_core_pytest.py:174**: `original_env = os.environ.copy()` — unnecessary if monkeypatch is used.
- **test_core_pytest.py:430-433**: `_CHECKER_CLASSES = {"CliCheck": ..., "MetadataCheck": ...}` is a dispatch table inside a function body. Hoist module-level.

## TOP 5 SIMPLIFICATIONS (round 1)

1. **Replace `os.environ.clear()/update()` boilerplate with `monkeypatch.setenv`** — eliminates ~30 LOC and a class of leak bugs. (F4)
2. **Replace `tempfile.mkdtemp()` + `shutil.rmtree` with `tmp_path` / `monkeypatch.chdir`** — eliminates ~10 LOC and gives failure-preservation for free. (F1)
3. **Consolidate `os.environ.get(...)` reads into a session-scoped `core_context` fixture** — single source of truth. (F6)
4. **Delete the `subprocess.CompletedProcess` synthesis on the api branch** — return a normalised `RunResult` namedtuple instead, defer log reads to failure path. (F2)
5. **Delete dead `_skip_api_executor`** — variable is never consulted. (F7)

These five together cut ~80 LOC from `test_core_pytest.py` (from 486 → ~400) without touching test semantics or the runner/tox APIs the user wants to keep.
