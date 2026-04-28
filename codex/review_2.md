# Round 2 Review — Apply pytest fixtures (`tmp_path`, `monkeypatch`) to `_run_flow`

## Context
- HEAD: `65869141 review round 1: drop dead _skip_api_executor` (round 1 landed).
- Baseline: 502 tests collected.
- Round 1 deferred F4 (the monkeypatch refactor) and F2 (subprocess.CompletedProcess synthesis) to here.

## Scope of this round
This is the **mechanical pytest fixture upgrade**. The functional behaviour of `_run_flow` doesn't change; what changes is *how* it manages process state.

## Findings

### F4 — Replace `tempfile.mkdtemp` + `os.chdir` + `os.environ.clear/update` with pytest fixtures

**File:** `test/core/test_core_pytest.py:158-202` and `:434-443`

Current (post round-1):
```python
def _run_flow(formatter, context, core_checks, env_base, executor):
    ...
    tempdir = tempfile.mkdtemp("_metaflow_test")
    try:
        os.chdir(tempdir)
        ...
        path = os.path.join(tempdir, "test_flow.py")
        original_env = os.environ.copy()
        try:
            ...
            os.environ.clear()
            os.environ.update(env)
            ...
        finally:
            if runner is not None:
                runner.cleanup()
            os.environ.clear()
            os.environ.update(original_env)
        return ret, path
    finally:
        os.chdir(_CORE_DIR)
        shutil.rmtree(tempdir)
```

This is two nested try/finally blocks for state restoration that pytest fixtures already do. With `tmp_path` and `monkeypatch`:

```python
def _run_flow(formatter, context, core_checks, env_base, executor, tmp_path, monkeypatch):
    ...
    monkeypatch.chdir(tmp_path)
    ...
    path = os.path.join(tmp_path, "test_flow.py")

    nonce = str(uuid.uuid4())
    env = {...}  # build env dict as before
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    runner = None
    try:
        # ... 200 lines of executor logic, indented one level less ...
        ret = 0
    finally:
        if runner is not None:
            runner.cleanup()
    return ret, path
```

Two finally blocks collapse to one (only `runner.cleanup()` remains, because Runner spawns subprocesses we own). Both `tmp_path` (auto-cleaned, preserved on failure) and `monkeypatch.setenv` (auto-unwound) are managed by pytest.

**Suggested fix:** Apply the refactor exactly as above; pass `tmp_path` and `monkeypatch` from `test_flow_triple` into `_run_flow`.

**Rationale:**
- ~30 LOC saved.
- Fail-safe: `tmp_path` survives test failure for inspection (vs `mkdtemp` + `rmtree` deleting evidence).
- Fail-safe: `monkeypatch.setenv` survives xdist worker SIGTERM (vs manual env restore corrupting next-test environment).
- No behaviour change: subprocess and Runner still receive the same `env=env` dict explicitly.

---

### F8 — Hoist `_DEFAULT_RUN_OPTIONS` and `_SASHIMI` into a module-level constant set, off the function path

**File:** `test/core/test_core_pytest.py:51-56` (after round-1 line shifts)

These two globals are referenced by `_context_from_env()`. They look like configuration but are baked-in test data. Leave them as module-level for now — the right fix is to bundle them with `core_context` in round 3.

**Verdict for round 2:** **defer to round 3**. No change.

---

### F11 — `_log` and `_log_lock` are unused after the api branch loses its CompletedProcess synthesis (preview of round 4)

**File:** `test/core/test_core_pytest.py:58-76`

Currently `_log` is called only on failure paths (api executor failure, scheduler failures, resume failures). Once we drop the synthesis (round 4), most of these calls disappear and the helper can become a single `pytest.fail` with a context string.

**Verdict for round 2:** defer.

## Verification plan for this round

```bash
/home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py --collect-only -q | tail -3
# expect: 502 tests collected
```

If pytest can still collect 502 tests, structural integrity is preserved. Smoke-running a single test is desirable but `core-local` requires a metaflow-config writable home + the editable install + datastore=local. The tox env handles this; running outside tox is fragile. We rely on collect-only as the round 2 gate.

## TOP simplifications (round 2)

1. **Drop `tempfile.mkdtemp` + `shutil.rmtree`**: use `tmp_path`. (F4)
2. **Drop `os.chdir` + restore**: use `monkeypatch.chdir(tmp_path)`. (F4)
3. **Drop `os.environ.clear` + restore**: use `monkeypatch.setenv` per key. (F4)
4. **Drop `original_env = os.environ.copy()`**: read os.environ inline instead of snapshotting. (F4)
5. **Collapse two `try/finally` into one**: only Runner.cleanup needs explicit handling. (F4)
