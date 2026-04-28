# Address — Round 2

## Summary
Apply F4 from round 1: replace `tempfile.mkdtemp` + `os.chdir` + `os.environ.clear/update` with `tmp_path` + `monkeypatch.chdir` + `monkeypatch.setenv`.

## Per-finding verdicts

| ID | Verdict | Reasoning |
|----|---------|-----------|
| F4 | **accept, applied** | Net win: deleted `import tempfile`, deleted `original_env = os.environ.copy()`, collapsed two nested `try/finally` to one. The behaviour change is failure-mode improvement: pytest unwinds env even on xdist worker SIGTERM, where the original `try/finally` could leak. |

## Changes applied

`test/core/test_core_pytest.py`:

- Function signature: `_run_flow(formatter, context, core_checks, env_base, executor)` → `_run_flow(formatter, context, core_checks, env_base, executor, tmp_path, monkeypatch)`.
- `tempdir = tempfile.mkdtemp(...)` + outer `try` + `os.chdir(tempdir)` → `monkeypatch.chdir(tmp_path)`.
- Outer `finally: os.chdir(_CORE_DIR); shutil.rmtree(tempdir)` → deleted (`tmp_path` auto-cleans, `monkeypatch` auto-restores chdir).
- `original_env = os.environ.copy()` → deleted; read `os.environ.get(...)` inline where needed.
- Inner `os.environ.clear(); os.environ.update(env)` → `for k, v in env.items(): monkeypatch.setenv(k, v)`.
- Inner `finally: ...; os.environ.clear(); os.environ.update(original_env)` → `finally: if runner is not None: runner.cleanup()` (only Runner cleanup remains).
- `test_flow_triple` signature: takes `tmp_path` and `monkeypatch` fixtures and forwards to `_run_flow`.
- Comment update: `os.chdir'd to tempdir` → `chdir'd to tmp_path`.
- `import tempfile` removed.

## Test strategy

1. `pytest --collect-only`: should still report 502.
2. Smoke run: `BasicArtifact / single-linear-step / cli` on local backend.
3. Differential check: confirm any pre-existing failures are NOT caused by this change by re-running them against `HEAD~1` (the round-1 commit).

## Verification

```
$ /home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py --collect-only -q | tail -3
-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
========================= 502 tests collected in 0.28s =========================

$ PATH=/home/coder/.venv/bin:$PATH METAFLOW_CORE_MARKER=local METAFLOW_USER=tester \
    METAFLOW_CORE_TOP_OPTIONS="--metadata=local --datastore=local --environment=local --event-logger=nullSidecarLogger --no-pylint --quiet" \
    METAFLOW_CORE_EXECUTORS=cli \
    /home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py -m local \
    -k "BasicArtifact and single-linear-step and cli" --tb=short
================= 1 passed, 250 deselected, 1 warning in 2.94s =================
```

### Differential check (RuntimeDag isolation issue is pre-existing)

When `BasicArtifact` and `RuntimeDag` are run together, RuntimeDag fails with `Flow('RuntimeDagFlow') does not exist`. This happens with **both** the current F4 code and the unmodified pre-F4 code (verified by checking out `HEAD~1` and re-running). It's an isolation bug in the parent-process metaflow client cache, **not** caused by this round's refactor. Filed for round 5 follow-up; no action this round.

## Test-speed impact this round

Negligible. `monkeypatch.setenv` per key has overhead similar to manual `os.environ.update`. No tests run differently.

## Scope adjustment / deferred items

- F2 (drop `subprocess.CompletedProcess` synthesis) → round 4.
- F3 (replace `construct_arg_dict` introspection with Runner `args=`) → round 3 (needs verifying Runner signature first).
- F5 (collapse `_log` helper) → round 4.
- F6 (consolidate context state into a typed `core_context` fixture) → round 3.
