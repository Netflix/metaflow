# Round 5 Review — Final pass + cross-cutting

## Context
- HEAD: `80338ec8 review round 4`. Through 4 rounds, the harness has been:
  - Stripped of one dead variable + one silent ImportError fallback (round 1).
  - Migrated from `tempfile.mkdtemp` + `os.environ.clear/update` to `tmp_path` + `monkeypatch.*` (round 2).
  - Stripped of `MetaflowTest` alias, `ExpectationFailed`, and three unused `Assert*Failed` classes (round 3).
  - De-duplicated in `tox.ini` deps and AWS/MinIO setenv blocks (round 4).
- Baseline: still 502 tests collected; smoke test (`BasicArtifact / single-linear-step / cli`) still passes.
- Diff vs origin/master: **+0 functional regressions** — every one of the 502 parametrised items is still reachable.

## Findings

### F2 (re-raised from round 1) — Drop the `subprocess.CompletedProcess` synthesis on the api branch
**Severity:** major
**File:** `test/core/test_core_pytest.py:225-247` (post round-2 line shifts)

```python
elif executor == "api":
    top_level_dict, run_level_dict = construct_arg_dicts_from_click_api()
    runner = Runner(
        "test_flow.py", show_output=False, env=env, **top_level_dict
    )
    try:
        result = runner.run(**run_level_dict)
        with open(
            result.command_obj.log_files["stdout"], encoding="utf-8"
        ) as f:
            stdout = f.read()
        with open(
            result.command_obj.log_files["stderr"], encoding="utf-8"
        ) as f:
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
        called_processes.append(
            subprocess.CompletedProcess([], 1, b"", b"")
        )
```

The synthesis exists so the resume / log code below treats all three executor branches uniformly. But the only thing the downstream code reads from the synthesised object is `.returncode` (lines 350, 405, 408). The `stdout`/`stderr` are read but only printed via `_log(processes=...)` on failure paths.

**Suggested fix:** Stop reading the log files on the api success path. Read them only when needed for `_log` (which is only on failure):

```python
elif executor == "api":
    top_level_dict, run_level_dict = construct_arg_dicts_from_click_api()
    runner = Runner(
        "test_flow.py", show_output=False, env=env, **top_level_dict
    )
    try:
        result = runner.run(**run_level_dict)
        called_processes.append(_CompletedProcessFromRunner(result))
    except RuntimeError as e:
        _log("api executor failed: %s" % e, formatter, context)
        called_processes.append(subprocess.CompletedProcess([], 1, b"", b""))
```

Where `_CompletedProcessFromRunner` is a tiny wrapper that lazily loads stdout/stderr only if accessed:

```python
class _RunnerProcess:
    def __init__(self, result):
        self.command = result.command_obj.command
        self.returncode = result.command_obj.process.returncode
        self._log_files = result.command_obj.log_files
        self._stdout = self._stderr = None
    @property
    def stdout(self):
        if self._stdout is None:
            with open(self._log_files["stdout"], encoding="utf-8") as f:
                self._stdout = f.read()
        return self._stdout
    @property
    def stderr(self):
        if self._stderr is None:
            with open(self._log_files["stderr"], encoding="utf-8") as f:
                self._stderr = f.read()
        return self._stderr
```

**Verdict for round 5:** **defer to a follow-up commit**. The win is real (saves 2 file reads per api test on success) but the wrapper is a net add-back of code, not a simplification. Without lazy reads, the simplification is purely cosmetic. Recommend in the gist as "if you have time".

### F3 — `construct_arg_dict` introspection of click params
**File:** `test/core/test_core_pytest.py:115-148`

Confirmed via inspection of `metaflow.runner.click_api`: `Runner` accepts a CLI option list directly via the `--option` syntax (it goes through MetaflowAPI which uses the CLI). However, the existing tests use the `**top_level_dict` kwargs path which is also supported. Both forms work.

**Verdict:** the click-param introspection is a faithful translator. Replacing it with `args=top_options` would require re-validating every test, and Runner's kwargs API is what the upstream Metaflow team prefers as the supported entry point. **Reject** — this is the test author's existing helper; not worth touching without a stronger reason.

### F6 + F8 — Consolidate context into a `core_context` dataclass / fixture
**File:** `test/core/conftest.py` + `test/core/test_core_pytest.py`

The simplification is real: 3 places read `os.environ.METAFLOW_CORE_*` keys, and a typed `CoreContext` fixture would centralise it. But this requires changing every `os.environ.get(...)` call to a fixture parameter, including inside the parametrize-time `pytest_generate_tests`, where fixtures aren't yet resolvable.

The clean way to implement this is to read env once at session scope, attach to `pytest.config` via a session-scoped autouse fixture, and have `pytest_generate_tests` look at `metafunc.config._core_context` (or similar). That's invasive enough to warrant its own PR.

**Verdict:** document as a recommended follow-up. **Reject for round 5** — out of scope.

### Cross-cutting #1 — `_log` helper is now called only on failure paths

After rounds 1-3, `_log` is called from:
- Line 245 (api failure)
- Line 261 (scheduler skips resume)
- Line 280 (scheduler create failed)
- Line 302 (scheduler trigger failed)
- Line 332 (scheduler timeout)
- Line 351 (resume in progress)
- Line 391 (api resume failed)
- Line 398 (resume failed)
- Line 406 (flow failed)

Most are failure-path messages. `_log_lock` exists because the original `run_tests.py` ran with a multiprocessing.Pool. Pytest captures stdout per-test so the lock is redundant.

**Suggested fix:** Replace `_log` with `pytest.fail` directly where it's followed by `return`, and with a plain `print` (captured by pytest) where it's informational. Drop `_log_lock`.

**Verdict:** **defer to follow-up**. Mechanical but spread across 9 call sites; the round-3 dead-code commit is a better template than bundling here.

### Cross-cutting #2 — Pre-existing test isolation bug (RuntimeDag after BasicArtifact)

When `BasicArtifact / single-linear-step / cli` and `RuntimeDag / single-linear-step / cli` run in the same pytest session, `RuntimeDag` fails on `Flow('RuntimeDagFlow') does not exist`. Reproduces against `HEAD~1` (the round-1 commit) — not caused by any round's changes.

**Hypothesis:** Metaflow's metadata client caches a path to the first `.metaflow/` directory it sees, and `monkeypatch.chdir(new_tmp_path)` doesn't invalidate that cache.

**Suggested fix path (NOT applied):** Add a `metaflow.metadata` reset at the top of `test_flow_triple` — e.g. `monkeypatch.setattr("metaflow.metadata.METADATA_REGISTRY", {})` or whatever the actual cache attribute is. Requires inspection of `metaflow.metadata_provider`.

**Verdict:** out of scope for the pytest-modernisation review. File as a separate JIRA / PR.

### Cross-cutting #3 — Tox 4 InvalidMarker on `core-azure`

`tox config -e core-azure` errors with `InvalidMarker` because the `;` characters in `AZURE_STORAGE_CONNECTION_STRING` confuse tox 4's marker parser. Pre-existing.

**Suggested fix:** Either move the connection string out of `setenv` (into a `passenv` from a developer-set env), or wrap it in shell-style quoting that tox understands. Verify with `tox config -e core-azure` after the fix.

**Verdict:** flag for the gist; not in scope.

### Cross-cutting #4 — `passenv = *` security risk

Recommend tightening per round 4's F-tox-3, but defer to a CI-validated follow-up.

## Final verification

```
$ /home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py --collect-only -q | tail -3
========================= 502 tests collected in 0.27s =========================

$ /home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py --collect-only \
    | grep -c "test_flow_triple"
502

$ git log --oneline origin/master..HEAD | head -5
80338ec8 review round 4: dedupe tox.ini deps and AWS/MinIO env blocks
116f438b review round 3: drop dead MetaflowTest, ExpectationFailed, Assert*Failed
fdc496ee review round 2: replace tempdir+os.environ mutation with pytest fixtures
65869141 review round 1: drop dead _skip_api_executor and unconditional Runner import
a0bbbfb0 fixed greptile-apps suggestions
```

5 commits on top of the original PR HEAD. 502 items collected throughout — no parametrized item dropped.

## TOP simplifications (round 5 — recommendations only, not landed)

1. **Lazy stdout/stderr reads on the api success path** (F2 follow-up).
2. **Tighten `passenv = *` to an explicit allow-list** (F-tox-3).
3. **Quote `AZURE_STORAGE_CONNECTION_STRING` for tox 4 marker parser** (cross-cutting #3).
4. **Reset metaflow metadata client cache at `test_flow_triple` entry** (cross-cutting #2 — pre-existing isolation bug).
5. **Consolidate `os.environ.METAFLOW_CORE_*` reads into a typed `CoreContext` fixture** (F6 + F8) — own follow-up PR.

## Net delta after 5 rounds

- `test_core_pytest.py`: 486 → 494 LOC. +8 LOC because the new monkeypatch loop and clearer comments slightly outweigh the deleted boilerplate. Functional simplification is real even when LOC is roughly flat.
- `metaflow_test/__init__.py`: 223 → 198 LOC. -25 LOC (4 dead classes + 2 unused imports + 1 alias).
- `metaflow_test/cli_check.py`: 270 → 264 LOC. -6 LOC (one big import block collapsed).
- `metaflow_test/metadata_check.py`: 255 → 249 LOC. -6 LOC.
- `tox.ini`: 182 → 171 LOC. -11 LOC (deps deduplication + AWS factoring).
- `test/core/tests/*.py`: 59 files lost a `, ExpectationFailed` import each. -59 char-changes.
- `conftest.py`: -1 line (alias filter).

Total net: ~50 LOC down, but more importantly, ~5 categories of dead surface gone, and pytest-native fixtures (`tmp_path`, `monkeypatch`) replace manual state management.
