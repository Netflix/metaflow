# Address — Round 1

## Summary of Codex review
Codex reviewed `test/core/test_core_pytest.py` (486 LOC) and `test/core/conftest.py` (151 LOC), the top-level harness. 10 findings: 1 critical (the harness is a near line-for-line port of the old `run_tests.py`), 5 major, 4 minor. Top-5 simplifications proposed:

1. Replace `os.environ.clear()/update()` with `monkeypatch.setenv` (F4)
2. Replace `tempfile.mkdtemp` with `tmp_path` (F1 / sub-item)
3. Consolidate env reads into a session `core_context` fixture (F6)
4. Drop `subprocess.CompletedProcess` synthesis on api branch (F2)
5. Delete dead `_skip_api_executor` (F7)

## Per-finding verdicts

| ID | Severity | File:line | Verdict | Reasoning |
|----|----------|-----------|---------|-----------|
| F1 | critical | `test_core_pytest.py:100-449` | **accept (split across rounds)** | Refactor scope is large enough to span rounds 1-3. Round 1 takes the lowest-risk slice (env + chdir). |
| F2 | major | `test_core_pytest.py:226-256` | **accept, defer to round 2** | Touches the api executor logic; want F4 in first to land monkeypatch infrastructure. |
| F3 | major | `test_core_pytest.py:123-156` | **partial accept, defer to round 2** | Real code-quality win, but Runner's `args=` API needs verification; defer until I can test. |
| F4 | major | `test_core_pytest.py:207-208, 443-444` | **accept now** | Highest leverage / lowest risk. monkeypatch is THE pytest idiom for env. |
| F5 | minor | `test_core_pytest.py:58-76` | **accept, defer to round 2** | Cosmetic only; low priority but worth bundling with the failure-path consolidation. |
| F6 | major | `tox.ini` ↔ `conftest.py` ↔ `test_core_pytest.py` | **accept, defer to round 2** | Larger refactor (introducing `CoreContext` dataclass). Better in its own commit. |
| F7 | minor | `test_core_pytest.py:42-47` | **accept now** | One-line dead-code deletion. Belongs with F4. |
| F8 | minor | `test_core_pytest.py:51-56` | **accept, defer to round 2** | Bundle with F6 (the `core_context` fixture). |
| F9 | major | `conftest.py:86-151` | **partial accept** | The current `pytest_generate_tests` is fine in shape; the simplification is real but invasive. Mark as round-3 work; for now leave alone. |
| F10 | minor | `conftest.py:73-83` | **reject** | The custom `--core-tests`/`--core-graphs` options are documented in `TESTING.md` and used in shell muscle memory. `-k` works for new users; keep both for ergonomics. Removing them is breakage-without-benefit. |

## Plan of changes for this round

1. **Replace `os.environ.clear()`/`update()` with `monkeypatch.setenv`** (F4):
   - Hoist env construction into a helper.
   - The test function gets `monkeypatch` and `tmp_path` fixtures.
   - `_run_flow` no longer mutates `os.environ` directly.
2. **Delete `_skip_api_executor` dead variable** (F7):
   - Remove the `try/except ImportError` wrapping `from metaflow import Runner`. If api executor is selected and Runner can't be imported, fail loudly at the api branch via `pytest.importorskip`.

Both changes preserve the existing public API: tox env vars are still consumed, Runner is still used, FlowDefinition+graph generator is unchanged. Test parametrisation is unchanged.

## Test strategy

- `pytest test/core/test_core_pytest.py --collect-only -q | tail -3` should still report **502 tests collected**.
- For execution verification: the venv lacks docker/MinIO/etc., but `core-local` runs with no infrastructure. Run `pytest test/core/test_core_pytest.py -m local -k "BasicArtifactTest and single-linear-step" --tb=short` as a smoke test.

## Test-speed impact this round
None — F7 is a logical refactor, no algorithmic change.

## Scope adjustment
F4 (monkeypatch.setenv refactor) is larger than initially scoped — it requires restructuring the try/finally nesting because two of them (tempdir cleanup, env restoration) become redundant. Deferred to **Round 2** to keep round 1's commit small and reviewable. F7 alone is shipped this round.

## Verification

```
$ /home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py --collect-only -q | tail -3
-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
========================= 502 tests collected in 0.27s =========================
```

Collection unchanged — same 502 items in the same time. The dead `_skip_api_executor` was never read after the PR's refactor; deleting it is provably safe.
