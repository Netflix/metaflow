# Round 2 Review (self-conducted, fast)

Codex's round-1 file landed; this round focuses on applying the deferred majors and a generic harness regression that surfaces only when a test file has more than one FlowSpec subclass.

## Findings

### F-r2-1 — Need `METAFLOW_CORE_EXECUTORS` per tox env, not hard-coded `[cli, api]`
**File:** `test/core/conftest.py:99-101` (current).
**Fix:** Read from env, default to `cli,api`, allow tox to override (`scheduler` for argo/sfn).
**Verdict:** apply.

### F-r2-2 — `_write_flow_module` emits whole module → "Multiple FlowSpec classes found"
**File:** `test/core/conftest.py:_write_flow_module`.
**Fix:** prologue stops at first `@…` or `class …` at column 0; flow source is `inspect.getsource(flow_cls)`.
**Verdict:** apply.

### F-r2-3 — `test_basic_parameters` lost coverage (per F5 from round 1)
**Verdict:** apply.

### F-r2-4 — `test_merge_artifacts` lost negative paths (per F7 from round 1)
**Verdict:** apply.

## Carried forward
- F1 remainder (scheduler executor).
- F6 (resume metadata).
- F9 (catch+retry full coverage).
- Migration of remaining 35 source tests.
