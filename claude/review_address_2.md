# Address — Round 2

## Summary
Self-conducted round per the deferral plan from round 1. Focus: F1 (per-backend executor opt-in via tox env), F5 (parameter coverage expansion), F7 (merge_artifacts negative paths), and a generic harness fix for tests with multiple FlowSpec classes per file.

## Changes applied

1. **F1 partial — `METAFLOW_CORE_EXECUTORS` per tox env.**
   - Added `METAFLOW_CORE_EXECUTORS = cli,api` to local/azure/gcs/batch/k8s; `= scheduler` to argo/sfn.
   - `executor` fixture now reads `METAFLOW_CORE_EXECUTORS` (default cli,api) and parametrizes accordingly.
   - The `scheduler` value isn't yet implemented in `metaflow_runner` — that's the remainder of F1, deferred to round 4 (needs a real argo create/trigger + Flow polling adapter).
2. **F5 — `test_basic_parameters` coverage restored.**
   - Added `bool_default_true`, `no_default_param`, `list_param`, `json_param` (JSONType).
   - Added `test_basic_parameters_immutable` that verifies reassignment is rejected and the original value persists.
   - `test_basic_parameters_env_override` now covers both `METAFLOW_RUN_BOOL_PARAM` and `METAFLOW_RUN_NO_DEFAULT_PARAM` plus an explicit `--int_param` override.
3. **F7 — `test_merge_artifacts` negative paths.**
   - Added `test_merge_artifacts_conflict_fails` (different values for `shared` on left/right → run fails).
   - Added `test_merge_artifacts_include_and_exclude_fails` (passing both options → run fails).
   - Original happy-path moved to `test_merge_artifacts_happy_path`.
4. **Generic harness fix — multiple FlowSpec per test file.**
   - `_write_flow_module` was emitting the entire test module, so files with 2+ FlowSpec subclasses (legitimately, one per scenario) tripped Runner's "Multiple FlowSpec classes found" check.
   - Now extracts only the imports/top-level helpers (everything up to the first `@…` decorator or `class …` line at column 0) plus `inspect.getsource(flow_cls)` for the target class.
   - This required a second fix: the previous prologue cutoff at `class ... FlowSpec` left the `@project(name=…)` decorator above the class, which then duplicated when `getsource(flow_cls)` re-included it.

## Verification

```
$ pytest test/core/tests/ -q
================== 69 passed, 1 warning in 134.02s (0:02:14) ===================
```

35 test functions × 2 executors (≈) = 69 items. Up from 64 in round 1.

## Test-speed impact this round
None for individual tests. Adding two negative-path merge_artifacts cases adds ~10s to suite time.

## Deferred to round 3
- F6: resume metadata correctness (origin-run-id, origin-task-id) on `test_resume_end_step` and friends.
- F9: catch+retry full coverage (attempt metadata, invisible artifacts, split/join retry).
- More test migrations (resume_foreach_inner, resume_originpath, etc.).

## Deferred to round 4
- F1 remainder: real `scheduler` executor adapter in `metaflow_runner`.
- Card test migrations (test_card_default_editable, test_card_error, etc.).

## Deferred to round 5
- Final cross-cutting verification + gist update.
