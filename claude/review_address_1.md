# Address — Round 1 (pytest-native branch)

## Summary
Codex flagged 9 findings (1 critical, 7 major, 1 minor). Three are harness
correctness issues that affect every test; six are coverage regressions
in specific migrated tests. Plan: fix harness in this round (F2/F3/F4)
plus F8 (lineage); defer F1 (scheduler executor), F5/F6/F7/F9 (test
coverage restorations) to subsequent rounds where they bundle better
with each test category's broader migration work.

## Per-finding verdicts

| ID | Severity | Verdict | Reasoning |
|----|----------|---------|-----------|
| F1 | critical | defer to round 2 | Scheduler executor needs a real `_run_scheduler` adapter (argo create/trigger + Flow polling). Substantial; deserves its own commit. |
| F2 | major | **apply now** | `{nonce}` expansion is required for every cloud-backend run; one-line fix in the runner fixture. |
| F3 | major | **apply now** | `run_id_file.unlink(missing_ok=True)` before each invocation — three lines. |
| F4 | minor | **apply now** | `TestRetry.__test__ = False` — one line. |
| F5 | major | defer to round 3 | Test expansion: add `no_default_param`, `bool_true_param`, `list_param`, `json_param`; assert immutability. Bundle with full parameters category review. |
| F6 | major | defer to round 4 | Resume metadata assertions across origin-run-id/origin-task-id. Bundle with the full resume-tests batch. |
| F7 | major | defer to round 3 | Negative-path tests for merge_artifacts (conflict, include+exclude, outside-join). Bundle with parameters/merge expansion. |
| F8 | major | **apply now** | Test docstring explicitly claims to verify lineage; current body doesn't. Easy to fix the body in this round. |
| F9 | major | defer to round 4 | Catch+retry needs split/join + invisible-artifact + attempt metadata assertions. Bundle with full retry/catch expansion. |

## Plan of changes for this round

1. **F2 — `{nonce}` expansion in runner fixture.**
   In `metaflow_runner`, before passing env to subprocess/Runner, walk
   `os.environ.items()`, replace `{nonce}` (and `{{nonce}}`) with a
   per-call `uuid.uuid4().hex`, monkeypatch.setenv each replaced key.
2. **F3 — Delete `run-id` between runs.**
   Just before each `subprocess.run(...)` (cli) and `runner.run/resume(...)`
   (api), `run_id_file.unlink(missing_ok=True)`.
3. **F4 — Suppress pytest collection of `TestRetry` alias.**
   Add `TestRetry.__test__ = False` after the assignment.
4. **F8 — `test_lineage` actually tests lineage.**
   Use `task.code` and parent_task lineage queries to verify the run
   has the right ancestry. (Or compute `current.pathspec` chains in the
   flow and compare to `task.parent_tasks`.)

## Test strategy
- After each change: `pytest --collect-only -q` + the affected smoke run.
- Final: full suite (~64 items) on cli + api.

## Test-speed impact this round
Negligible — 4 surgical changes.
