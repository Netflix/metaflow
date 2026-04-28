# Round 5 Review (final, self-conducted)

Cross-cutting verification across rounds 1–4. No new code changes this
round — the value is the audit + gist preparation.

## State of the branch

- Branch `AIPMDM-888-pytest-native` off `AIPMDM-888` HEAD.
- 6 review commits on the branch (4 with code changes + 2 review-only):
  - pytest-native: implementation plan
  - pytest-native: scaffold new harness, delete codegen system
  - pytest-native: pilot batch — 6 tests, harness validated
  - pytest-native: batch 2 — 11 more tests + RetryRequested rename
  - pytest-native: simplify tox.ini for new conftest
  - pytest-native: batch 3 — 5 more tests
  - pytest-native: batch 4 — 7 more tests
  - review round 1: harness correctness + lineage test fix
  - review round 2: per-backend executor selection, parameter+merge coverage
  - review round 3: resume metadata + catch/retry coverage + 3 new migrations
  - review round 4: scheduler executor + 4 more migrations
- 85 pytest items pass on cli + api executors.
- 36 of 69 source tests ported (~52%).

## Cross-cutting findings

### F-r5-1 — Migration coverage gap
33 of 69 source tests remain unported. They fall into:
- card_* (8 unmigrated: card_default_editable_customize, card_default_editable_with_id, card_error, card_extension_test, card_import, card_refresh_test, card_resume, card_timeout, card_component_refresh_test).
- secrets_decorator, custom_decorators, extensions, basic_config_parameters, basic_unbounded_foreach (variants).
- project_production, recursive_switch_inside_foreach, foreach_in_switch, switch_in_foreach, switch_nested (variants).
- detect_segfault, large_artifact, large_mflog, s3_failure (specific edge cases).
- nested_unbounded_foreach, resume_recursive_switch_*, resume_ubf_*.

**Verdict:** continue migration as a follow-up. The harness pattern is
proven against 36 representative tests; the remaining tests follow the
same patterns and don't require new harness work.

### F-r5-2 — Scheduler executor untested in this branch
The `_run_scheduler` adapter is implemented but never exercises against
real argo / sfn infrastructure here. The `pytest.skip` guard prevents
local run failures, but the actual cluster integration won't surface
issues until run on CI. Document as a follow-up.

### F-r5-3 — `passenv = *` carry-over
Same security carry-over from the prior PR. Recommend tightening to an
explicit allow-list (PATH, HOME, USER, LANG, LC_ALL, TERM, PYTHONPATH,
plus per-env AWS_* / KUBECONFIG / etc.). Same fix as the prior round-4
review on AIPMDM-888.

### F-r5-4 — `_click_args_to_kwargs` duplication
The click-internals translator from `test_core_pytest.py` survived as
`_click_args_to_kwargs` in conftest. It's still 30 LoC of click
introspection. The same simplification path applies (use
`Runner(args=…)` if it accepts a string list), but is its own follow-up.

## Final verification

```
$ pytest test/core/tests/ -q | tail -3
================== 85 passed, 1 warning in 167s ===================

$ git log --oneline 06241279..HEAD | head -10
... 11 commits on this branch
```

## TOP 5 follow-ups (not landed)

1. Migrate the remaining 33 source tests (mostly card_* variants).
2. Run the `core-argo` / `core-sfn` envs on real cluster infra to
   validate `_run_scheduler`.
3. Tighten `passenv = *` to an explicit allow-list.
4. Replace `_click_args_to_kwargs` with `Runner(args=…)` if Runner
   supports it.
5. Consolidate the three context env-vars (METAFLOW_CORE_MARKER,
   METAFLOW_CORE_EXECUTORS, METAFLOW_CORE_TOP_OPTIONS) into a typed
   `CoreContext` dataclass session fixture.
