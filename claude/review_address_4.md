# Address — Round 4

## Summary
F1 remainder (scheduler executor) + 4 more migrations.

## Changes

- Added `_run_scheduler(...)` to conftest.py: runs `<scheduler> create`,
  `<scheduler> trigger --run-id-file ...`, then polls
  `Flow(name)[run_id].finished` with a 600 s default timeout. Falls
  back to `pytest.skip` when `METAFLOW_CORE_SCHEDULER` isn't set so the
  executor fixture's `params=["scheduler"]` (from argo/sfn tox envs)
  doesn't fail to collect on local runs.
- New tests:
  - `test_project_branch` — @project branch_name default user.<USER>.
  - `test_resume_foreach_split` — resume from start re-runs all branches.
  - `test_resume_foreach_join` — resume from join re-runs join only.
  - `test_switch_in_branch` — switch nested inside top-level branch.
- `test_basic_config_parameters` attempted but deferred; metaflow Config
  with default-dict access requires more setup than the simple test
  could provide.

## Verification

```
$ pytest test/core/tests/ -q
================== 85 passed, 1 warning in 166.86s ===================
```

Up from 75 in round 3 → 85 in round 4. Roughly 42-43 test functions × 2 executors.

## Migration progress
Migrated source tests (count, by category):
- basic_*: 6 (artifact, foreach, log, parallel, parameters, tags, unbounded_foreach)
- resume_*: 5 (end_step, start_step, succeeded_step, foreach_inner, foreach_join, foreach_split, originpath)
- switch_*: 4 (basic, nested, recursive_switch, in_branch)
- card_*: 3 (simple, multiple, id_append)
- core decorators: catch_retry, timeout_decorator, runtime_dag, lineage, task_exception
- params/tags: param_names, dynamic_parameters, basic_tags, tag_mutation, run_id_file
- merge: merge_artifacts, merge_artifacts_propagation, nested_foreach, wide_foreach, current_singleton, constants, flow_options, project_branch

= 36 source tests migrated; 33 of the original 69 not yet ported. The
remaining ones are mostly card variants, project/secrets/custom_decorators,
the foreach_in_switch / switch_in_foreach cross combinations, and the
unbounded foreach resume variants. They follow the same patterns as the
migrated ones.

## Carried forward to round 5
- Final cross-cutting verification.
- Update gist.
- Document remaining migrations as TODOs or a follow-up commit.
