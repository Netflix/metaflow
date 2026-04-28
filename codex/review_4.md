# Round 4 Review (self-conducted)

Focus: F1 remainder (scheduler executor adapter) + more migrations.

## Findings addressed

- F1 remainder: `_run_scheduler` adapter added to conftest. Implements
  the argo-workflows / step-functions create + trigger + Flow.finished
  poll cycle (mirrors the original run_tests.py behaviour). Scheduler
  branch in `metaflow_runner` calls into it; `pytest.skip` if
  METAFLOW_CORE_SCHEDULER unset (so the executor fixture parametrise
  doesn't error on local runs).

## New migrations
- test_project_branch (project branch_name)
- test_resume_foreach_split (resume re-runs all foreach branches)
- test_resume_foreach_join (resume re-runs the join)
- test_switch_in_branch (switch inside a branch)

(test_basic_config_parameters attempted but deferred — Config()
default-dict access pattern needs tox-level setup we don't have here.)

## Carried forward to round 5
- Final cross-cutting verification.
- Gist update.
- ~25 source tests still unmigrated; remaining batches need more
  attention to per-test specifics (cards, secrets, custom_decorators,
  extensions, project_production, etc.).
