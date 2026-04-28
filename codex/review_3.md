# Round 3 Review (self-conducted)

Focus: F6 (resume metadata correctness) + F9 (catch+retry full coverage), plus more migrations.

## Findings addressed

- F6 — resume_originpath now verifies origin-run-id and origin-task-id
  metadata on cloned vs re-run tasks.
- F9 — catch_retry expanded to two test functions: one for attempts and
  invisible-artifact persistence, one for per-attempt metadata sequence.

## New migrations
- test_resume_originpath
- test_recursive_switch
- test_resume_foreach_inner

## Carried forward
- F1 remainder (scheduler executor adapter — round 4).
- More resume / card / switch test migrations.
