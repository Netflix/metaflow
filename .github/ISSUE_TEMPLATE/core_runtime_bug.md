---
name: Core runtime bug
about: Report a bug in core runtime paths (execution, runner, datastore, metadata, orchestrators)
title: "[BUG][CORE RUNTIME] "
labels: bug, core-runtime
assignees: ""
---

## Summary

Describe the user-visible issue in one to two sentences.

## Why This Is Core Runtime

Select all that apply:

- [ ] Execution engine (`runtime.py`, `task.py`, `flowspec.py`)
- [ ] Runner / subprocess management (`runner/`)
- [ ] CLI plumbing (`cli.py`, `cli_components/`, `runner/click_api.py`)
- [ ] Datastore / metadata (`datastore/`, `metadata_provider/`)
- [ ] Orchestrator plugins (`plugins/argo`, `plugins/aws/batch`, `plugins/aws/step_functions`, `plugins/kubernetes`)
- [ ] Other core runtime area (specify below)

If other, specify:

## Minimal Reproduction (Required)

Provide the smallest runnable example and exact command(s).

```python
# Minimal repro flow/script
```

```bash
# Exact commands used to reproduce
```

## Runtime + Evidence (Required)

- Runtime used (local / kubernetes / batch / argo / step-functions / airflow):
- Where the failure is observed (parent console / task logs / UI logs / metadata):
- Relevant links or output:

```text
# Full traceback / logs
```

## Root Cause Analysis (Required)

What invariant is violated and why does this happen?

## Why This Fix Is Correct (Required)

Explain the fix and why it addresses the root cause.

## Failure Modes Considered (Required)

List at least two edge cases or failure modes you considered.

1.  
2.  

## Environment

- OS:
- Python version:
- Metaflow version:
- Related integrations/services:

## Additional Context

Any other details that may help maintainers.
