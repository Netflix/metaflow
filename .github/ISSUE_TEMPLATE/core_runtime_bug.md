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
- [ ] Datastore (`datastore/`, `plugins/datastores/`)
- [ ] Metadata (`metadata_provider/`, `plugins/metadata_providers/`)
- [ ] AWS client / credentials (`plugins/aws/aws_client.py`, `plugins/datatools/s3/`)
- [ ] Config / parameters (`metaflow_config.py`, `parameters.py`, `user_configs/`)
- [ ] Logging / capture (`mflog/`, `system/`, `debug.py`)
- [ ] Decorators (core) (`decorators.py`)
- [ ] Graph / DAG (`graph.py`)
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

## Suspected Root Cause (Optional)

If you have a hypothesis, describe what may be happening and why.

## Related Scenarios / Edge Cases (Optional)

Are there adjacent scenarios that might also be affected?

## Environment

- OS:
- Python version:
- Metaflow version:
- Related integrations/services:

## Additional Context

Any other details that may help maintainers.
