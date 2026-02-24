---
name: Core Runtime Bug
about: >
  Bug in the execution engine, subprocess management, datastore, metadata,
  orchestrator plugins, or other Core Runtime paths. Read the higher bar
  requirements in CONTRIBUTING.md before filing.
title: "[Core Runtime] <short description>"
labels: bug, core-runtime
assignees: ""
---

<!--
Core Runtime covers (see CONTRIBUTING.md for the authoritative list):

  Execution engine:      metaflow/runtime.py, metaflow/task.py, metaflow/flowspec.py
  Subprocess / runner:   metaflow/runner/metaflow_runner.py,
                         metaflow/runner/subprocess_manager.py,
                         metaflow/runner/deployer_impl.py,
                         metaflow/runner/click_api.py
  CLI plumbing:          metaflow/cli.py, metaflow/cli_components/
  Datastore:             metaflow/datastore/, metaflow/plugins/datastores/
  Metadata:              metaflow/metadata_provider/, metaflow/plugins/metadata_providers/
  AWS client / S3:       metaflow/plugins/aws/aws_client.py, metaflow/plugins/datatools/s3/
  Config / parameters:   metaflow/metaflow_config.py, metaflow/parameters.py,
                         metaflow/user_configs/
  Logging / capture:     metaflow/mflog/, metaflow/system/, metaflow/debug.py
  Decorators (core):     metaflow/decorators.py
  Graph / DAG:           metaflow/graph.py
  Orchestrators:         metaflow/plugins/argo/, metaflow/plugins/aws/batch/,
                         metaflow/plugins/aws/step_functions/,
                         metaflow/plugins/kubernetes/

If your bug is in user-facing APIs or a non-core plugin, use the standard
Bug Report template instead.
-->

## Problem Statement

<!-- One paragraph: what user-visible behavior is wrong and why it matters. -->

## Minimal Reproduction

<!--
Provide a flow that fails in the REAL execution mode (not just locally if the
bug is Kubernetes/Batch/Argo). Pseudocode is not acceptable here.
-->

```python
# minimal_flow.py
from metaflow import FlowSpec, step

class MinimalFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    MinimalFlow()
```

**Exact commands:**

```bash
python minimal_flow.py run --with kubernetes   # adjust runtime flag
```

**Runtime:** <!-- local / kubernetes / batch / argo / step-functions -->

**Where evidence shows up:** <!-- parent console / task logs / metadata service / UI -->

<details>
<summary>Error / log output (before fix)</summary>

```
paste here
```

</details>

## Root Cause Analysis

<!--
Required. Explain:
  1. Which invariant was violated (e.g. "atomic write guarantee broken by X")
  2. The exact code path (file:line) where the failure occurs
  3. Why this failure is not caught by existing tests
-->

## Proposed Fix (optional)

<!--
If you already have a fix in mind, describe it here.
Why is it correct? Why is it minimal?
Open a PR and link it if you have code ready.
-->

## Failure Modes Considered

<!--
Required. List at least two failure modes your analysis or fix accounts for.
Examples: concurrency/retries, subprocess boundary propagation,
env-var leakage, backward compatibility, partial writes, signal handling.
-->

1.
2.

## Environment

| Field | Value |
|---|---|
| OS | <!-- e.g. Ubuntu 22.04 --> |
| Python version | <!-- python --version --> |
| Metaflow version | <!-- python -c "import metaflow; print(metaflow.__version__)" --> |
| Runtime | <!-- local / kubernetes / batch / argo --> |

## Checklist

- [ ] I confirmed this is in a Core Runtime path (see paths above)
- [ ] I have a minimal reproduction that fails in the real runtime
- [ ] I have identified the failing code path (file and line)
- [ ] I have listed at least two failure modes considered
- [ ] I searched existing issues and this is not a duplicate
