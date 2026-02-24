---
name: Bug Report
about: Report a reproducible bug in Metaflow
title: "[Bug] <short description>"
labels: bug
assignees: ""
---

## Summary

<!-- One or two sentences: what went wrong and what you expected instead. -->

## Steps to Reproduce

<!--
Provide the simplest possible flow that triggers the bug.
Paste real code — not pseudocode.
-->

```python
# minimal_flow.py
from metaflow import FlowSpec, step

class MinimalFlow(FlowSpec):
    @step
    def start(self):
        # paste failing code here
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    MinimalFlow()
```

**Command(s) run:**

```bash
# paste exact commands
python minimal_flow.py run
```

**Runtime:** <!-- local / kubernetes / batch / argo / step-functions -->

**Where evidence shows up:** <!-- parent console / task logs / metadata / UI -->

## Actual Behavior

<details>
<summary>Full error / log output</summary>

```
paste here
```

</details>

## Expected Behavior

<!-- What should have happened instead? -->

## Environment

| Field | Value |
|---|---|
| OS | <!-- e.g. Ubuntu 22.04 / macOS 14.3 --> |
| Python version | <!-- python --version --> |
| Metaflow version | <!-- python -c "import metaflow; print(metaflow.__version__)" --> |
| Deployment | <!-- local / AWS Batch / Kubernetes / Argo / Step Functions --> |
| Cloud provider | <!-- AWS / GCP / Azure / none --> |

## Additional Context

<!--
Anything else that might help: config snippets, decorator stack,
relevant parts of `~/.metaflowconfig`, etc.
-->

## Checklist

- [ ] I searched existing issues and this is not a duplicate
- [ ] I can reproduce this with the minimal flow above
- [ ] I have included the full error output / stack trace
- [ ] I have filled in all environment fields above
