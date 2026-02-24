---
name: Feature Request
about: Propose a new feature or meaningful enhancement
title: "[Feature] <short description>"
labels: enhancement
assignees: ""
---

<!--
Before opening a feature request:
  - Search existing issues and discussions for duplicates.
  - For Core Runtime changes (runtime.py, task.py, orchestrators, etc.)
    discuss in the chatroom first: http://chat.metaflow.org
-->

## Problem / Motivation

<!--
Describe the concrete problem you are trying to solve.
Focus on the user-visible pain point, not the solution.
e.g. "When running a flow on Kubernetes I cannot easily inspect intermediate
     artifacts without writing custom code, because..."
-->

## Proposed Solution

<!--
What would you like to see added or changed?
Describe the user-facing behavior: API shape, CLI flags, decorator arguments,
output format — whatever is relevant.
-->

**Example usage (if applicable):**

```python
# Show what using the feature would look like
```

## Alternatives Considered

<!--
What other approaches did you consider and why did you rule them out?
This helps maintainers understand the design space.
-->

## Affected Area

<!-- Check all that apply -->

- [ ] Core Runtime (execution engine, subprocess, datastore, orchestrators)
- [ ] CLI / runner
- [ ] Decorators / user-facing API
- [ ] Plugins (AWS, Kubernetes, Argo, etc.)
- [ ] Documentation / examples
- [ ] Tooling / dev experience

## Additional Context

<!--
Links to related issues, PRs, or external references.
Screenshots or diagrams if helpful.
-->

## Checklist

- [ ] I searched existing issues and discussions — this is not a duplicate
- [ ] I described the problem, not just the solution
- [ ] For Core Runtime features: I am prepared to discuss design before implementation
