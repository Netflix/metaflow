## PR Type

<!-- Check one -->

- [ ] Bug fix
- [ ] New feature
- [ ] Core Runtime change (higher bar -- see [CONTRIBUTING.md](../CONTRIBUTING.md#core-runtime-contributions-higher-bar))
- [ ] Docs / tooling
- [ ] Refactoring

## Summary

<!-- What user-visible behavior changes? 1-2 sentences. -->

## Issue

<!-- Link to issue. Required for bug fixes, required for Core Runtime. -->

Fixes #

## Reproduction

<!-- Required for bug fixes. Required for Core Runtime changes. -->
<!-- Provide a minimal reproduction that fails before and succeeds after. -->

**Runtime:** <!-- local / kubernetes / batch / argo / etc. -->

**Commands to run:**
```bash
# paste exact commands
```

**Where evidence shows up:** <!-- parent console / task logs / metadata / UI -->

<details>
<summary>Before (error / log snippet)</summary>

```
paste here
```

</details>

<details>
<summary>After (evidence that fix works)</summary>

```
paste here
```

</details>

## Root Cause

<!-- Required for Core Runtime. Recommended for all bug fixes. -->
<!-- Explain the causal chain: what invariant was violated, where in the code. -->
<!-- See PRs #2796, #2751, #2714 for examples of the level of detail we're looking for. -->

## Why This Fix Is Correct

<!-- What invariant is restored? Why is the fix minimal? -->

## Failure Modes Considered

<!-- Required for Core Runtime (at least 2). Recommended for all bug fixes. -->
<!-- Examples: concurrency/retries, subprocess output propagation, env-var leakage, backward compat -->

1.
2.

## Tests

- [ ] Unit tests added/updated
- [ ] Reproduction script provided (required for Core Runtime)
- [ ] CI passes
- [ ] If tests are impractical: explain why below and provide manual evidence above

## Non-Goals

<!-- What you intentionally did not change. Helps reviewers scope their review. -->

## AI Tool Usage

<!-- We welcome responsible AI use. See CONTRIBUTING.md for our full policy. -->

- [ ] No AI tools were used in this contribution
- [ ] AI tools were used (describe below)

<!-- If you used AI tools:
- Which tool(s)?
- What did you use them for?
- Did you review, understand, and test all generated code?
-->
