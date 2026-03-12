# Overlapping Submodule Promotions in `metaflow_extensions`

This directory contains examples that demonstrate how the Metaflow extension
system now handles **overlapping submodule promotions** – the situation where
two or more extension packages promote the same alias via
`__mf_promote_submodules__`.

## Background

Metaflow extensions are Python packages under the `metaflow_extensions`
namespace.  Each extension can *promote* submodules so they become accessible
under a short `metaflow.*` alias (e.g. `metaflow.plugins.datatools` →
`metaflow_extensions.my_org.plugins.datatools`).

The promotion is declared by setting `__mf_promote_submodules__` in the
extension's config module:

```python
# metaflow_extensions/my_org/plugins/mfextinit_my_org.py
__mf_promote_submodules__ = ["datatools"]
```

### The Problem (Issue #3011)

Before this fix, if **two** extension packages both promoted the same alias
(e.g. both declared `["datatools"]`), the conflict was handled **silently** –
the second package's target would simply overwrite the first one with no
indication to the user.

### The Fix

The extension system now:

1. **Detects** overlapping promotions from *different* packages.
2. **Emits a `UserWarning`** that identifies both the overriding and overridden
   packages, the alias in question, and the concrete target modules.
3. **Resolves deterministically**: the **last-loaded** package wins. Load order
   is determined by topological sort of package dependencies, then alphabetical
   order for ties – exactly as it was before, but now *documented* and *visible*.
4. If the **same** package re-promotes the same alias (e.g. loaded twice), no
   warning is emitted because there is no actual conflict.

## Examples in this Directory

| File | Description |
|------|-------------|
| `01_single_extension_no_conflict.py` | Baseline: a single extension promoting submodules works cleanly. |
| `02_two_extensions_overlap_warning.py` | Two extensions promote the same alias; demonstrates the warning. |
| `03_partial_overlap.py` | Two extensions with only *some* overlapping aliases. |
| `04_three_way_overlap.py` | Three extensions competing for the same alias; two warnings are emitted. |
| `05_same_package_repromotion.py` | Same package promoting twice – no warning. |

## How to Run

Each example is a standalone Python script.  Run from the repository root:

```bash
python3 examples/overlapping_submodule_promotions/01_single_extension_no_conflict.py
```

> **Note:** These examples use the *internal* `alias_submodules` API with
> synthetic (fake) modules.  They are designed to be educational – they show
> exactly what happens inside the extension system without requiring you to
> install actual extension packages.
