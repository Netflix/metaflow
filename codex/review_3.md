# Round 3 Review — `test/core/metaflow_test/{__init__.py,cli_check.py,metadata_check.py}` and dead imports across `tests/`

## Context
- HEAD: `fdc496ee review round 2`. Round 1 deleted `_skip_api_executor`; round 2 swapped tempdir+os.environ for pytest fixtures.
- Baseline: 502 tests collected.

This round audits the framework layer + cleans dead imports that proliferate across the test directory.

## Findings

### F11 — `MetaflowTest = FlowDefinition` alias has zero internal callers
**Severity:** minor
**File:** `test/core/metaflow_test/__init__.py:144`

```python
# Backward-compatibility alias — existing tests that still import MetaflowTest will work.
MetaflowTest = FlowDefinition
```

Grep shows no `from metaflow_test import MetaflowTest`, no `MetaflowTest(` instantiation, no `MetaflowTest)` subclassing anywhere in the repo (the matches inside `metaflow_extensions/test_org/exceptions/mfextinit_test_org.py:MetaflowTestException` are a different identifier — substring false-positive). The alias was added during the `MetaflowTest → FlowDefinition` rename and every test was migrated; the alias is dead.

`conftest.py:41` filters classes named "MetaflowTest" out of iteration — that filter becomes redundant once the alias is gone.

**Suggested fix:** Remove the alias. Update `conftest.py:41` to drop "MetaflowTest" from the exclusion tuple.

```python
# metaflow_test/__init__.py
# DELETE: MetaflowTest = FlowDefinition

# conftest.py
# BEFORE:
if (
    name not in ("MetaflowTest", "FlowDefinition")
    and isinstance(obj, type)
    ...
)
# AFTER:
if (
    name != "FlowDefinition"
    and isinstance(obj, type)
    ...
)
```

**Rationale:** Dead public surface. Costs nothing to keep, but every minute someone spends wondering "is this my hook" is a bad minute.

---

### F12 — `ExpectationFailed` is imported by 59/64 test files but raised by zero
**Severity:** minor
**File:** `test/core/tests/*.py` (59 files); class def at `metaflow_test/__init__.py:81-87`

```bash
$ grep -l "ExpectationFailed" test/core/tests/ | wc -l
59
$ grep -lE "raise ExpectationFailed|ExpectationFailed\(" test/core/tests/*.py | wc -l
0
```

Every test file does `from metaflow_test import FlowDefinition, ExpectationFailed, steps[, tag]` but no test raises `ExpectationFailed` anywhere. The class itself extends `AssertionError` and exists "for backward compatibility" per its own docstring.

**Suggested fix:** Strip the import from every test file with a single `sed -i 's/, ExpectationFailed//' test/core/tests/*.py`. Delete the class from `metaflow_test/__init__.py`.

**Rationale:** 59 files updated, ~59 lines net (one `, ExpectationFailed` removed each). Net reduction in noise; nothing breaks because nothing raises it.

---

### F13 — `AssertArtifactFailed`, `AssertLogFailed`, `AssertCardFailed` imported by checkers but never raised
**Severity:** minor
**File:** `test/core/metaflow_test/cli_check.py:9-16`, `test/core/metaflow_test/metadata_check.py:7-13`

```python
# cli_check.py
from . import (
    MetaflowCheck,
    AssertArtifactFailed,
    AssertLogFailed,
    truncate,
    AssertCardFailed,
)
```

Each is a no-op subclass of `AssertionError`. None of `AssertArtifactFailed`, `AssertLogFailed`, `AssertCardFailed` is `raise`d anywhere in the codebase (verified by grep across `test/core/`). They exist in `__init__.py` and are imported into the checkers, but every checker uses bare `assert` statements (which raise plain `AssertionError`).

**Suggested fix:** Drop the four imports across `cli_check.py` and `metadata_check.py`. Drop the three class definitions from `__init__.py:69-87` (and `ExpectationFailed` per F12).

**Rationale:** Dead — and dead in a way that misleads (a future contributor will assume there's a reason these exist).

---

### F14 — `metaflow_test/__init__.py` imports six things at module top that are not all used
**Severity:** nit
**File:** `test/core/metaflow_test/__init__.py:1-6`

```python
import sys
import os
from metaflow.exception import MetaflowException
from metaflow import current
from metaflow.cards import get_cards
from metaflow.plugins.cards.exception import CardNotPresentException
```

`sys` is unused in the module. `os` is unused after F12/F13 cleanup (only `import os` was used by other helpers if any).

**Verdict for round 3:** delete unused imports as part of this cleanup commit.

---

### F15 — `truncate` referenced by name from cli_check.py / metadata_check.py
**Severity:** nit
**File:** `test/core/metaflow_test/__init__.py:29-33`

```python
def truncate(var):
    var = str(var)
    if len(var) > 500:
        var = "%s..." % var[:500]
    return var
```

Used by checker error messages. Keep — has real callers.

## Verification plan

Same as before:
- `pytest --collect-only` → expect 502.
- Smoke run: `BasicArtifact / single-linear-step / cli`.
- Spot-check: ensure no test file errors on `ImportError: cannot import name 'ExpectationFailed'` after the sed pass.

## TOP simplifications (round 3)

1. **Drop `MetaflowTest = FlowDefinition` alias + matching `conftest.py` filter.** (F11)
2. **Strip `ExpectationFailed` import from 59 test files** + delete class. (F12)
3. **Drop `Assert{Artifact,Log,Card}Failed` from checker imports** + delete classes. (F13)
4. **Drop unused module-top imports in `metaflow_test/__init__.py`.** (F14)

These changes alone net ~60+ LOC of dead surface across the framework.
