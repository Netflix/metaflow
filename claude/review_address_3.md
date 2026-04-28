# Address — Round 3

## Summary
Apply F11–F14 from the round-3 review: drop dead `MetaflowTest`/`Expectation*`/`Assert*Failed` symbols and unused module-top imports.

## Per-finding verdicts

| ID | Verdict | Reasoning |
|----|---------|-----------|
| F11 | accept, applied | Alias has zero callers — safe delete. Conftest filter updated to drop the now-stale `"MetaflowTest"` string. |
| F12 | accept, applied | 59 test files imported `ExpectationFailed`; zero raised it. `sed` strip + class delete is mechanical. |
| F13 | accept, applied | `Assert{Artifact,Log,Card}Failed` imported into both checkers, raised by neither. Identical pattern, identical fix. |
| F14 | accept, applied | `import sys`/`import os` at top of `metaflow_test/__init__.py` were unused. Reordered remaining imports alphabetically while we're touching the block. |

## Changes applied

`test/core/metaflow_test/__init__.py`:
- Drop `import sys`, `import os` (unused).
- Reorder remaining metaflow imports alphabetically.
- Delete `class AssertArtifactFailed`, `class AssertLogFailed`, `class AssertCardFailed`, `class ExpectationFailed`.
- Delete `MetaflowTest = FlowDefinition` alias.

`test/core/metaflow_test/cli_check.py`:
- Replace `from . import (MetaflowCheck, AssertArtifactFailed, AssertLogFailed, truncate, AssertCardFailed)` with `from . import MetaflowCheck, truncate`.

`test/core/metaflow_test/metadata_check.py`:
- Same one-line collapse.

`test/core/conftest.py:41`:
- `name not in ("MetaflowTest", "FlowDefinition")` → `name != "FlowDefinition"`.

`test/core/tests/*.py` (59 files):
- `sed -i 's/, ExpectationFailed//' test/core/tests/*.py` — strip the dead import.

## Verification

```
$ /home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py --collect-only -q | tail -3
========================= 502 tests collected in 0.32s =========================

$ PATH=/home/coder/.venv/bin:$PATH METAFLOW_CORE_MARKER=local METAFLOW_USER=tester \
    METAFLOW_CORE_TOP_OPTIONS="--metadata=local --datastore=local --environment=local --event-logger=nullSidecarLogger --no-pylint --quiet" \
    METAFLOW_CORE_EXECUTORS=cli \
    /home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py -m local \
    -k "BasicArtifact and single-linear-step and cli" --tb=short
================= 1 passed, 250 deselected, 1 warning in 2.98s =================
```

Collection still 502, smoke pass.

## Test-speed impact this round

Negligible — pure dead-code removal. Imports a few microseconds faster per test file (no ExpectationFailed lookup), but unmeasurable in practice.
