# Implementation Plan — pytest-native test/core (`AIPMDM-888-pytest-native`)

Branch: `AIPMDM-888-pytest-native` off `AIPMDM-888` HEAD `a3d9376e`.

## Goal

Maximally pytest-native `test/core`. Drop the codegen architecture entirely:
no `FlowDefinition`, no `FlowFormatter`, no `graphs/*.json` templates, no
`pytest_generate_tests` matrix expansion, no synthesised `test_flow.py`
written from `inspect.getsourcelines()`. Each test becomes a normal pytest
module containing a real `FlowSpec` subclass and one or more `def test_…`
functions.

Constraints from the user:
- **Keep** the runner/tox-environments APIs (`Runner`, `tox -e core-*`).
- **Keep** the cli + api executor distinction.
- **Drop** everything else that's there for the codegen — formatter, graph
  templates, FlowDefinition decorators, custom check classes, the
  cartesian-product parametrize-time logic.

## Current state vs target

| Layer | Now | After |
|-------|-----|-------|
| Test definition | `class X(FlowDefinition): @steps(prio, [quals]) def step_…` | `class XFlow(FlowSpec): @step def start(self): …` |
| Graph topology | 15 JSON templates × 64 FlowDefinition classes → 502 items | One topology baked into each FlowSpec |
| Flow file | Generated at runtime via `FlowFormatter` → `tmp_path/test_flow.py` | The pytest module file itself contains the FlowSpec |
| Test invocation | `test_flow_triple(flow_triple, …)` parametrised over `(graph, test, executor)` | Direct `def test_<name>(metaflow_runner_cli, …)` |
| Run mechanism | `_run_flow` (350 LoC) wraps subprocess.run / Runner / scheduler polling | Small `metaflow_runner` fixture — wraps `Runner` lifecycle, returns a callable |
| Result check | `MetaflowCheck` / `CliCheck` / `MetadataCheck` with custom assertion methods | Direct `Flow(name)[run_id]` queries, plain `assert` |
| Executor matrix | Tox env exports `METAFLOW_CORE_EXECUTORS` → conftest reads and parametrises | `pytest.fixture(params=["cli", "api"])` next to each fixture; tox env can override via `pytest -m "cli"` |
| Backend matrix | `METAFLOW_CORE_MARKER` env-var → conftest applies marker | `pytest -m local` / `-m gcs` / etc. — markers stay |

## Architectural decisions

### A1 — One FlowSpec per file, one topology per FlowSpec

The `FlowDefinition × graph` matrix's main value was running each invariant
("artifact propagates through all steps") against many shapes. Cost: codegen
+ qual matching + manual indentation. Trade: write the FlowSpec for the
topology that actually exercises the property under test, accept smaller
matrix.

For tests where multiple topologies matter (e.g. `BasicArtifact` should
propagate through linear, foreach, branch, switch), use
`@pytest.mark.parametrize` with a small set of explicit FlowSpec classes —
or split into multiple test functions. **Don't** bring back a generator.

### A2 — `metaflow_runner` fixture wraps `Runner`

```python
# conftest.py
@pytest.fixture
def metaflow_runner(tmp_path, monkeypatch, top_options):
    """Yields a function that runs a FlowSpec via cli or api executor.

    Writes the flow to tmp_path/test_flow.py, runs it, returns
    (returncode, run_id, run). Caller asserts.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("METAFLOW_USER", "tester")
    runners = []

    def _run(flow_cls, executor="cli", run_options=None, expect_fail=False, resume_step=None):
        flow_module_path = _write_flow_module(tmp_path, flow_cls)
        if executor == "cli":
            return _run_cli(flow_module_path, top_options, run_options or [], expect_fail)
        elif executor == "api":
            r = Runner(flow_module_path, ...)
            runners.append(r)
            return _run_api(r, run_options or [], expect_fail, resume_step)
        else:
            raise ValueError(executor)

    yield _run
    for r in runners:
        r.cleanup()
```

### A3 — `executor` parametrize at the fixture level, not at collection

```python
@pytest.fixture(params=["cli", "api"])
def executor(request):
    return request.param
```

Tests that need both executors take `executor` and pass it to
`metaflow_runner`. Tests that only work for one (scheduler-only flows)
declare it explicitly.

### A4 — Backend marker via tox env, no per-test parametrisation

```python
# pytest.ini
markers =
    local: …
    gcs: …
    …
```

Tox env sets `METAFLOW_DEFAULT_DATASTORE` etc. via `setenv` (unchanged).
Conftest applies `pytest.mark.<backend>` to every test in the dir via a
`pytest_collection_modifyitems` hook reading one env var
(`METAFLOW_CORE_MARKER`). Still pytest-native; the marker model is what
pytest is for.

### A5 — Direct `Flow(name)[run_id]` for assertions

`CliCheck` and `MetadataCheck` go away. Tests that need to verify
"artifact X equals Y" do:

```python
from metaflow import Flow
run = Flow("BasicArtifactFlow")[run_id]
assert run["end"].task.data.data == "abc"
```

For card / log assertions there are still small helpers in
`metaflow_test/__init__.py` but they take `Run` objects directly.

### A6 — Resume tests orchestrate their own resume

Instead of `RESUME = True` flag triggering harness-level retry:

```python
def test_resume_end_step(metaflow_runner, executor):
    rc, run_id, _ = metaflow_runner(ResumeEndStepFlow, executor=executor)
    assert rc != 0  # first run failed via ResumeFromHere

    rc, run_id_resumed, _ = metaflow_runner(
        ResumeEndStepFlow, executor=executor, resume_step="end"
    )
    assert rc == 0
    assert Flow("ResumeEndStepFlow")[run_id_resumed]["end"].task.data.data == "foo"
```

### A7 — Helpers retained

In `metaflow_test/__init__.py` (small):

- `ResumeFromHere`, `TestRetry` exceptions (used inside FlowSpec step bodies).
- `try_to_get_card`, `retry_until_timeout` (for card timing tests).
- `is_resumed`, `origin_run_id_for_resume` (small helpers).

Everything else (`FlowDefinition`, `MetaflowTest`, `@steps`, `@tag`,
`MetaflowCheck`, `CliCheck`, `MetadataCheck`, `truncate`, the Assert*Failed
classes which are already gone) is deleted.

## Files affected

### Deleted
- `test/core/metaflow_test/formatter.py` (203 LoC)
- `test/core/metaflow_test/cli_check.py` (270 LoC)
- `test/core/metaflow_test/metadata_check.py` (255 LoC)
- `test/core/test_core_pytest.py` (494 LoC after round 2)
- `test/core/graphs/` (15 JSON files)

### Heavily reduced
- `test/core/conftest.py`: from 151 LoC to ~80 LoC (markers + 2 fixtures).
- `test/core/metaflow_test/__init__.py`: from 198 LoC (post round 3) to ~70 LoC (just the helper exceptions + `try_to_get_card`).

### Added
- `test/core/conftest.py` rewritten with `metaflow_runner`, `executor`,
  marker hooks.

### Migrated (69 files in `test/core/tests/`)
Each becomes a normal pytest module:
- Top: imports + `FlowSpec` subclass(es).
- Bottom: one or more `def test_…(metaflow_runner, executor)` functions.

### Updated
- `test/core/tox.ini`: drop `METAFLOW_CORE_TOP_OPTIONS` / `EXECUTORS` /
  `DISABLED_TESTS` / `ENABLED_TESTS` / `DISABLE_PARALLEL` etc. — the
  conftest no longer reads them. Keep marker-driven selection
  (`pytest -m local`).
- `test/core/pytest.ini`: unchanged or trivially updated.

## Migration plan

Done in phases. After each phase, `pytest --collect-only` should still
work (collect more tests progressively as old harness shrinks).

### Phase 0: Branch + plan (this commit)
Create `AIPMDM-888-pytest-native` from `AIPMDM-888`. Write this plan.

### Phase 1: New harness scaffold
- Rewrite `conftest.py` with the new fixtures (no parametrize-time matrix).
- Reduce `metaflow_test/__init__.py` to helper exceptions + small utilities.
- Delete `formatter.py`, `cli_check.py`, `metadata_check.py`,
  `test_core_pytest.py`, `graphs/`.
- Suite is now empty; pytest collects 0 items.

### Phase 2: Migrate tests in batches
69 files. Group by pattern:
- **Trivial linear** (basic_artifact, current_singleton, basic_tags, etc.) — ~30 files. Each becomes a 30-50-line pytest module.
- **Foreach** (basic_foreach, nested_foreach, wide_foreach, etc.) — ~7 files.
- **Switch** (switch_basic, switch_nested, branch_in_switch, etc.) — ~7 files.
- **Resume** (resume_end_step, resume_foreach_*, etc.) — ~10 files.
- **Cards** (card_*) — ~15 files (some can collapse).
- **Specific** (s3_failure, secrets_decorator, custom_decorators) — handled per-file.

### Phase 3: tox.ini cleanup + verify each backend env config
Drop `METAFLOW_CORE_*` setenv that the new conftest doesn't read. Run
`pytest --collect-only -m local` and confirm count.

### Phase 4: 5 rounds of codex review + fixes
After implementation lands, iterate. Each round can flag:
- Bugs in the new harness.
- Tests that lost coverage in the migration.
- Boilerplate that should be a fixture.
- Better assertion patterns.

After every round: judge → fix → local commit (per the existing skill).

## Risks

1. **Coverage shrinkage**: 502 → ~120 items. Mitigation: for the few tests
   where multi-topology coverage genuinely matters, hand-write the variants
   (`BasicArtifactLinear`, `BasicArtifactForeach`, `BasicArtifactBranch`)
   with `@pytest.mark.parametrize` over the FlowSpec class.

2. **Lost regression catches**: maybe the "test against 15 graph shapes"
   logic catches bugs the simpler tests won't. Document tests where
   coverage dropped meaningfully in a `MIGRATION.md` so the team can decide
   to add explicit variants.

3. **Resume tests get more verbose**: each test now orchestrates its
   own resume. Worth it for clarity, but adds ~10 LoC per resume test.

4. **Card tests rely on `try_to_get_card` polling**: keep the helper.
   Pytest-native enough.

## Acceptance criteria

- All 69 test files migrated; `pytest --collect-only` reports a sensible
  number (≥ 100 items, with executor parametrisation).
- Smoke runs pass for representative tests (`test_basic_artifact`,
  `test_basic_foreach`, `test_resume_end_step`, etc.) on `core-local`.
- No `FlowDefinition`, `FlowFormatter`, `graphs/`, `cli_check.py`,
  `metadata_check.py`, `test_core_pytest.py`, `MetaflowCheck` references
  anywhere in `test/core/`.
- `tox -c test/core/tox.ini -e core-local` succeeds (or fails for
  reasons unrelated to the harness rewrite).
- 5 rounds of codex review applied, with their per-round commits.

## Out of scope

- Migrating `test/ux/` (separate suite).
- Refactoring the api executor's click-API introspection (Runner kwargs
  remains the entry point).
- The `passenv = *` security tightening — leave for a follow-up.

## Order

1. Phase 0: this commit (branch + plan).
2. Phase 1: harness rewrite. Single commit.
3. Phase 2: tests in batches of ~10–20 files per commit.
4. Phase 3: tox cleanup. Single commit.
5. Phase 4: codex review × 5.
6. Final: gist update + push branch (no, per user constraint — local only;
   gist documents the result).
