# `MutableFlow.add_step` / `remove_step`

Programmatic graph mutation lets a `FlowMutator` insert steps into or
remove steps from a Metaflow flow at class-decoration time. The most
common use is wrapping a pure function as a new step:

```python
from metaflow import FlowSpec, step
from metaflow.user_decorators.user_flow_decorator import FlowMutator


def double_bar(bar):
    return bar * 2


class AddDoubler(FlowMutator):
    def pre_mutate(self, flow):
        flow.add_step(
            "compute_foo",
            after="start",
            func=double_bar,
            produces="foo",
        )

    def mutate(self, flow):
        pass


@AddDoubler
class MyFlow(FlowSpec):
    @step
    def start(self):
        self.bar = 21
        self.next(self.end)

    @step
    def end(self):
        print(self.foo)  # 42
```

`AddDoubler` inserts a new step `compute_foo` between `start` and
`end`. At runtime the step reads `self.bar` (written by `start`),
calls `double_bar(bar=21)`, and assigns the return value to
`self.foo`. The wrapper handles the FlowSpec calling convention
automatically; you do not write `self.next(...)` inside the function.

## API surface

```python
MutableFlow.add_step(
    name: str,
    after: str | list[str],
    func: Callable,
    *,
    produces: str | tuple[str, ...] | list[str] | None = None,
    inputs: dict[str, str] | None = None,
    outputs: dict[str, str] | None = None,
    decorators: list | None = None,
    next_steps: str | list[str] | None = None,
    overwrite: bool = False,
) -> MutableStep
```

```python
MutableFlow.remove_step(name: str) -> bool
```

Both methods may only be called inside `FlowMutator.pre_mutate`,
matching the timing of `add_parameter` / `remove_parameter`.

## Calling styles

### Style A — pure-function fast path (the default)

Pass `func=` and `produces=`. Inputs are inferred from the function's
parameter names via `inspect.signature(func)`; the return value is
assigned to `self.<produces>`. The 80% case.

**Single return:**

```python
def double_bar(bar):
    return bar * 2

flow.add_step("compute_foo", after="start", func=double_bar, produces="foo")
# Inferred: inputs={"bar": "bar"}, output -> self.foo
```

**Multiple returns:**

```python
def split_value(x):
    return x * 2, x + 1

flow.add_step(
    "split", after="start",
    func=split_value,
    produces=("doubled", "incremented"),
)
# Wrapper assigns self.doubled, self.incremented = result
# Length mismatch at runtime raises RuntimeError.
```

**Default-bearing parameters:**

```python
def double_or_default(bar=42):
    return bar * 2

flow.add_step("dod", after="start", func=double_or_default, produces="out")
# If the producer wrote self.bar, that value flows through.
# If the producer omitted self.bar, the default 42 is used.
```

Default values are snapshotted at `add_step` decoration time and
captured in the wrapper closure. Mutating `func.__defaults__` after
`add_step` returns does NOT change the wrapper's behavior.

### Style B — explicit input/output mapping (escape hatch)

When the consumer's parameter name differs from the producer's
artifact name (e.g., the producer wrote `self.parent_bar` but your
function takes `bar`):

```python
def double_bar(bar):
    return bar * 2

flow.add_step(
    "compute",
    after="parent",
    func=double_bar,
    inputs={"bar": "parent_bar"},   # internal_name -> external_name
    produces="result",
)
```

### Advanced mode (R8) — sandboxed `self`

When `func` takes a single positional argument named `self`, the
wrapper invokes it with a `_NamespacedSelf` proxy. The body operates
on the proxy like a regular step body, and `embedded("helper")(...)`
can be used to invoke helpers:

```python
def complex_compute(self):
    self.tmp = embedded("helper")(self.parent_bar)
    self.result = self.tmp + 1
    return self.result

flow.add_step(
    "complex",
    after="parent",
    func=complex_compute,
    inputs={"parent_bar": "bar"},
    produces="result",
)
```

`embedded(...)` is rejected by lint rule **L-NS-007** when called
outside an `add_step`-registered callable (regular `@step` bodies)
OR inside a Style A function (no `self` argument). The restriction
exists because `embedded()` requires a `_NamespacedSelf` context,
which only exists in R8 mode.

## Removing a step

```python
flow.remove_step("middle")
```

Edge rewiring: every node whose `out_funcs` referenced the removed
step now references the removed step's successors (typically a single
linear successor). Removing an `@step(start=True)`-annotated step or
an `@step(end=True)`-annotated step is refused — Metaflow flows need
a single start and single end.

## Signature rules (R1..R13 + R-EXTRA)

`add_step` rejects unsupported call patterns at decoration time with
a `TypeError`:

| Rule | Pattern | Outcome |
|------|---------|---------|
| R1 | `*args` (`VAR_POSITIONAL`) | REJECT |
| R2 | `**kwargs` (`VAR_KEYWORD`) | REJECT |
| R3 | lambda (`__name__ == "<lambda>"`) | REJECT — not packageable |
| R4 | `functools.partial` | REJECT |
| R5 | decorated function (`__wrapped__` present) | ACCEPT — signature follows `__wrapped__` |
| R6 | bound method | REJECT |
| **R7** | default values present | ACCEPT (after mutable-default rejection) — defaults snapshotted at decoration time |
| **R8** | single positional `self` arg | ACCEPT — advanced mode (`_NamespacedSelf`) |
| R9 | `async def` | REJECT |
| R10 | generator function (`yield`) | REJECT |
| R11 | class object | REJECT |
| R12 | callable instance | REJECT |
| R13 | keyword-only param | REJECT — promote to positional-or-keyword |
| R-EXTRA | Style A func with `embedded()` in body | REJECT at lint time (L-NS-007) |

## Mutable defaults (UD-9)

Default values that are mutable instances of the following stdlib
types are rejected with a `TypeError` at decoration time, with a
None-sentinel workaround suggested in the error message:

```python
list, dict, set, bytearray,
collections.deque, collections.OrderedDict,
collections.defaultdict, collections.Counter,
array.array,
```

The recommended replacement pattern:

```python
def f(items=None):
    if items is None:
        items = []
    ...
```

**Third-party mutables (numpy.ndarray, pandas.DataFrame, custom
mutable subclasses, etc.) are NOT detected** by this denylist. They
are the user's responsibility. This is consistent with B006-style
lint coverage. If you encounter cross-task pollution from a
non-stdlib mutable default, the same None-sentinel pattern applies.

## Lint rules

Five new lint rules fire at graph-build time in addition to the
existing Metaflow checks:

- **L-NS-005** — a mutator-added step declares an input that no
  ancestor statically produces. Defensively skipped when any ancestor
  is a regular `@step` (whose output surface is opaque in Phase 1) so
  it does not false-positive on `self.<x> = ...` writes in legacy
  bodies.
- **L-NS-006** — a mutator-added step declares an internal input name
  that collides with a flow `Parameter` or a class-level constant.
  The class-level read-only property installed by
  `task.py:_init_parameters` would shadow the overlay branch at
  runtime, so the step would read the Parameter / constant value
  instead of the producer's artifact.
- **L-NS-007** — `embedded(...)` may ONLY be called inside callables
  registered via `MutableFlow.add_step(...)` AND only when the
  callable is in R8 advanced mode (single positional `self` arg).
  The rule scans BOTH `@step`-decorated method bodies on the flow
  class AND the source of every user function wrapped by an
  `add_step`-synthesized wrapper.
- **L-NS-008** — a mutator-added join step declares an input whose
  two or more upstream branches produce under DIFFERENT external
  names. `inputs[i].<internal>` resolution would be ambiguous.
- **L-NS-009** — `remove_step` would orphan a foreach split or its
  matching join.

## Known caveats and Phase-1 limitations

These trade-offs are documented in the design memo at
`docs/design/flowoutput-forward-compat.md` (forward-compat) and
accepted for Phase 1:

### Traceback frames show `mutable_flow.py`

Runtime exceptions inside the synthesized wrapper show
`mutable_flow.py:<line>` frames rather than your `transform()` body's
file. `DAGNode.source_file` resolves correctly to your file via
`inspect.unwrap`, but Python provides no public API to rewrite the
wrapper's `co_filename` cheaply in Phase 1. A `types.CodeType.replace`-
based rewrite is on the Phase 2 roadmap.

If you see a `mutable_flow.py:NNN` frame in a traceback, your
declared input is likely missing from the producer's namespace and
the wrapper hit the `getattr` fallback. The frame above it (your
function) shows the offending parameter.

### Style A binds parameter names to producer artifact names

Renaming a positional parameter on the consumer (`def transform(bar)`
→ `def transform(b)`) silently rewires the input source because
Style A infers inputs from the signature. Renaming the producer's
`produces=` likewise breaks consumers silently until the consumer
step runs. Style B (`inputs={...}` explicit dict) is the escape
hatch when this fragility is unacceptable.

### Third-party mutable defaults not detected (UD-9 scope)

See "Mutable defaults" above. The denylist is intentionally limited
to 9 stdlib types for predictable, false-positive-free behavior.

### Producer fan-out deferred to Phase 2

One internal artifact → one external name. A producer cannot emit
the same artifact under multiple external names for different
consumers in Phase 1. Phase 2 will extend the `outputs=` shape and
the namespace resolver to support this.

### `embedded()` alias detection has false negatives

The L-NS-007 AST scan catches direct `embedded(...)`, qualified
`metaflow.embedded(...)`, and aliased imports
(`from metaflow import embedded as e; e(...)`). It does NOT catch
runtime alias rebinding (`e = embedded; e(...)`) or `getattr`-style
dynamic resolution. Don't rely on those patterns.

### Reload-limitation for user-registered `data_flow` kinds

`importlib.reload(metaflow)` is safe for the three built-in kinds
(`explicit_inputs`, `explicit_outputs`, `embedded_callable`) because
the registry compares specs and treats matches as idempotent.

User modules that register custom kinds with a `validator` callable
are NOT reload-safe in Phase 1: `importlib.reload(your_module)`
produces a fresh function object, so the registry's identity-based
comparison `existing["validator"] == new_validator` returns False
and re-registration raises `ValueError`. Phase 1 ships zero
user-registered kinds; Phase 2 may switch to a content-hash
comparison if user-registered validators become common.

### `factory.<locals>.Inner` qualname collisions

`PROCESSED_BY` (the idempotency guard) keys on
`(flow_cls.__qualname__, step_name)`. Two FlowSpec classes defined
inside the same outer factory function share `__qualname__`
(`factory.<locals>.Inner`); the guard treats them as one. This is
vanishingly rare in real flow code and is accepted as a documented
limitation.

## Worked examples

The end-to-end test file at
`test/unit/mutators/test_add_step.py` (specifically
`TestAddStepGraph::test_basic_insertion_between_start_and_end` and
`test_remove_step_rewires_edges`) is the canonical executable
worked example for Phase 1.

## What's coming in Phase 2

- Producer-owned fan-out (one internal artifact → many external names).
- Embedded `FlowSpec` composition beyond the function-level `embedded()`
  sentinel.
- `co_filename` rewrite via `types.CodeType.replace` for full
  traceback fidelity in mutator-defined steps.
- Producer-verification lint (static check that the producer step's
  body assigns `self.<output> = ...` for every declared output).
- Reload-safety for user-registered kinds with `validator` callables.
- Card UI rendering of `data_flow` step kinds.
- Cross-flow type checking on declared input/output types.

## See also

- `docs/design/flowoutput-forward-compat.md` — the per-step
  `data_flow` schema slot reserved for FlowOutput / Phase 2.
- `tools/p001_baseline.txt` — pinned upstream SHA used by the
  AC-AST-UNCHANGED regression test to verify
  `metaflow/graph.py:221-303` (`DAGNode._parse`) stays byte-identical
  to the baseline as Phase 1 lands.
