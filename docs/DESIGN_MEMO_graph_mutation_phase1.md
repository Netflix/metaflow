# Design Memo — Programmatic Graph Mutation in Metaflow (Phase 1)

**Status:** Implementation-ready. Battle-tested through 4 rounds of adversarial design review (3 Architect + 4 Critic agent passes; 9 user directives locked).

**Scope:** Add `add_step` / `remove_step` to Metaflow flow mutators, plus per-step namespaced inputs/outputs in the step graph.

**Out of scope:** Phase 2 producer-fanout, embedded `FlowSpec` composition, `co_filename` traceback rewrite, producer-verification lint, card UI rendering, cross-flow type checking.

---

## 1. Goals

A user-written flow mutator can do this:

```python
def double_bar(bar):
    return bar * 2

class MyMutator(FlowMutator):
    def pre_mutate(self, flow):
        flow.add_step("compute_foo", after="parent_step",
                      func=double_bar, produces="foo")
```

The graph gains a new step named `compute_foo` between `parent_step` and `parent_step`'s original successor. At runtime, the new step reads `parent_step`'s `self.bar`, calls `double_bar(bar)`, and writes the result to `self.foo` so downstream steps see it.

**The 80% case must require zero ceremony.** Inputs are inferred from `inspect.signature(func)`; the return value goes to `self.<produces>`. The heavy `inputs={...}/outputs={...}` dict form remains accessible for the cases where explicit name mapping is needed (advanced escape hatch — "Style B").

`remove_step(name)` excises a step and rewires the edges.

---

## 2. Non-negotiable constraints

1. **No change to `self.next(...)`** — its parsing, keyword arguments, and the AST tail classifier at `metaflow/graph.py:221-303` (`DAGNode._parse`) stay byte-identical. Mutator-added steps bypass the AST parser via an early-return path keyed on `_mf_edges`. Verified by `AC-AST-UNCHANGED` and `AC-P-001`.
2. **Backward compatibility:** flows that don't call `add_step`/`remove_step` produce a byte-identical `_graph_info` to today. The new `data_flow` key is suppressed when empty. Verified by `AC-BYTE-EQ`.
3. **`add_step` / `remove_step` run at class-decoration time** via `FlowMutator.pre_mutate`, matching `add_parameter`/`remove_parameter` timing. They affect `_graph_info`, lint, and orchestrator emission.

---

## 3. Public API

```python
# metaflow/user_decorators/mutable_flow.py
class MutableFlow:
    def add_step(
        self,
        name: str,
        after: Union[str, List[str]],
        func: Callable,
        *,
        produces: Union[str, Tuple[str, ...], None] = None,
        inputs: Optional[Dict[str, str]] = None,     # Style B escape hatch
        outputs: Optional[Dict[str, str]] = None,    # Style B escape hatch
        decorators: Optional[List] = None,
        next_steps: Union[str, List[str], None] = None,
        overwrite: bool = False,
    ) -> "MutableStep": ...

    def remove_step(self, name: str) -> bool: ...
```

### Style A vs Style B

- **Style A (the default, 80% case):** caller passes `func=` and `produces=`. Inputs inferred from `inspect.signature(func).parameters` keyed by parameter name. Return assigned to `self.<produces>` (or unpacked into multiple `self.*` if `produces` is a tuple).
- **Style B (advanced):** caller passes `inputs={internal_name: external_name, ...}` and `outputs={internal_name: external_name, ...}`. Useful when consumer-side parameter names differ from producer-side artifact names.
- **R8 advanced mode (`embedded()`):** `func` takes a single positional arg named `self`. The body operates on a `_NamespacedSelf` proxy and may use `embedded("helper")(self.x)` (see §7).

### Worked examples

```python
# Example 1 — single return (the 80% case)
def double_bar(bar): return bar * 2
flow.add_step("compute_foo", after="parent", func=double_bar, produces="foo")
# Inferred: inputs={"bar":"bar"}, output → self.foo

# Example 2 — multi-return
def split_value(x): return x * 2, x + 1
flow.add_step("split", after="parent", func=split_value,
              produces=("doubled","incremented"))
# Wrapper unpacks: self.doubled, self.incremented = result
# Length mismatch at runtime → RuntimeError

# Example 3 — default-bearing parameter
def double_or_default(bar=42): return bar * 2
flow.add_step("dod", after="parent", func=double_or_default, produces="out")
# Wrapper substitutes 42 if producer omits self.bar

# Example 4 — advanced (R8 / embedded)
def complex_compute(self):
    self.tmp = embedded("helper")(self.parent_bar)
    self.result = self.tmp + 1
    return self.result
flow.add_step("complex", after="parent", func=complex_compute,
              inputs={"parent_bar":"bar"}, produces="result")
```

---

## 4. Signature rules (evaluated at `add_step` decoration time)

Rules are evaluated in order. The first matching rule decides the outcome.

| #  | Pattern                                       | Action |
|----|-----------------------------------------------|--------|
| R1 | `*args` (`VAR_POSITIONAL`)                    | REJECT — `TypeError` |
| R2 | `**kwargs` (`VAR_KEYWORD`)                    | REJECT |
| R3 | lambda (`func.__name__ == "<lambda>"`)        | REJECT (not packageable) |
| R4 | `functools.partial`                           | REJECT in Phase 1 |
| R5 | Decorated function (`__wrapped__` present)    | ACCEPT — `inspect.signature` follows `__wrapped__` automatically |
| R6 | Bound method                                  | REJECT |
| **R7** | **Default values present**               | Two-phase: (i) mutable default per §5 → REJECT; (ii) otherwise ACCEPT and capture default in wrapper closure |
| **R8** | First param named `self`, only positional arg | ACCEPT — advanced mode (`_NamespacedSelf`). Detection uses `inspect.unwrap(func)` recursively to find the original signature. Decorator-injected `self` may be misclassified (documented edge case). |
| R9  | `async def`                                  | REJECT |
| R10 | Generator function                           | REJECT |
| R11 | `inspect.isclass(func)`                      | REJECT |
| R12 | Callable instance                            | REJECT |
| R13 | Keyword-only parameter                       | REJECT |
| **R-EXTRA-EMBED-STYLE-A** | Style A user func (no `self` arg) whose source contains `embedded()` | REJECT at lint via L-NS-007 |

---

## 5. Mutable-default rejection (R7 phase i)

Implemented as `_reject_mutable_defaults(func, sig)` invoked between signature inspection and wrapper synthesis in `mutable_flow.py`:

```python
import collections, array, inspect

_MUTABLE_DEFAULT_TYPES = (
    list, dict, set, bytearray,
    collections.deque,
    collections.OrderedDict,
    collections.defaultdict,
    collections.Counter,
    array.array,
)

def _reject_mutable_defaults(func, sig):
    for p in sig.parameters.values():
        if p.default is inspect.Parameter.empty:
            continue
        if isinstance(p.default, _MUTABLE_DEFAULT_TYPES):
            raise TypeError(
                f"add_step: parameter {p.name!r} on {func.__qualname__!r} has "
                f"mutable default {p.default!r}. "
                f"Use None sentinel + in-function check:\n"
                f"  def f({p.name}=None):\n"
                f"      if {p.name} is None: {p.name} = []  # or default literal\n"
                f"      ..."
            )
```

**Scope:** denylist covers 9 stdlib mutable types likely to appear as default literals. Superset of Bugbear B006's stdlib coverage. Third-party mutables (`numpy.ndarray`, `pandas.DataFrame`, custom mutable subclasses) are NOT detected — user responsibility. Documented in `docs/api/mutable_flow.md`.

**Why denylist over allowlist:** an immutable allowlist (`int/str/tuple/frozenset/float/bool/bytes/None`) would reject legitimate immutable user types (`frozenset`-like custom classes); the denylist trades false negatives on exotic mutables for zero false positives on legitimate immutable user types.

---

## 6. Wrapper synthesis

`mutable_flow.py::_synthesize_wrapper` walks the signature once and emits a closure that adapts a plain function to the FlowSpec runtime calling convention:

```python
def _synthesize_wrapper(name, func, inputs_map, produces, after_steps, next_steps):
    sig = inspect.signature(func)                  # follows __wrapped__ per R5
    param_names = []
    defaults = {}                                  # captured at decoration time
    for p_name, p in sig.parameters.items():
        param_names.append(p_name)
        if p.default is not inspect.Parameter.empty:
            defaults[p_name] = p.default           # SNAPSHOT — closed over
    external = {p: inputs_map.get(p, p) for p in param_names}

    def _wrapper(self):
        kwargs = {}
        for p_name in param_names:
            try:
                kwargs[p_name] = getattr(self, p_name)   # triggers __getattr__ + overlay
            except AttributeError:
                if p_name in defaults:
                    kwargs[p_name] = defaults[p_name]    # R7 fallback
                else:
                    raise
        result = func(**kwargs)
        if isinstance(produces, str):
            setattr(self, produces, result)
        elif produces is not None:                  # tuple
            if not isinstance(result, tuple) or len(result) != len(produces):
                raise RuntimeError(
                    f"step {name!r}: produces declared {len(produces)} "
                    f"values but function returned {result!r}"
                )
            for slot_name, value in zip(produces, result):
                setattr(self, slot_name, value)
        self.next(*[_resolve_step(s) for s in (next_steps or [_NEXT_STEP])])

    _wrapper.__name__     = name
    _wrapper.__qualname__ = f"{flow_cls.__name__}.{name}"
    _wrapper.__module__   = func.__module__
    _wrapper.__wrapped__  = func
    _wrapper.__doc__      = func.__doc__
    _wrapper.is_step      = True
    _wrapper._mf_edges    = {"in": list(after_steps), "out": list(next_steps or [_NEXT_STEP])}
    _wrapper._mf_dataflow = {"inputs": dict(external), "outputs": _outputs_for(produces)}
    _wrapper._mf_defaults = dict(defaults)
    return _wrapper
```

**Wrapper invariants:**
- `__name__` / `__qualname__` / `__module__` / `__wrapped__` / `__doc__` preserved.
- `inspect.getfile(_wrapper)` returns `mutable_flow.py` (the wrapper's compilation site). Runtime tracebacks inside the wrapper show `mutable_flow.py:<line>` frames. **Accepted UX cost** — `co_filename` rewrite via `types.CodeType.replace` is deferred to Phase 2.
- `inspect.getfile(_wrapper.__wrapped__)` returns the user's file — this is what `_create_nodes` uses to populate `DAGNode.source_file`.

---

## 7. `embedded()` and packaging

`embedded("helper")(args...)` is a sentinel callable available ONLY inside callables registered via `add_step(...)`. It is rejected by lint (`L-NS-007`) when called from:

- a regular `@step`-decorated method body (no `_NamespacedSelf` exists there)
- a Style A user func (no `self` arg → no `_NamespacedSelf` context)

When `add_step(func=helper)` is called, the mutator records `inspect.getfile(helper)` into `_flow_cls._flow_state[FlowStateItems.PACKAGED_CALLABLES]`. The packaging hook in `metaflow/package/__init__.py` unions this set with auto-discovered files — `helper`'s source travels with the flow to remote runners.

`_NamespacedSelf` (in new file `metaflow/_namespaced_self.py`, ~120 LOC) is a proxy that exposes the parent flow's overlay-resolved attributes to advanced-mode (R8) callables. `__slots__` is intentionally NOT used (the iter-2 Architect noted that `__slots__ = ("__dict__", ...)` defeats the optimization; drop the constraint entirely).

---

## 8. Namespace mechanism — consumer-owned lazy overlay

**Decision (locked):** consumer step declares its own input names; producer persists artifacts under the consumer's chosen external name; resolver walks an instance-level `_mf_input_overlay` via an extended `FlowSpec.__getattr__`.

**Why this and not the alternatives:**
- *Producer-owned with rename-on-read* (the steelman) would preserve producer authority but require touching `_graph_info` serialization on every flow. Rejected for Phase 1.
- *`__getattribute__` override* would add per-access overhead on every attribute lookup. Rejected — violates the byte-equivalence principle.

**Phase 1 constraint accepted:** one internal name → one external name. Producer-fanout (one internal → many externals) deferred to Phase 2.

### 8.1 `FlowSpec.__getattr__` extension

`metaflow/flowspec.py` (existing `__getattr__` at line 599-607). Insert ONE branch BEFORE the datastore lookup:

```python
def __getattr__(self, name: str):
    overlay = self.__dict__.get("_mf_input_overlay")
    if overlay is not None and name in overlay:
        external_name = overlay[name]
        if self._datastore and external_name in self._datastore:
            x = self._datastore[external_name]
            setattr(self, name, x)          # cache for subsequent reads
            return x
        raise AttributeError(
            f"Step declared input {name!r} (resolved to artifact "
            f"{external_name!r}) is not present in the parent datastore"
        )
    # Existing path preserved VERBATIM:
    if self._datastore and name in self._datastore:
        x = self._datastore[name]
        setattr(self, name, x)
        return x
    raise AttributeError(f"Flow {self.name} has no attribute '{name}'")
```

**Zero overhead on the no-namespace path:** when `_mf_input_overlay` is not in `self.__dict__`, the `dict.get` returns `None` and the function falls through to the existing code path immediately. Byte-equivalence preserved (`AC-NO-NS-IO`).

**Cache lifetime:** per-task (per FlowSpec instance). Resume creates a fresh FlowSpec so the cache rebuilds from the (potentially renamed) origin datastore.

### 8.2 Interaction with `_init_parameters` — class-level shadowing

`metaflow/task.py:191-250` installs class-level read-only properties for parameters AND for non-method class-level values (`task.py:232-240` walks `dir(cls)`). Class-level descriptors take precedence over `__getattr__`. Overlay entries that collide with parameters OR class constants would be silently shadowed.

Lint rule **L-NS-006** enumerates the SAME set as `task.py:232-240` (must MIRROR the filter verbatim including the `var in all_vars` exclusion and bare `getattr(cls, var)` with no default):

```python
def _l_ns_006_shadow_names(cls):
    from types import MethodType, FunctionType
    NP = getattr(cls, "_NON_PARAMETERS", frozenset())
    shadowed = set()
    all_vars = set()                                   # MIRRORS task.py:_init_parameters
    for var, _ in cls._get_parameters():
        all_vars.add(var)
    for var in dir(cls):
        if var.startswith("_") or var in NP:
            continue
        if var in all_vars:                            # CRITICAL — mirror task.py:233
            continue
        val = getattr(cls, var)                        # bare; no default — mirror task.py
        if isinstance(val, (MethodType, FunctionType, property, type)):
            continue
        shadowed.add(var)
    return shadowed
```

**Regression test (`AC-LINT-006-MIRRORS-TASKPY`):** construct a fixture flow with a `Parameter`, a class constant, and a step-input declaration colliding with each; assert the helper's shadowed set equals the names that `_init_parameters` actually installs.

### 8.3 Overlay installation point

Installed inside `MetaflowTask.run_step` at `task.py`, immediately AFTER `current._update_env({...})` (joins: line 843; linear: line 868) and CRITICALLY BEFORE the `task_pre_step` decorator dispatch (line 902). Pre-step hooks must not access `inp.<attr>` before the overlay is in place, otherwise the existing datastore branch caches under the wrong name.

```python
# Inserted after task.py:868 (after current._update_env on the linear path)
# AND after task.py:843 (after current._update_env on the join path),
# BEFORE the task_pre_step loop at task.py:902
edges_meta = getattr(step_func, "_mf_dataflow", None)
if edges_meta is not None:
    overlay = dict(edges_meta["inputs"])
    # PM-001 mitigation: clear pre-cached attrs whose name is in overlay
    for internal_name in overlay:
        self.flow.__dict__.pop(internal_name, None)
    self.flow.__dict__["_mf_input_overlay"] = overlay
    # For joins, propagate to each inp BEFORE pre_step hooks
    if input_obj is not None:
        for inp_flow in input_obj.flows:
            for internal_name in overlay:
                inp_flow.__dict__.pop(internal_name, None)
            inp_flow.__dict__["_mf_input_overlay"] = dict(overlay)
else:
    self.flow.__dict__.pop("_mf_input_overlay", None)
```

### 8.4 Foreach + join

Each `inp` in `input_obj.flows` gets the overlay. Producer persisted under consumer's external name (UD-2 consumer-owned), so `inp.model` first checks the overlay, finds `"model"`, then reads the datastore under external `"model"`. Lint rule **L-NS-008** catches the ambiguous case where different branches declare different external names for the same internal join input.

---

## 9. `add_step` / `remove_step` mechanics

### 9.1 `add_step` at decoration time (called from `FlowMutator.pre_mutate`)

1. **Signature inspection** — apply R1..R13 + R-EXTRA-EMBED-STYLE-A; reject early with `TypeError` on rule failure.
2. **Mutable-default rejection** — `_reject_mutable_defaults(func, sig)` per §5.
3. **Wrapper synthesis** — `_synthesize_wrapper(...)` per §6.
4. **Idempotency guard (PROCESSED_BY):**
   ```python
   from metaflow.flowspec import FlowStateItems
   mutator_key = (self._flow_cls.__qualname__, name)     # NO id(self)
   processed = self._flow_cls._flow_state.setdefault(
       FlowStateItems.PROCESSED_BY, {}
   )
   if mutator_key in processed:
       prior = processed[mutator_key]
       if prior == _spec_signature(func, produces, after, decorators):
           return self._existing_mutable_step(name)       # idempotent
       raise MetaflowException(
           f"add_step({name!r}): conflicting prior registration; "
           f"use overwrite=True or pick a unique name"
       )
   processed[mutator_key] = _spec_signature(func, produces, after, decorators)
   ```
   Key is `(qualname, step_name)` — stable across `importlib.reload`, NOT `id(self)` which changes on reload.
5. **Edge wiring:** `after` (str or list) identifies predecessor(s); the new step is inserted between `after` and `after`'s original successor. `next_steps` overrides the auto-inferred next step.
6. **Class mutation:**
   - `setattr(self._flow_cls, name, wrapper)`
   - `wrapper._mf_added_by_mutator = True`
   - Append `inspect.getfile(func.__wrapped__ or func)` to `PACKAGED_CALLABLES`.

### 9.2 `remove_step`

1. Pre-conditions: name exists; `is_step=True`; not start/end; not orphaning a foreach split/join (lint rule **L-NS-009**).
2. Edge rewiring: predecessor's `out_funcs` replaces `name` with `name`'s successor(s).
3. Removal: `delattr(self._flow_cls, name)`; remove from `PROCESSED_BY`.
4. Returns `True` on success, `False` if no such step.

### 9.3 Name collision

`overwrite=False` (default) on existing name → `MetaflowException`. `overwrite=True` → equivalent to `remove_step(name)` then add.

### 9.4 Ordering

Per the order of mutators in `_flow_state[FlowStateItems.FLOW_MUTATORS]`; within a single mutator, call order. Matches the existing `add_parameter` ordering.

### 9.5 Re-execution under pytest parametrize / `importlib.reload`

Two distinct scenarios:

- **Same class, `pre_mutate` called twice in same process:** existing `cls._configs_processed` boolean at `flowspec.py:361-364` already short-circuits. `PROCESSED_BY` guard adds defense-in-depth.
- **`importlib.reload(user_flow_module)`:** a fresh class object is created. Its `_flow_state` is empty. `PROCESSED_BY` does not span classes — and that's the intended behavior. The new class needs its own wrapped step added once.

---

## 10. AST parser bypass

`graph.py:221-303` (`DAGNode._parse`) is NOT modified. An early-return path is added at the TOP of `_create_nodes`'s per-step loop (~`graph.py:432`, inside the `if callable(func) and hasattr(func, "is_step"):` block):

```python
mf_edges = getattr(func, "_mf_edges", None)
if mf_edges is not None:
    # Convert opaque inspect.unwrap ValueError into a clear domain error
    try:
        inner = inspect.unwrap(func)
    except ValueError as e:
        raise MetaflowException(
            f"Decorator chain cycle on step {name!r}: {e}. "
            f"Check for __wrapped__ pointing back to itself."
        )
    source_file = inspect.getfile(inner)
    source_lineno = getattr(inner.__code__, "co_firstlineno", 0)

    node = DAGNode(
        None,                                       # no func_ast — bypass _parse
        func.decorators,
        func.wrappers,
        func.config_decorators,
        func.__doc__,
        source_file,
        source_lineno,
        is_start_step=False,
        is_end_step=False,
        node_info=getattr(func, "node_info", None),
        name=element,
        num_args=len(inspect.signature(func).parameters),
    )
    node.out_funcs = list(mf_edges["out"])
    node.type = mf_edges.get("type", "linear")
    node.has_tail_next = True
    node.invalid_tail_next = False
    nodes[element] = node
    continue
# Normal path follows — UNCHANGED from today.
```

For steps WITHOUT `_mf_edges`, the existing AST parser code path is REACHED unchanged — verified by `AC-P-001` (positive assertion).

---

## 11. Bundled `_data_flow_registry.py`

Standalone registry — no dependency on un-merged FlowOutput work. Three kinds register at import time. Idempotent on matching specs; fail-fast on conflicting specs.

```python
# metaflow/_data_flow_registry.py
"""Standalone data-flow kind registry."""

from typing import Callable, Dict, Optional

_REGISTRY: Dict[str, Dict[str, object]] = {}
_ALLOWED_SEMANTICS = {"descriptive", "routing"}


def register_data_flow_kind(
    kind: str,
    semantics: str = "descriptive",
    schema_validator: Optional[Callable[[dict], None]] = None,
) -> None:
    """Re-registration with the SAME spec is an idempotent no-op;
    re-registration with a DIFFERENT spec raises ValueError."""
    if semantics not in _ALLOWED_SEMANTICS:
        raise ValueError(
            f"semantics must be one of {_ALLOWED_SEMANTICS}; got {semantics!r}"
        )
    existing = _REGISTRY.get(kind)
    if existing is not None:
        if (existing["semantics"] == semantics
                and existing.get("validator") == schema_validator):
            return                                  # idempotent no-op
        raise ValueError(
            f"data_flow kind {kind!r} already registered with "
            f"semantics={existing['semantics']!r}; "
            f"cannot re-register with semantics={semantics!r}"
        )
    _REGISTRY[kind] = {"semantics": semantics, "validator": schema_validator}


def get_kind(kind: str) -> Dict[str, object]:
    return _REGISTRY[kind]


def list_kinds() -> Dict[str, Dict[str, object]]:
    return dict(_REGISTRY)


# Built-in kinds — registered eagerly at import:
register_data_flow_kind("explicit_inputs", "descriptive")
register_data_flow_kind("explicit_outputs", "descriptive")
register_data_flow_kind("embedded_callable", "descriptive")
```

**Eager import** — append one line to `metaflow/__init__.py` after `from .flowspec import FlowSpec` (currently line 102):

```python
from . import _data_flow_registry  # noqa: F401
```

**Reload caveat:** `importlib.reload(metaflow)` is safe (built-in registrations are idempotent on matching specs). User modules that register custom kinds with a `validator` callable are NOT reload-safe — function identity changes across reloads. Phase 1 ships zero user-registered kinds; documented in `docs/api/mutable_flow.md`.

**Forward-compat with FlowOutput:** the `validator` hook is reserved for FlowOutput phase 2 use; no built-in kind registers a validator. A reference excerpt of the FlowOutput shape is committed at `docs/design/flowoutput-forward-compat.md` so the forward-compat claim is auditable.

---

## 12. `_graph_info` schema

`graph.py:608-646` (`node_to_dict`, defined inside `output_steps` at `graph.py:591`) is the only schema-emit change:

```python
def node_to_dict(name, node):
    d = {...}                                       # existing keys, byte-identical
    df_entries = getattr(node, "_mf_dataflow_entries", None)
    if df_entries:                                  # empty → suppressed
        d["data_flow"] = df_entries
    return d
```

When no `add_step`/`remove_step` is used, no step has `_mf_dataflow_entries`, so `"data_flow"` is absent. `_graph_info` is byte-identical to today (`AC-BYTE-EQ`).

Size bound: 100 namespaced steps × (5 inputs + 5 outputs) × ~80 bytes/entry ≈ 80KB (`AC-SIZE-BOUND` ≤ 100KB).

---

## 13. Lint rules

`metaflow/lint.py` gains five new rules registered via the existing `@linter.check` decorator. Lint runs at graph-build time inside `cli._check` (`cli.py:665-670`).

| Rule | Trigger | Timing |
|------|---------|--------|
| **L-NS-005** | Step declares input for an artifact no ancestor produces | graph-build |
| **L-NS-006** | Declared input shadows a flow Parameter OR a class-level constant (per `dir(cls)` filter mirroring `task.py:232-240`) | graph-build |
| **L-NS-007** | `embedded()` called from a non-allowed location (see below) | graph-build (AST scan) |
| **L-NS-008** | Join reads `inputs[i].<name>` where upstream branches declare different external names | graph-build |
| **L-NS-009** | `remove_step` would orphan a foreach split/join | graph-build |

### L-NS-007 dual scope

Scans BOTH:
- **(a) `@step`-decorated method bodies** on the flow class — `embedded()` here is ALWAYS rejected (no `_NamespacedSelf` in regular @step runtime).
- **(b) Source of every callable in `PACKAGED_CALLABLES`** (populated by `add_step`). `embedded()` ALLOWED only when the callable is in R8 advanced mode (signature has a single positional `self` parameter). Style A user funcs (no `self`) containing `embedded()` are rejected — the **R-EXTRA-EMBED-STYLE-A** case.

```python
@linter.check
def L_NS_007_embedded_scope(graph, flow_cls):
    # (a) Scan @step bodies on the flow class:
    for name, attr in vars(flow_cls).items():
        if not getattr(attr, "is_step", False): continue
        if getattr(attr, "_mf_added_by_mutator", False): continue   # skip wrappers
        try:
            src = inspect.getsource(attr)
        except (OSError, TypeError):
            continue
        for loc in _find_embedded_calls(src, attr.__module__):
            yield ("L-NS-007", f"embedded() in @step body {name!r} at {loc}")

    # (b) Scan PACKAGED_CALLABLES source:
    packaged = flow_cls._flow_state.get(FlowStateItems.PACKAGED_CALLABLES, [])
    for func, is_advanced_mode in packaged:
        try:
            src = inspect.getsource(func)
        except (OSError, TypeError):
            continue
        for loc in _find_embedded_calls(src, func.__module__):
            if not is_advanced_mode:
                yield ("L-NS-007", f"embedded() in Style A user func "
                                   f"{func.__qualname__!r} at {loc}; "
                                   f"embedded() requires advanced (self-arg) mode")
```

`_find_embedded_calls` uses `ast.walk` and builds an alias table from the module's top-level imports. **Coverage:** direct identifier `embedded(...)`, qualified `metaflow.embedded(...)`, aliased imports (`from metaflow import embedded as e; e(...)`). **NOT covered (documented):** runtime alias rebinding inside the function (`e = embedded; e(...)`), `getattr(metaflow, "embedded")(...)`.

---

## 14. Pre-mortems

Five concrete failure modes considered, each with a tied mitigation:

1. **Foreach branches overlay-shadow** — prior attribute cache on FlowSpec shadows the overlay because `__getattr__` only fires on misses. **Mitigation:** at overlay-install time, `__dict__.pop(internal_name, None)` for every name in the overlay (§8.3).
2. **`_mf_edges` leaks across classes** — two flow classes in same process share a step function. **Mitigation:** wrapper synthesis creates a NEW function object per `add_step` invocation.
3. **Packaging drops callable source** — `helper` lives in another module; default packaging walker misses it. **Mitigation:** `PACKAGED_CALLABLES` records `inspect.getfile(helper)`; packaging hook unions.
4. **Mutator re-execution under pytest parametrize** — `PROCESSED_BY` keyed on `id(self)` would fail (instance id changes). **Mitigation:** key on `(flow_cls.__qualname__, step_name)` (§9.1 step 4).
5. **Pure-function signature edge cases** — varargs, kwargs, lambdas, partial, decorated funcs, methods. **Mitigation:** R1..R13 + R-EXTRA evaluated at `add_step` time (§4).

---

## 15. File-by-file change list

| File | Change | LOC |
|------|--------|-----|
| `metaflow/_data_flow_registry.py` (NEW) | Standalone registry + idempotent semantics | ~55 |
| `metaflow/__init__.py` | One-line eager import | 1 |
| `metaflow/flowspec.py` | Extend `__getattr__`; add `PACKAGED_CALLABLES` + `PROCESSED_BY` to `FlowStateItems` | ~20 |
| `metaflow/graph.py` | AST bypass for `_mf_edges`; `data_flow` key in `node_to_dict`; use `__wrapped__` for `source_file` | ~32 |
| `metaflow/lint.py` | L-NS-005, L-NS-006 (mirroring filter), L-NS-007 (dual-scope), L-NS-008, L-NS-009 | ~250 |
| `metaflow/user_decorators/mutable_flow.py` | `add_step`, `remove_step`, wrapper synthesis, `_reject_mutable_defaults`, edge rewiring, PROCESSED_BY guard | ~520 |
| `metaflow/task.py` | Overlay install point after `_update_env` (linear: line 868; join: line 843), BEFORE `task_pre_step` dispatch at line 902 | ~22 |
| `metaflow/package/__init__.py` | Honor `PACKAGED_CALLABLES` | ~10 |
| `metaflow/_namespaced_self.py` (NEW) | `_NamespacedSelf` proxy + `embedded()` sentinel | ~120 |
| `docs/api/mutable_flow.md` (NEW) | API docs (incl. traceback caveat + Style A fragility note + reload limitation) | ~210 |
| `docs/design/flowoutput-forward-compat.md` (NEW) | FlowOutput schema reference excerpt | ~60 |
| `tools/p001_baseline.txt` (NEW) | Pinned baseline SHA for AC-AST-UNCHANGED | ~3 |
| `test/perf/bench_no_namespace.py` (NEW) | Benchmark for AC-NO-NS-IO | ~80 |
| `test/perf/baseline_no_namespace.json` (NEW) | Frozen baseline | ~20 |
| `test/perf/bench_lazy_unread_input.py` (NEW) | Benchmark for AC-LAZY | ~60 |
| `test/unit/mutators/flows/_graph_info_baseline.json` (NEW) | Frozen baseline for AC-BYTE-EQ | ~150 |
| **Source total** | | **~1,622** |
| Test files (§16) | | ~3,100 |
| **Grand total** | | **~4,720** |

`tools/p001_baseline.txt` content:
```
6cc34313f95fcc2a59a0acc8cbd1d406664f0f2b
# P-001 baseline: last upstream commit touching metaflow/graph.py:221-303
# (DAGNode._parse) before the graph-mutation Phase-1 work begins on this branch.
# AC-AST-UNCHANGED uses this SHA to verify _parse stays byte-identical.
```

---

## 16. Acceptance criteria (58 total — abbreviated table)

Every criterion is verified by a specific test. Full mapping in §17.

**Core / API (5):**
- `AC-API-001` — `add_step` callable on a `MutableFlow` instance.
- `AC-API-002` — `remove_step` callable on a `MutableFlow` instance.
- `AC-REMOVE-START` — `remove_step` on a `start=True` step raises.
- `AC-OVERWRITE` — `add_step` on an existing name without `overwrite=True` raises.
- `AC-WRAP-NAMES` — wrapper preserves `__name__/__qualname__/__module__/__wrapped__/__doc__`.

**Signature rules (14):** `AC-SIG-R01` .. `AC-SIG-R13`, `AC-SIG-R-EXTRA-EMBED-STYLE-A`.

**UD-6 defaults (3):**
- `AC-R7-DEFAULT-USED` — producer omits attr → default value passes.
- `AC-R7-DEFAULT-OVERRIDDEN` — producer writes attr → producer value passes.
- `AC-R7-LATE-DEFAULT-CAPTURE` — mutating `func.__defaults__` after `add_step` does NOT change the wrapped behavior.

**UD-9 mutable rejection (5):** `AC-R7-LIST-REJECTED`, `AC-R7-DICT-REJECTED`, `AC-R7-SET-REJECTED`, `AC-R7-DEQUE-REJECTED`, `AC-R7-IMMUTABLE-ACCEPTED`, `AC-R7-NONE-SENTINEL-ACCEPTED`.

**Wrapper / traceback (3):**
- `AC-WRAP-SRCFILE` — `DAGNode.source_file == inspect.getfile(func.__wrapped__)` for a clean `def`.
- `AC-WRAP-SRCFILE-BARE-DECORATOR` — bare-decorator chain (no `functools.wraps`) resolves to user's file via recursive `inspect.unwrap`.
- `AC-UNWRAP-CYCLE-CLEAN-ERROR` — `f.__wrapped__ = f` cycle raises `MetaflowException` (not raw `ValueError`).

**Laziness / I/O (3):**
- `AC-LAZY` — namespaced-but-unread input triggers ZERO datastore reads.
- `AC-NO-NS-IO` — no-namespace path I/O profile identical to today (benchmark).
- `AC-BYTE-EQ` — `_graph_info` byte-equivalent for zero-mutation flows.

**Schema (2):**
- `AC-SIZE-BOUND` — `_graph_info` ≤100KB for 100 namespaced steps.
- `AC-FLOWOUTPUT-DOC` — `docs/design/flowoutput-forward-compat.md` present and contains expected schema keys.

**AST preservation (3):**
- `AC-P-001` — for an unmutated step, the AST parser code path is REACHED (positive assertion, not just textual diff).
- `AC-AST-UNCHANGED` — `git log -L 202,286:metaflow/graph.py $(head -1 tools/p001_baseline.txt)..HEAD --oneline` emits empty (UNCHANGED).
- `AC-P001-BASELINE-FILE-PRESENT` — `tools/p001_baseline.txt` exists and the SHA resolves via `git rev-parse`.

**Registry (5):** `AC-REG`, `AC-REG-IMPORT-CHAIN`, `AC-REG-IDEMPOTENT`, `AC-REG-CONFLICT`, `AC-REG-RELOAD`.

**Lint (7):** `AC-LINT-005`, `AC-LINT-006`, `AC-LINT-006-MIRRORS-TASKPY`, `AC-LINT-007-STEP-BODY`, `AC-LINT-007-PACKAGED-CALLABLE`, `AC-LINT-007-ALIAS-COVERED`, `AC-LINT-008`, `AC-LINT-009`.

**Mutator re-execution (2):** `AC-MUTATOR-REEXEC-SAME-CLASS`, `AC-MUTATOR-REEXEC-RELOAD`.

**Misc (3):** `AC-EMB-PKG` (remote packaging picks up `add_step(func=helper)`), `AC-FOREACH-JOIN` (foreach + rename + join), `AC-DOCS` (API docs file present), `AC-DOCS-RELOAD-LIMITATION` (reload caveat documented).

### 16.1 Key non-obvious AC commands

**AC-AST-UNCHANGED** (verifies the non-negotiable P-001 constraint):
```bash
cd /path/to/metaflow && \
  P001_BASELINE_SHA=$(head -1 tools/p001_baseline.txt) && \
  git rev-parse "$P001_BASELINE_SHA" >/dev/null 2>&1 || { echo "BASELINE_MISSING"; exit 1; } && \
  out=$(git log -L 221,303:metaflow/graph.py "$P001_BASELINE_SHA"..HEAD --oneline 2>&1) && \
  [ -z "$out" ] && echo UNCHANGED || echo CHANGED
```
Expected: `UNCHANGED`.

**AC-REG-CONFLICT** (registry idempotency conflict path):
```bash
pytest test/unit/test_data_flow_registry.py::test_register_different_spec_raises -v 2>&1 | grep -E "PASSED"
```
Expected: line containing `PASSED`.

---

## 17. Test plan

Tests grouped by tier. Every AC has a backing test in `§16` mapped here.

### 17.1 Unit tests (~44 tests under `test/unit/mutators/` and `test/unit/`)
- `test_add_step_api.py` — API existence + collision (UT-01, UT-40)
- `test_remove_step.py` — API + start-step rejection (UT-02, UT-39)
- `test_add_step_signature.py` — R01..R13 + R-EXTRA (UT-03..UT-16)
- `test_add_step_wrapper.py` — dunder preservation + source_file + bare decorator + unwrap cycle (UT-17, UT-18, UT-18b, UT-18c)
- `test_add_step_defaults.py` — used / overridden / late-mutation / mutable rejection variants / None sentinel (UT-19..UT-21f)
- `test_data_flow_registry.py` — kinds registered / import chain / idempotent / conflict / reload (UT-22..UT-26)
- `test_namespace_overlay.py` — no-namespace I/O unchanged (UT-27)
- `test_graph_info_byteequiv.py` — byte-equivalence + size bound (UT-28, UT-29)
- `test_graph_ast_parser.py` — AST reached for unmutated + git log -L (UT-30, UT-31)
- `test_lint_namespace.py` — L-NS-005..009 + L-NS-007 dual-scope cases (UT-32..UT-38)
- `test_mutator_reexec.py` — same-class + reload (UT-41, UT-42)
- `test_docs_mutable_flow.py`, `test_docs_flowoutput_forward_compat.py` — docs presence (UT-43, UT-44)
- `test_lint_mirrors_taskpy.py` — `_l_ns_006_shadow_names` mirrors `task.py:232-240` (regression)

### 17.2 Integration tests (`test/integration/`)
- `test_add_step_integration.py::test_add_step_full_flow_runs_local` — Style A example end-to-end
- `test_add_step_integration.py::test_foreach_rename_join` — AC-FOREACH-JOIN
- `test_add_step_packaging.py::test_embedded_callable_packaged_remotely` — AC-EMB-PKG
- `test_add_step_integration.py::test_default_fallback_end_to_end` — UD-6 end-to-end

### 17.3 E2E tests
- `test_add_step_e2e.py::test_three_examples_end_to_end` — all §3 examples
- `test_add_step_e2e.py::test_default_example_end_to_end` — Example 3 (defaults)

### 17.4 Observability / perf
- `bench_no_namespace.py::bench_no_namespace_io` — AC-NO-NS-IO (baseline compare)
- `bench_lazy_unread_input.py::bench_lazy_unread_input` — AC-LAZY (zero reads)

---

## 18. Accepted tradeoffs (Phase 1 limitations to document)

1. **`co_filename` divergence.** Runtime tracebacks for mutator-defined steps show `mutable_flow.py` frames, not the user's `transform()` body. `DAGNode.source_file` correctly resolves to the user's file via `inspect.unwrap(func)`, but Python provides no public API to rewrite `co_filename` cheaply. Documented in `docs/api/mutable_flow.md` as a Phase-1 traceback caveat.
2. **Third-party mutables not in denylist.** `numpy.ndarray`, `pandas.DataFrame`, custom mutable subclasses are NOT detected by `_MUTABLE_DEFAULT_TYPES`. Consistent with B006-style lint coverage. User responsibility.
3. **Producer-owned fan-out deferred to Phase 2.** One internal artifact → one external name. The consumer-owned namespace model precludes a producer artifact fanning out to multiple consumers with different externals.
4. **Qualname collision edge case.** Two classes defined in the same outer factory function share `__qualname__` (`factory.<locals>.Inner`). Vanishingly rare. Documented.
5. **`embedded()` alias detection has false negatives.** Static AST scan catches direct, qualified, and aliased-import calls. Misses runtime alias rebinding and `getattr(metaflow, "embedded")(...)`. Documented.
6. **Reload limitation for user-registered kinds with validators.** `importlib.reload(user_module)` produces a fresh validator function object → conflicting-spec ValueError. Phase 1 ships zero user-registered kinds; documented.
7. **Style A silent refactor fragility.** Renaming `produces=` on a producer or renaming a positional parameter on a consumer silently rewires inputs. Style B (`inputs={...}/outputs={...}`) retained as escape hatch.

---

## 19. Phase 2 follow-ups

Documented separately so the next phase can pick them up cleanly:

- **Producer-owned fan-out** — extend `outputs=` shape and the namespace resolver.
- **Embedded `FlowSpec` composition** — sub-flow embedding inside `add_step` bodies beyond the function-level `embedded()` sentinel.
- **`co_filename` rewrite via `types.CodeType.replace`** — full traceback fidelity for mutator-defined steps; CPython-only.
- **Producer-verification lint** — statically check that the producer step body assigns `self.<output> = ...` for every declared output.
- **Reload-safety for user-registered kinds with validators** — content-hash or qualname-based equality.
- **Card UI rendering** of new step kinds registered in `_data_flow_registry`.
- **Cross-flow type checking** — declared input/output types compared across producer/consumer boundaries.
- **Allowlist-based mutable-default detection** if false-negatives on third-party mutables (numpy/pandas) prove painful.

---

## 20. Implementation order suggestion

A staff engineer or agent can execute this design in roughly this order to minimize cross-cutting churn:

1. **Bundled registry first** — `metaflow/_data_flow_registry.py` + eager import in `__init__.py`. Pure addition; no other code reads it yet. Tests: `AC-REG*`.
2. **`FlowStateItems` enum extension** — add `PACKAGED_CALLABLES` and `PROCESSED_BY` to `flowspec.py`. No behavior change.
3. **`_namespaced_self.py`** + `embedded()` sentinel — pure addition, used only by Phase-1 advanced mode and Phase-2 substep.
4. **`__getattr__` overlay branch** — extend `flowspec.py:599-607`. Tests: `AC-NO-NS-IO`, `AC-LAZY`.
5. **`graph.py` early-return path for `_mf_edges`** — `_create_nodes` only; `_parse` untouched. Tests: `AC-AST-UNCHANGED`, `AC-P-001`.
6. **`node_to_dict` `data_flow` key** — empty-suppression preserves `AC-BYTE-EQ`.
7. **`mutable_flow.py::add_step` + `remove_step`** — wrapper synthesis, `_reject_mutable_defaults`, R1..R13, PROCESSED_BY guard. This is the largest piece (~520 LOC source).
8. **`task.py` overlay install point** — between `_update_env` and `task_pre_step`.
9. **`package/__init__.py`** — honor `PACKAGED_CALLABLES`.
10. **`lint.py` rules L-NS-005..009** — L-NS-006 mirrors `task.py:232-240` verbatim; L-NS-007 dual-scope scan.
11. **`tools/p001_baseline.txt`** committed last so `AC-AST-UNCHANGED` resolves.
12. **Docs** — `docs/api/mutable_flow.md`, `docs/design/flowoutput-forward-compat.md`.

Tests can be developed incrementally alongside each piece. The 58 acceptance criteria are the contract; every one has an executable verification command (most invoke pytest tests; a few exercise `git log -L`, `grep`, or `python -c`).
