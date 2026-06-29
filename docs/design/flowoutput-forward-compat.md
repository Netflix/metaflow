# FlowOutput / `data_flow` Forward-Compatibility Reference

This document is the auditable reference for the schema slot that the
Phase 1 graph-mutation work reserves for the future `FlowOutput`
feature. Phase 1 ships a standalone `_data_flow_registry` with three
built-in kinds. The full `FlowOutput` work (a separate design, not
implemented in this branch) will extend the registry vocabulary
additively.

## Per-step `data_flow` schema slot

In `_graph_info["steps"][<step_name>]`, mutator-touched steps may carry
a `data_flow` key. The entry shape is:

```python
{
    "kind": "<registered-kind-name>",
    "payload": {<kind-specific keys>}
}
```

- The `kind` field is a string that MUST be registered with
  `metaflow._data_flow_registry.register_data_flow_kind(...)` before
  it can appear here.
- The `payload` is an arbitrary JSON-compatible dict. Its shape is
  determined by the kind's optional `schema_validator` callable.
- Steps that do not declare explicit dataflow (the default — every
  step in flows that don't use `MutableFlow.add_step`) MUST NOT have a
  `data_flow` key. The key is suppressed when empty so `_graph_info`
  is byte-identical to today's output for unmutated flows.

## Phase 1 built-in kinds (registered eagerly in `metaflow/__init__.py`)

| Kind                  | Semantics    | Payload shape                                    |
|-----------------------|--------------|--------------------------------------------------|
| `explicit_inputs`     | descriptive  | `{"inputs": {<internal>: <external>, ...}}`      |
| `explicit_outputs`    | descriptive  | `{"outputs": {<internal>: <external>, ...}}`     |
| `embedded_callable`   | descriptive  | `{"callable_qualname": <str>}`                   |

`semantics="descriptive"` means: a consumer that does not recognize
the kind may safely ignore the entry. The Phase 1 runtime does not
inspect `data_flow` at all; it is metadata only.

## Phase 2 / FlowOutput vocabulary (RESERVED — not implemented here)

The `FlowOutput` design (separate spec, not present in this branch)
is expected to register at least the following kinds at import time:

| Kind                  | Semantics    | Payload shape (sketch — authoritative spec elsewhere) |
|-----------------------|--------------|-------------------------------------------------------|
| `flow_output`         | descriptive  | `{"name": str, "type": str, "doc": str}`              |
| `flow_input`          | descriptive  | `{"name": str, "type": str, "default": object}`       |
| `data_flow_route`     | routing      | `{"from": <step>, "to": <step>, "binding": dict}`     |
| `embedded_output`     | routing      | `{"source": <substep>, "external_name": str}`         |

`semantics="routing"` means: a consumer that does not recognize the
kind CANNOT safely ignore it because the runtime semantics depend on
that consumer reading the kind. The Phase 1 registry accepts both
`"descriptive"` and `"routing"` for forward compatibility; no Phase 1
kind uses `"routing"`.

## Validator hook (RESERVED for Phase 2)

`register_data_flow_kind(kind, semantics, schema_validator=None)`
accepts an optional `schema_validator` callable that may be invoked
on a payload before it is written to `_graph_info`. Phase 1 built-in
kinds register with `schema_validator=None`. The hook is reserved for
FlowOutput Phase 2 use; the Phase 1 registry stores the value but
never invokes it.

## Reload safety contract

`register_data_flow_kind` is idempotent on matching specs and raises
`ValueError` on conflicting specs. `importlib.reload(metaflow)` is
safe for the three built-in kinds (their specs match across reloads).

User modules that register custom kinds with a non-None
`schema_validator` are NOT reload-safe in Phase 1 because Python's
function-identity equality compares fresh post-reload function
objects as unequal. Phase 1 ships zero user-registered kinds.
Phase 2 should switch to a content-based equality (e.g., qualname +
source hash) if user-registered validators become common.

## Stability promise

- The `data_flow` key is **additive** in `_graph_info` and absent from
  unmutated flows. This is the byte-equivalence contract.
- Registered `kind` strings are **stable** — a kind name once registered
  retains its name and semantics across versions.
- Payload schemas may grow new fields additively but never remove or
  rename existing fields without a major-version bump.
