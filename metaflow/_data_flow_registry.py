"""Standalone data-flow kind registry for graph mutation Phase 1.

Tracks the named "kinds" of dataflow that can appear on a step in
``_graph_info``. Phase 1 ships three built-in descriptive kinds; future
phases (notably FlowOutput) extend the vocabulary at import time.

Registration semantics (idempotent + fail-fast):
- Re-registering the same ``(kind, semantics, schema_validator)`` triple
  is a no-op. This makes ``importlib.reload(metaflow)`` safe for the
  built-in kinds.
- Re-registering a kind with a different spec raises ``ValueError``.

The ``validator`` hook is reserved for FlowOutput Phase 2 use and is not
exercised by any built-in kind in Phase 1.
"""

from typing import Callable, Dict, Optional


_REGISTRY: Dict[str, Dict[str, object]] = {}
_ALLOWED_SEMANTICS = {"descriptive", "routing"}


def register_data_flow_kind(
    kind: str,
    semantics: str = "descriptive",
    schema_validator: Optional[Callable[[dict], None]] = None,
) -> None:
    """Register a data-flow kind.

    Re-registration with the same spec is an idempotent no-op;
    re-registration with a different spec raises ``ValueError``.
    """
    if semantics not in _ALLOWED_SEMANTICS:
        raise ValueError(
            "semantics must be one of %s; got %r" % (_ALLOWED_SEMANTICS, semantics)
        )
    existing = _REGISTRY.get(kind)
    if existing is not None:
        if (
            existing["semantics"] == semantics
            and existing.get("validator") == schema_validator
        ):
            return
        raise ValueError(
            "data_flow kind %r already registered with semantics=%r"
            " (validator=%r); cannot re-register with semantics=%r"
            " (validator=%r)"
            % (
                kind,
                existing["semantics"],
                existing.get("validator"),
                semantics,
                schema_validator,
            )
        )
    _REGISTRY[kind] = {"semantics": semantics, "validator": schema_validator}


def get_kind(kind: str) -> Dict[str, object]:
    """Return the registered spec for ``kind``. Raises ``KeyError`` if absent."""
    return _REGISTRY[kind]


def list_kinds() -> Dict[str, Dict[str, object]]:
    """Return a shallow copy of the registry."""
    return dict(_REGISTRY)


# Built-in kinds — registered eagerly at import time.
register_data_flow_kind("explicit_inputs", "descriptive")
register_data_flow_kind("explicit_outputs", "descriptive")
register_data_flow_kind("embedded_callable", "descriptive")
