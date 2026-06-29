"""Advanced-mode (R8) `self` proxy and the `embedded()` runtime sentinel.

Phase 1 graph mutation: when a callable passed to ``MutableFlow.add_step``
takes a single positional argument named ``self`` (rule R8 in the design
memo), the wrapper invokes it through a ``_NamespacedSelf`` proxy. The
proxy is a thin pass-through to the underlying FlowSpec instance; the
heavy lifting (overlay-resolved attribute reads) lives in
``FlowSpec.__getattr__``.

``embedded(...)`` is a runtime sentinel used inside R8-mode bodies to
invoke helper callables. Lint rule L-NS-007 rejects ``embedded()``
calls in regular ``@step`` bodies and in Style A (no-self) user funcs
at graph-build time, so by the time runtime executes ``embedded(...)``,
the call is in an R8 context the user opted into.
"""

import sys
from typing import Any, Callable, Union


class _NamespacedSelf:
    """Proxy passed to advanced-mode (R8) step bodies as ``self``.

    Forwards all attribute access to the wrapped FlowSpec instance.
    Attribute reads go through ``FlowSpec.__getattr__``, which honors
    the per-task overlay installed by ``MetaflowTask.run_step``. Writes
    go to the underlying flow's ``__dict__`` so downstream steps see
    them as if the wrapper had written them directly.

    ``__slots__`` is intentionally NOT used — declaring slots with
    ``__dict__`` defeats the slots optimization, and per-instance
    state is negligible since the proxy holds only one reference.
    """

    # The wrapped flow is stored under a name unlikely to collide with
    # user attributes. We use object.__setattr__ to bypass our own
    # __setattr__ during construction.
    _MF_ORIGIN_ATTR = "_mf_namespaced_origin_flow"

    def __init__(self, flow: Any):
        object.__setattr__(self, _NamespacedSelf._MF_ORIGIN_ATTR, flow)

    def __getattr__(self, name: str) -> Any:
        # __getattr__ is only called when normal lookup fails, so we never
        # collide with our own slot/dict; just forward.
        return getattr(
            object.__getattribute__(self, _NamespacedSelf._MF_ORIGIN_ATTR), name
        )

    def __setattr__(self, name: str, value: Any) -> None:
        if name == _NamespacedSelf._MF_ORIGIN_ATTR:
            object.__setattr__(self, name, value)
            return
        setattr(
            object.__getattribute__(self, _NamespacedSelf._MF_ORIGIN_ATTR),
            name,
            value,
        )

    def __repr__(self) -> str:
        try:
            return "<_NamespacedSelf wrapping %r>" % (
                object.__getattribute__(self, _NamespacedSelf._MF_ORIGIN_ATTR),
            )
        except Exception:
            return "<_NamespacedSelf uninitialized>"


def embedded(target: Union[str, Callable]) -> Callable:
    """Sentinel for invoking a helper from inside an advanced-mode (R8) step.

    Two call forms are accepted:

    - ``embedded(callable_object)`` — returns the callable unchanged.
    - ``embedded("qualname")`` — resolves the name against the caller
      frame's locals first, then globals. Useful when the helper is
      imported by name at module level.

    Lint rule L-NS-007 rejects ``embedded()`` calls outside callables
    registered via ``MutableFlow.add_step``. By the time runtime
    executes ``embedded(...)``, the call site has been opted into R8
    mode.

    If the string form resolves to a non-callable or fails to resolve,
    ``RuntimeError`` is raised so the failure is loud rather than
    silently no-op.
    """
    if callable(target):
        return target
    if not isinstance(target, str):
        raise TypeError(
            "embedded() expects a callable or a string qualname; got %r" % (target,)
        )
    frame = sys._getframe(1)
    try:
        if target in frame.f_locals:
            obj = frame.f_locals[target]
        elif target in frame.f_globals:
            obj = frame.f_globals[target]
        else:
            raise RuntimeError(
                "embedded(%r): no callable named %r found in caller's "
                "frame locals or globals." % (target, target)
            )
    finally:
        del frame
    if not callable(obj):
        raise RuntimeError(
            "embedded(%r): resolved to a non-callable %r" % (target, obj)
        )
    return obj
