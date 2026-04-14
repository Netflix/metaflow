"""
FunctionSpec -- a single-computation unit within Metaflow.

FunctionSpec is a FlowSpec subclass with a degenerate graph: one
synthesized "call" node (no start, no end, no self.next()). All
Metaflow infrastructure -- Parameters, flow decorators, CLI, configs --
works unchanged because FunctionSpec IS-A FlowSpec.

All divergent paths are gated on is_function_spec = True.

Lifecycle:
    init()  -- called once per worker (model loading)
    call()  -- called per row or batch (computation)
"""

import atexit

from .flowspec import FlowSpec, FlowSpecMeta, FlowStateItems


class FunctionSpecMeta(FlowSpecMeta):
    """Metaclass for FunctionSpec.

    Extends FlowSpecMeta with:
    - _registry: collects all user-defined FunctionSpec subclasses
    - One-shot atexit handler on first subclass creation
    - Propagation of flow-level NflxResources to the synthesized call node
    """

    _registry = []
    _atexit_registered = False

    def __init__(cls, name, bases, attrs):
        if name == "FunctionSpec":
            type.__init__(cls, name, bases, attrs)
            return

        super().__init__(name, bases, attrs)

        if cls.call is FunctionSpec.call:
            from .exception import MetaflowException

            raise MetaflowException(
                "%s must implement call(). "
                "FunctionSpec subclasses require a call() method." % name
            )

        _propagate_flow_decorators_to_call_node(cls)

        FunctionSpecMeta._registry.append(cls)

        if not FunctionSpecMeta._atexit_registered:
            atexit.register(FunctionSpecMeta._on_exit)
            FunctionSpecMeta._atexit_registered = True

    @staticmethod
    def _on_exit():
        FunctionSpecMeta._registry.clear()


def _propagate_flow_decorators_to_call_node(cls):
    """Convert flow-level NflxResources into a step-level ResourcesDecorator
    on the synthesized call node, so runtime hooks fire normally."""

    if not hasattr(cls, "_graph") or "call" not in cls._graph:
        return

    call_node = cls._graph["call"]
    flow_decos = cls._flow_state.get(FlowStateItems.FLOW_DECORATORS, {})

    nflx_res = flow_decos.get("nflx_resources")
    if nflx_res:
        from .plugins.resources_decorator import ResourcesDecorator

        deco = ResourcesDecorator(
            attributes=nflx_res[0].attributes, statically_defined=True
        )
        call_node.decorators.append(deco)

    cls._function_spec_decos = list(call_node.decorators)


class FunctionSpec(FlowSpec, metaclass=FunctionSpecMeta):
    """Base class for single-computation algo specifications.

    Subclass this instead of FlowSpec when your algorithm is a single
    init() + call() unit with no multi-step DAG.
    """

    is_function_spec = True

    _NON_PARAMETERS = FlowSpec._NON_PARAMETERS | {
        "init",
        "call",
        "is_function_spec",
    }

    def init(self):
        """Called once per worker before any call() invocations."""
        pass

    def call(self, *args, **kwargs):
        """Called per row or batch. Must be overridden."""
        raise NotImplementedError("Subclasses must implement call()")

    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)
