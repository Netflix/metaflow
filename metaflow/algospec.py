"""
AlgoSpec -- a single-computation unit within Metaflow.

AlgoSpec is a FlowSpec subclass with a degenerate graph: one
synthesized "call" node (no start, no end, no self.next()). All
Metaflow infrastructure -- Parameters, flow decorators, CLI, configs --
works unchanged because AlgoSpec IS-A FlowSpec.

All divergent paths are gated on is_algo_spec = True.

Lifecycle:
    init()  -- called once per worker (model loading)
    call()  -- called per row or batch (computation)
"""

import atexit

from .flowspec import FlowSpec, FlowSpecMeta, FlowStateItems


class AlgoSpecMeta(FlowSpecMeta):
    """Metaclass for AlgoSpec.

    Extends FlowSpecMeta with:
    - _registry: collects all user-defined AlgoSpec subclasses
    - One-shot atexit handler on first subclass creation
    - Propagation of flow-level NflxResources to the synthesized call node
    """

    _registry = []
    _atexit_registered = False

    def __init__(cls, name, bases, attrs):
        if name == "AlgoSpec":
            type.__init__(cls, name, bases, attrs)
            return

        super().__init__(name, bases, attrs)

        if cls.call is AlgoSpec.call:
            from .exception import MetaflowException

            raise MetaflowException(
                "%s must implement call(). "
                "AlgoSpec subclasses require a call() method." % name
            )

        _propagate_flow_decorators_to_call_node(cls)

        AlgoSpecMeta._registry.append(cls)

        if not AlgoSpecMeta._atexit_registered:
            atexit.register(AlgoSpecMeta._on_exit)
            AlgoSpecMeta._atexit_registered = True

    @staticmethod
    def _on_exit():
        AlgoSpecMeta._registry.clear()


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

    cls._algo_spec_decos = list(call_node.decorators)


class AlgoSpec(FlowSpec, metaclass=AlgoSpecMeta):
    """Base class for single-computation algo specifications.

    Subclass this instead of FlowSpec when your algorithm is a single
    init() + call() unit with no multi-step DAG.
    """

    is_algo_spec = True

    _NON_PARAMETERS = FlowSpec._NON_PARAMETERS | {
        "init",
        "call",
        "is_algo_spec",
    }

    def init(self):
        """Called once per worker before any call() invocations."""
        pass

    def call(self, *args, **kwargs):
        """Called per row or batch. Must be overridden."""
        raise NotImplementedError("Subclasses must implement call()")

    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)
