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

from .flowspec import FlowSpec, FlowSpecMeta


class AlgoSpecMeta(FlowSpecMeta):
    """Metaclass for AlgoSpec.

    Extends FlowSpecMeta with:
    - _registry: collects all user-defined AlgoSpec subclasses
    - One-shot atexit handler on first subclass creation

    NflxResources propagation to the call node happens in
    NflxResources.flow_init (correct timing -- after decorators
    are collected, before Maestro builds the workflow).
    """

    _registry = []
    _atexit_registered = False

    def __init__(cls, name, bases, attrs):
        if name == "AlgoSpec":
            # Let FlowSpecMeta set up _flow_state (needed by subclass MRO walk).
            # Graph building works fine -- SyntheticDAGNode handles the base call().
            super().__init__(name, bases, attrs)
            return

        super().__init__(name, bases, attrs)

        if cls.call is AlgoSpec.call:
            from .exception import MetaflowException

            raise MetaflowException(
                "%s must implement call(). "
                "AlgoSpec subclasses require a call() method." % name
            )

        AlgoSpecMeta._registry.append(cls)

        if not AlgoSpecMeta._atexit_registered:
            atexit.register(AlgoSpecMeta._on_exit)
            AlgoSpecMeta._atexit_registered = True

    def _init_graph(cls):
        from .graph import FlowGraph

        cls._graph = FlowGraph(cls)
        # Use the SyntheticDAGNode itself as the "step" — it has .decorators
        # and .name, which is all _attach_decorators_to_step needs. This lets
        # runtime decorator attachment (e.g. @titus via DEFAULT_DECOSPECS)
        # work the same as for normal FlowSpec steps.
        if cls._graph.is_algo_spec and "call" in cls._graph:
            cls._steps = [cls._graph["call"]]
        else:
            cls._steps = []

    @staticmethod
    def _on_exit():
        AlgoSpecMeta._registry.clear()


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
