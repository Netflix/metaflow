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

    Key design: decorators live on the call METHOD (like @step does for
    FlowSpec functions). SyntheticDAGNode reads from the method. This
    means _attach_decorators modifies the method's list, and when
    _init_graph is called again, the new SyntheticDAGNode picks them up.
    """

    _registry = []
    _atexit_registered = False

    def __init__(cls, name, bases, attrs):
        if name == "AlgoSpec":
            super().__init__(name, bases, attrs)
            return

        # Stamp call with @step-like attributes BEFORE super().__init__,
        # which calls _init_graph -> SyntheticDAGNode reads from these.
        call_fn = attrs.get("call")
        if call_fn is not None and callable(call_fn):
            call_fn.is_step = True
            call_fn.decorators = []
            call_fn.wrappers = []
            call_fn.config_decorators = []
            call_fn.name = "call"

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
        # _steps contains the call method (stamped with .decorators etc.)
        # so _attach_decorators works. Same pattern as FlowSpec where
        # _steps contains @step-decorated functions.
        if cls._graph.is_algo_spec:
            cls._steps = [cls.call]
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

    _EPHEMERAL = FlowSpec._EPHEMERAL | {"is_algo_spec"}

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
