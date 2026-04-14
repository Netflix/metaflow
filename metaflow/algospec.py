"""
AlgoSpec -- a single-computation unit within Metaflow.

AlgoSpec is a FlowSpec subclass with a single step: the call method,
marked as @step(start=True, end=True). FlowGraph._identify_start_end
picks it up via the is_start/is_end attributes — no special-casing
needed in graph traversal, lint, runtime, or Maestro.

Lifecycle:
    init()  -- called once per worker (model loading)
    call()  -- called per row or batch (computation)
"""

import atexit

from .flowspec import FlowSpec, FlowSpecMeta


class AlgoSpecMeta(FlowSpecMeta):
    """Metaclass for AlgoSpec.

    Marks call() as @step(start=True, end=True) before FlowSpecMeta
    builds the graph. The step name is the class name lowercased.
    """

    _registry = []
    _atexit_registered = False

    def __init__(cls, name, bases, attrs):
        if name == "AlgoSpec":
            super().__init__(name, bases, attrs)
            return

        from .decorators import step

        call_fn = attrs.get("call")
        if call_fn is not None and callable(call_fn):
            attrs["call"] = step(call_fn, start=True, end=True)
            attrs["call"].name = name.lower()
            attrs["call"].__name__ = name.lower()
            cls.call = attrs["call"]
            cls._algo_step_name = name.lower()

        super().__init__(name, bases, attrs)

        if not hasattr(cls.call, "is_step"):
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
        # The method is cls.call but node.name is the class-derived name.
        if cls._graph.is_algo_spec:
            cls._steps = [cls.call]
        else:
            cls._steps = [getattr(cls, node.name) for node in cls._graph]

    @staticmethod
    def _on_exit():
        AlgoSpecMeta._registry.clear()


class AlgoSpec(FlowSpec, metaclass=AlgoSpecMeta):
    """Base class for single-computation algo specifications."""

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

    def call(self):
        """Main computation. Must be overridden."""
        raise NotImplementedError("Subclasses must implement call()")

    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)

    def __getattr__(self, name):
        # Resolve the class-derived step name to the call method
        if name == getattr(self.__class__, "_algo_step_name", None):
            return self.call
        return super().__getattr__(name)
