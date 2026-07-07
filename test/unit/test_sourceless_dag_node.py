"""
Tests for the sourceless DAGNode contract used by FunctionSpec.

FunctionSpec is an upcoming FlowSpec-like construct, currently shipped as an
out-of-tree extension, that synthesizes a single `@step(start=True, end=True)`
method via a metaclass that uses ``compile()`` + ``exec()``. Because the
synthetic step has no source file, ``inspect.getsourcelines()`` cannot recover
its source and ``ast.parse()`` cannot be applied.

Core metaflow does not ship FunctionSpec, but it does carry the support hooks
the extension relies on:

- ``DAGNode`` accepts optional ``name=`` / ``num_args=`` kwargs and tolerates
  ``func_ast=None``.
- ``FlowGraph._create_sourceless_single_step_node`` builds a DAGNode without
  AST parsing for ``start=True, end=True`` steps.
- The lint pipeline accepts the resulting graph.

This file pins those hooks against silent regressions. It uses ``compile()`` +
``exec()`` + ``type()`` to simulate what FunctionSpec's metaclass would
produce, without depending on the extension package.
"""

from metaflow import FlowSpec, step
from metaflow.lint import linter


def _make_dynamic_single_step_flow():
    namespace = {}
    exec(
        compile("def only(self):\n    self.x = 42\n", "<synthetic>", "exec"), namespace
    )
    only = step(start=True, end=True)(namespace["only"])
    return type(
        "DynamicSingleStepFlow",
        (FlowSpec,),
        {"__module__": __name__, "only": only},
    )


def test_dynamic_single_step_without_inspectable_source():
    """Dynamically-generated @step(start=True, end=True) works without source."""
    graph = _make_dynamic_single_step_flow()._graph

    assert graph.start_step == "only"
    assert graph.end_step == "only"
    assert graph["only"].type == "end"
    assert graph["only"].num_args == 1
    assert graph["only"].func_lineno == 1
    assert graph["only"].source_file == "<synthetic>"

    linter.run_checks(graph)
