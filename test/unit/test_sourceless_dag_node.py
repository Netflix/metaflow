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

import pytest

from metaflow import FlowSpec, step
from metaflow.lint import linter

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dynamic_single_step_flow_class():
    """Fixture to dynamically generate a FlowSpec class without an inspectable source file."""
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_sourceless_single_step_generates_valid_graph(dynamic_single_step_flow_class):
    """Test that a dynamically-generated @step(start=True, end=True) parses and lints correctly without source."""
    # Act
    graph = dynamic_single_step_flow_class._graph

    # Assert: Graph properties are correctly synthesized
    assert graph.start_step == "only"
    assert graph.end_step == "only"
    assert graph["only"].type == "end"
    assert graph["only"].num_args == 1
    assert graph["only"].func_lineno == 1
    assert graph["only"].source_file == "<synthetic>"

    # Assert: The synthesized graph passes standard lint checks without crashing
    linter.run_checks(graph)
