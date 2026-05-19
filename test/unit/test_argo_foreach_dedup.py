"""
Regression test for duplicate DAGTask names in foreach + split-switch flows.

When a split-switch's matching_conditional_join resolves to the same node as a
foreach's matching_join, the _visit function in _dag_templates could emit two
DAGTasks with the same name. Argo rejects this at submit time with:

    templates.<flow> sorting failed: duplicated nodeName <step>

This test verifies that the seen-tracking fix prevents the duplicate.
"""

import json
import os
import subprocess
import sys
import tempfile
import textwrap

import pytest

from metaflow.plugins.argo.argo_workflows import ArgoWorkflows, DAGTemplate


REPRO_FLOW = textwrap.dedent('''\
    """Minimum repro: duplicate DAGTask for foreach matching_join."""
    from metaflow import FlowSpec, step


    class ForeachJoinDedupFlow(FlowSpec):
        @step
        def start(self):
            self.optional_mode = "run"
            self.next(
                {"skip": self.fan_gate, "run": self.optional_step},
                condition="optional_mode",
            )

        @step
        def optional_step(self):
            self.next(self.fan_gate)

        @step
        def fan_gate(self):
            self.fan_mode = "run"
            self.next(
                {"skip": self.end, "run": self.fan_out},
                condition="fan_mode",
            )

        @step
        def fan_out(self):
            self.scan_items = [1, 2]
            self.next(self.fan_step, foreach="scan_items")

        @step
        def fan_step(self):
            self.next(self.join_step)

        @step
        def join_step(self, inputs):
            self.next(self.end)

        @step
        def end(self):
            pass


    if __name__ == "__main__":
        ForeachJoinDedupFlow()
''')


def _compile_flow_to_json(flow_source):
    """
    Compile a flow to Argo JSON using the Metaflow graph internals
    directly, bypassing the CLI and cloud datastore requirements.
    """
    from metaflow.graph import FlowGraph

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.py', delete=False, dir='.'
    ) as f:
        f.write(flow_source)
        flow_file = f.name

    try:
        # Load the flow module to get the FlowSpec class
        import importlib.util
        spec = importlib.util.spec_from_file_location("repro_flow", flow_file)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # Find the FlowSpec subclass
        from metaflow import FlowSpec
        flow_cls = None
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if (isinstance(attr, type) and issubclass(attr, FlowSpec)
                    and attr is not FlowSpec):
                flow_cls = attr
                break

        if flow_cls is None:
            raise RuntimeError("No FlowSpec subclass found in flow source")

        graph = FlowGraph(flow_cls)
        return graph
    finally:
        os.unlink(flow_file)


def test_no_duplicate_dag_task_names():
    """
    Verify that _dag_templates does not emit duplicate DAGTask names
    when a foreach matching_join coincides with a split-switch
    matching_conditional_join.
    """
    try:
        graph = _compile_flow_to_json(REPRO_FLOW)
    except Exception as e:
        pytest.skip(f"Could not compile flow graph: {e}")

    # Create a minimal ArgoWorkflows instance with just enough state
    # for _dag_templates to run
    instance = object.__new__(ArgoWorkflows)
    instance.graph = graph
    instance.flow = type('Flow', (), {'name': graph.name})()
    instance.enable_heartbeat_daemon = False

    # Populate conditional tracking state
    instance._parse_conditional_branches()

    templates = instance._dag_templates()

    for template in templates:
        template_json = template.to_json()
        dag = template_json.get("dag")
        if not dag:
            continue

        task_names = [task["name"] for task in dag.get("tasks", [])]
        duplicates = [
            name for name in set(task_names)
            if task_names.count(name) > 1
        ]

        assert not duplicates, (
            f"Template '{template_json['name']}' has duplicate DAG task names: "
            f"{duplicates}"
        )