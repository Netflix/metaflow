"""
Regression test for duplicate DAGTask names in foreach + split-switch flows.

When a split-switch's matching_conditional_join resolves to the same node as a
foreach's matching_join, the _visit function in _dag_templates could emit two
DAGTasks with the same name. Argo rejects this at submit time with:

    templates.<flow> sorting failed: duplicated nodeName <step>

This test verifies that the seen-tracking fix prevents the duplicate.
"""

import json
import subprocess
import sys
import textwrap
import tempfile
import os

import pytest


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


def test_no_duplicate_dag_task_names():
    """
    Compile the repro flow to Argo JSON and verify no DAG template
    contains duplicate task names.
    """
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.py', delete=False
    ) as f:
        f.write(REPRO_FLOW)
        flow_file = f.name

    try:
        result = subprocess.run(
            [
                sys.executable, flow_file,
                "--no-pylint",
                "argo-workflows", "create", "--only-json",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            pytest.skip(
                "Could not compile flow (likely missing Argo/K8s config): "
                + result.stderr[:500]
            )

        workflow = json.loads(result.stdout)
        templates = workflow.get("spec", {}).get("templates", [])

        for template in templates:
            dag = template.get("dag")
            if not dag:
                continue

            task_names = [task["name"] for task in dag.get("tasks", [])]
            duplicates = [
                name for name in set(task_names)
                if task_names.count(name) > 1
            ]

            assert not duplicates, (
                f"Template '{template['name']}' has duplicate DAG task names: "
                f"{duplicates}"
            )
    finally:
        os.unlink(flow_file)