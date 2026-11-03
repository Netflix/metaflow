import base64
from math import inf

import pytest

from metaflow import FlowSpec, step
from metaflow.plugins.argo.argo_workflows import ArgoWorkflows
from metaflow.plugins.argo.conditional_input_paths import generate_input_paths
from metaflow.util import compress_list, decompress_list

RUN_ID = "argo-run"


class ChainSkipReproFlow(FlowSpec):
    @step
    def start(self):
        self.route1 = "step2"
        self.next({"end": self.end, "step2": self.step2}, condition="route1")

    @step
    def step2(self):
        self.route2 = "end"
        self.next({"end": self.end, "step3": self.step3}, condition="route2")

    @step
    def step3(self):
        self.next(self.end)

    @step
    def end(self):
        pass


@pytest.fixture
def chain_skip_argo(mocker):
    mocker.patch.object(ArgoWorkflows, "_compile_workflow_template", return_value=None)
    mocker.patch.object(ArgoWorkflows, "_compile_sensor", return_value=None)

    return ArgoWorkflows(
        name="chain-skip-repro",
        graph=ChainSkipReproFlow._graph,
        flow=ChainSkipReproFlow(use_cli=False),
        code_package_metadata={},
        code_package_sha="sha",
        code_package_url="s3://metaflow/chain-skip-repro",
        production_token="token",
        metadata=None,
        flow_datastore=None,
        environment=None,
        event_logger=None,
        monitor=None,
        username="test-user",
    )


def _encode_input_paths(paths):
    return base64.b64encode(compress_list(paths, zlibmin=inf).encode("utf-8")).decode(
        "utf-8"
    )


def _decode_input_paths(paths):
    return decompress_list(paths)


def _task_path(step_name):
    return "%s/%s/%s-task" % (RUN_ID, step_name, step_name)


def _unresolved_task_path(step_name):
    return "%s/%s/{{{{tasks.%s.outputs.parameters.task-id}}}}" % (
        RUN_ID,
        step_name,
        step_name,
    )


def test_chain_skip_fallback_uses_latest_executed_split_switch(chain_skip_argo):
    node = chain_skip_argo.graph["end"]
    assert node.in_funcs == ["start", "step2", "step3"]

    skippable_steps = chain_skip_argo._skippable_input_steps_in_dag_order(node)
    assert skippable_steps == ["step2", "start"]

    input_paths = _encode_input_paths(
        [_task_path("start"), _task_path("step2"), _unresolved_task_path("step3")]
    )

    result = generate_input_paths(input_paths, skippable_steps)

    assert _decode_input_paths(result) == [_task_path("step2")]


@pytest.mark.parametrize(
    "paths, skippable_steps, expected",
    [
        (
            [_task_path("left"), _task_path("right")],
            [],
            [_task_path("left"), _task_path("right")],
        ),
        (
            [_task_path("start"), _task_path("branch")],
            ["start"],
            [_task_path("branch")],
        ),
        ([_task_path("step"), _task_path("step2")], ["step"], [_task_path("step2")]),
    ],
    ids=["normal_join", "non_skippable_executed", "exact_step_name"],
)
def test_generate_input_paths_filters_by_exact_step_name(
    paths, skippable_steps, expected
):
    result = generate_input_paths(_encode_input_paths(paths), skippable_steps)

    assert _decode_input_paths(result) == expected
