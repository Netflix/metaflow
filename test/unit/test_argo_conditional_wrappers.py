import pytest

from metaflow import FlowSpec, parallel, retry, step
from metaflow.plugins.argo.argo_workflows import ArgoWorkflows


class ConditionalForeachFlow(FlowSpec):
    @step
    def start(self):
        self.route = "fanout"
        self.next(
            {"fanout": self.fanout, "shortcut": self.shortcut},
            condition="route",
        )

    @step
    def fanout(self):
        self.items = [1, 2]
        self.next(self.worker, foreach="items")

    @step
    def worker(self):
        self.next(self.fanout_join)

    @step
    def fanout_join(self, inputs):
        self.next(self.outer_join)

    @step
    def shortcut(self):
        self.next(self.outer_join)

    @step
    def outer_join(self):
        self.next(self.end)

    @step
    def end(self):
        pass


class ConditionalParallelFlow(FlowSpec):
    @step
    def start(self):
        self.route = "fanout"
        self.next(
            {"fanout": self.fanout, "shortcut": self.shortcut},
            condition="route",
        )

    @step
    def fanout(self):
        self.next(self.parallel_worker, num_parallel=2)

    @retry(times=2)
    @parallel
    @step
    def parallel_worker(self):
        self.next(self.parallel_join)

    @step
    def parallel_join(self, inputs):
        self.next(self.outer_join)

    @step
    def shortcut(self):
        self.next(self.outer_join)

    @step
    def outer_join(self):
        self.next(self.end)

    @step
    def end(self):
        pass


@pytest.fixture
def conditional_foreach_argo(mocker):
    mocker.patch.object(ArgoWorkflows, "_compile_workflow_template", return_value=None)
    mocker.patch.object(ArgoWorkflows, "_compile_sensor", return_value=None)
    return ArgoWorkflows(
        name="conditional-foreach",
        graph=ConditionalForeachFlow._graph,
        flow=ConditionalForeachFlow(use_cli=False),
        code_package_metadata={},
        code_package_sha="sha",
        code_package_url="s3://metaflow/test",
        production_token="token",
        metadata=None,
        flow_datastore=None,
        environment=None,
        event_logger=None,
        monitor=None,
        username="test-user",
        enable_heartbeat_daemon=False,
    )


@pytest.fixture
def conditional_parallel_argo(mocker):
    mocker.patch.object(ArgoWorkflows, "_compile_workflow_template", return_value=None)
    mocker.patch.object(ArgoWorkflows, "_compile_sensor", return_value=None)
    return ArgoWorkflows(
        name="conditional-parallel",
        graph=ConditionalParallelFlow._graph,
        flow=ConditionalParallelFlow(use_cli=False),
        code_package_metadata={},
        code_package_sha="sha",
        code_package_url="s3://metaflow/test",
        production_token="token",
        metadata=None,
        flow_datastore=None,
        environment=None,
        event_logger=None,
        monitor=None,
        username="test-user",
        enable_heartbeat_daemon=False,
    )


def test_conditional_foreach_completion_is_normalized(conditional_foreach_argo):
    templates = conditional_foreach_argo._dag_templates()
    top_level = templates[-1].payload["dag"]["tasks"]
    tasks = {task["name"]: task for task in top_level}
    assert tasks["fanout-join"]["depends"] == (
        "fanout.Succeeded && "
        "(fanout-foreach-items.Succeeded || fanout-foreach-items.Skipped)"
    )
    assert tasks["outer-join"]["depends"] == (
        "fanout-join.Succeeded && shortcut.Succeeded"
    )

    by_name = {template.payload["name"]: template.payload for template in templates}
    wrapper = by_name["fanout-join"]
    assert wrapper["steps"][0][0]["template"] == "cond-fanout-join"
    assert wrapper["steps"][0][0]["when"] == (
        "{{inputs.parameters.should-run-fanout}} == true"
    )

    body_task = by_name["fanout-foreach-items"]["dag"]["tasks"][0]
    assert body_task["name"] == "worker"
    assert "should-run-fanout" not in {
        parameter["name"] for parameter in body_task["arguments"]["parameters"]
    }
    body_wrapper = by_name["worker"]
    assert "should-run-fanout" not in {
        parameter["name"] for parameter in body_wrapper["inputs"]["parameters"]
    }
    assert "when" not in body_wrapper["steps"][0][0]
    assert "outputs" not in by_name["fanout-foreach-items"]


def test_conditional_parallel_resource_is_wrapped(conditional_parallel_argo):
    templates = conditional_parallel_argo._dag_templates()
    by_name = {template.payload["name"]: template.payload for template in templates}
    wrapper = by_name["parallel-worker"]
    assert wrapper["steps"][0][0]["template"] == "cond-parallel-worker"
    assert "when" not in wrapper["steps"][0][0]
    assert "should-run-fanout" not in {
        parameter["name"] for parameter in wrapper["inputs"]["parameters"]
    }
    assert {parameter["name"] for parameter in wrapper["outputs"]["parameters"]} == {
        "should-run",
        "num-parallel",
        "task-id-entropy",
    }

    body_task = by_name["fanout-foreach-parallel"]["dag"]["tasks"][0]
    body_parameters = {
        parameter["name"]: parameter["value"]
        for parameter in body_task["arguments"]["parameters"]
    }
    assert "should-run-fanout" not in body_parameters
    assert body_parameters["retryCount"] == "{{retries}}"
    assert body_parameters["jobset-name"] == (
        "js-{{inputs.parameters.task-id-entropy}}{{retries}}"
    )
    inner_parameters = {
        parameter["name"]: parameter["value"]
        for parameter in wrapper["steps"][0][0]["arguments"]["parameters"]
    }
    assert inner_parameters["retryCount"] == "{{inputs.parameters.retryCount}}"
    assert inner_parameters["jobset-name"] == "{{inputs.parameters.jobset-name}}"
    assert "outputs" not in by_name["fanout-foreach-parallel"]

    join_wrapper = by_name["parallel-join"]
    assert join_wrapper["steps"][0][0]["when"] == (
        "{{inputs.parameters.should-run-fanout}} == true"
    )

    top_level = templates[-1].payload["dag"]["tasks"]
    tasks = {task["name"]: task for task in top_level}
    assert tasks["parallel-join"]["depends"] == (
        "fanout.Succeeded && "
        "(fanout-foreach-parallel.Succeeded || fanout-foreach-parallel.Skipped)"
    )
    assert tasks["outer-join"]["depends"] == (
        "parallel-join.Succeeded && shortcut.Succeeded"
    )
