"""Regression tests for how conditional steps reference the outputs of a
predecessor that never executed.

Argo's treatment of such a reference differs by version:

* `<3.7.11`: unresolved `{{...}}` tags are passed through literally.
* `3.7.11`-`3.7.12` / `4.0.2`-`4.0.3`: the controller requeues forever when
  any referenced variable is missing (argoproj/argo-workflows#15442), so the
  downstream task is never created. Rejected at deploy time.
* `>=3.7.13` / `>=4.0.4`: a skipped ancestor's outputs are in scope with
  empty values (#15841).

The generated templates never emit a bare `{{tasks.X.outputs...}}` tag for a
possibly-Omitted `X`; every such access lives inside a `{{=...}}` expression
gated on a **positive** `.status == 'Succeeded'` check.

Two operators are banned from these expressions:
* `?.` - misbehaves for tasks inside foreach-templated DAGs.
* `??` - once an Omitted node's outputs are in scope, a `??` chain never
  falls through and always settles on the first branch.
"""

import pytest

from metaflow import FlowSpec, step
from metaflow.plugins.argo.argo_workflows import ArgoWorkflows


# ── Flows ────────────────────────────────────────────────────────────────────


class ChainSkipFlow(FlowSpec):
    """Two switches that can each jump straight to `end`. When `start` routes
    to `end`, `step2` is Omitted while `end`'s `depends` is still satisfied
    through `start` - so `end`'s `when` is evaluated with an Omitted switch
    predecessor in scope."""

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


class ConditionalInForeachFlow(FlowSpec):
    """A switch inside a foreach: the foreach scope is closed out by a
    conditional join, so only one of `b`/`c` has a real task-id per item."""

    @step
    def start(self):
        self.items = [1, 2, 3]
        self.next(self.fan, foreach="items")

    @step
    def fan(self):
        self.route = "b"
        self.next({"b": self.b, "c": self.c}, condition="route")

    @step
    def b(self):
        self.next(self.fjoin)

    @step
    def c(self):
        self.next(self.fjoin)

    @step
    def fjoin(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_argo(mocker, flow_cls, name):
    mocker.patch.object(ArgoWorkflows, "_compile_workflow_template", return_value=None)
    mocker.patch.object(ArgoWorkflows, "_compile_sensor", return_value=None)
    return ArgoWorkflows(
        name=name,
        graph=flow_cls._graph,
        flow=flow_cls(use_cli=False),
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


def _dag_task(aw, node_name):
    templates = aw._dag_templates()
    sanitized = ArgoWorkflows._sanitize(node_name)
    for task in templates[-1].payload["dag"]["tasks"]:
        if task["name"] == sanitized:
            return task
    raise AssertionError("no DAG task found for step %r" % node_name)


def _when(aw, node_name):
    return _dag_task(aw, node_name).get("when")


def _param(aw, node_name, param_name):
    for parameter in _dag_task(aw, node_name)["arguments"]["parameters"]:
        if parameter["name"] == param_name:
            return parameter["value"]
    raise AssertionError("no %r parameter on step %r" % (param_name, node_name))


def _template(aw, template_name):
    for template in aw._dag_templates():
        if template.payload["name"] == template_name:
            return template.payload
    raise AssertionError("no DAG template named %r" % template_name)


@pytest.fixture
def chain_skip_argo(mocker):
    return _make_argo(mocker, ChainSkipFlow, "chain-skip")


@pytest.fixture
def foreach_argo(mocker):
    return _make_argo(mocker, ConditionalInForeachFlow, "cond-in-foreach")


# ── `when` clauses ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "node_name, switch_in_funcs",
    [("end", ["start", "step2"]), ("step2", ["start"]), ("step3", ["step2"])],
    ids=["conditional_skip_join", "first_branch", "second_switch_branch"],
)
def test_switch_when_guards_predecessor_status(
    chain_skip_argo, node_name, switch_in_funcs
):
    """A `when` clause may only read a switch predecessor's `switch-step`
    behind a `.status == 'Succeeded'` check. Reading it unguarded breaks on
    Argo <3.7.13, where an Omitted predecessor has no `outputs` in
    scope: substitution leaves the raw `{{=...}}`, which `shouldExecute()`
    then rejects with "Invalid token: '{{='" and errors the task out."""
    when = _when(chain_skip_argo, node_name)

    for in_func in switch_in_funcs:
        sanitized = ArgoWorkflows._sanitize(in_func)
        guarded = (
            "(tasks['%s'].status == 'Succeeded'"
            " ? tasks['%s'].outputs.parameters['switch-step']"
            " : nil) == '%s'" % (sanitized, sanitized, node_name)
        )
        assert guarded in when
        # No unguarded lookup anywhere else in the expression.
        assert when.count("tasks['%s'].outputs" % sanitized) == when.count(guarded)


# ── input-paths ──────────────────────────────────────────────────────────────


def test_conditional_input_paths_use_status_gated_expression(chain_skip_argo):
    """`end` has conditional predecessors, so its input-paths must be a single
    status-gated expression rather than bare per-predecessor tags. A bare tag
    for an Omitted predecessor resolves to an empty task-id on Argo >=3.7.13,
    producing a broken pathspec that crashes the step."""
    value = _param(chain_skip_argo, "end", "input-paths")

    assert value.startswith("{{=sprig.trimSuffix(',',")
    for in_func in ("start", "step2", "step3"):
        sanitized = ArgoWorkflows._sanitize(in_func)
        assert (
            "(tasks['%s'].status == 'Succeeded'"
            " ? 'argo-' + workflow.name + '/%s/'"
            " + tasks['%s'].outputs.parameters['task-id'] + ','"
            " : '')" % (sanitized, in_func, sanitized)
        ) in value


def test_non_conditional_input_paths_stay_plain(chain_skip_argo):
    """A step whose predecessors always run keeps the cheaper bare-tag form -
    there is nothing to guard against."""
    value = _param(chain_skip_argo, "step2", "input-paths")

    assert value == chain_skip_argo._input_path_ref("start")
    assert "status ==" not in value


def test_input_paths_expression_bans_unsafe_operators(chain_skip_argo):
    """Neither `?.` nor `??` may appear - both are version-dependent."""
    value = _param(chain_skip_argo, "end", "input-paths")

    assert "?." not in value
    assert "??" not in value


# ── foreach scopes closed out by a conditional join ──────────────────────────


def test_executed_task_id_expr_uses_positive_status_chain(foreach_argo):
    """The task-id of whichever branch ran is picked with a positive
    `.status == 'Succeeded'` chain. The previous `?.`/`??` formulation broke
    once Omitted outputs are in scope (>=3.7.13), where `?.outputs` is never
    nil so the chain always settled on the first branch."""
    expr = foreach_argo._executed_task_id_expr(["b", "c"])

    assert expr == (
        "tasks['b'].status == 'Succeeded'"
        " ? tasks['b'].outputs.parameters['task-id']"
        " : (tasks['c'].status == 'Succeeded'"
        " ? tasks['c'].outputs.parameters['task-id']"
        " : ('SKIPPED'))"
    )
    assert "?." not in expr
    assert "??" not in expr


def test_foreach_template_task_id_output_uses_guarded_expression(foreach_argo):
    outputs = _template(foreach_argo, "start-foreach-items")["outputs"]["parameters"]
    (task_id,) = [parameter for parameter in outputs if parameter["name"] == "task-id"]

    assert task_id["valueFrom"]["expression"] == foreach_argo._executed_task_id_expr(
        ["b", "c"]
    )
