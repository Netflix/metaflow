"""Cross-version regression tests for how conditional steps reference the
outputs of a predecessor that never executed.

Argo resolves such a reference in two incompatible ways:

* `<3.7.16` / `<4.0.7` (incl. 3.6.x): a Skipped/Omitted node contributes no
  `outputs` to the scope at all. Simple `{{...}}` tags are left unsubstituted
  (`<3.7.11`) or make the controller requeue (`3.7.11`+, which introduced
  `ReplaceStrict`), and an expression that dereferences the missing `outputs`
  fails to substitute - the raw `{{=...}}` then reaches `shouldExecute()`,
  which rejects it as an invalid `when` expression and errors the task out.
* `>=3.7.16` / `>=4.0.7` (argoproj/argo-workflows#15932, #16223): a
  Skipped/Omitted node's *declared* output parameters are populated in scope,
  resolving to `valueFrom.default` when one is declared and to nil otherwise.

The generated templates therefore have to stay readable to both generations:
declare a `default` for every output a conditional successor may reference,
and never dereference a possibly-absent `outputs` unguarded.
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


def _when(aw, node_name):
    templates = aw._dag_templates()
    sanitized = ArgoWorkflows._sanitize(node_name)
    for task in templates[-1].payload["dag"]["tasks"]:
        if task["name"] == sanitized:
            return task.get("when")
    raise AssertionError("no DAG task found for step %r" % node_name)


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
    Argo <3.7.16/<4.0.7, where an Omitted predecessor has no `outputs` in
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


# ── foreach scopes closed out by a conditional join ──────────────────────────


def test_executed_task_id_expr_skips_non_executed_branches(foreach_argo):
    """Both "did not execute" shapes have to be skipped explicitly: an absent
    `outputs` (older Argo) and a `SKIPPED`/nil declared output (newer Argo).
    A plain `??` chain over `?.outputs` only handles the former - on
    >=3.7.16/>=4.0.7 `?.outputs` is never nil, so the chain would always
    settle on the first branch regardless of which one ran."""
    expr = foreach_argo._executed_task_id_expr(["b", "c"])

    assert expr == (
        "(get(tasks['b']?.outputs?.parameters, 'task-id') ?? 'SKIPPED') != 'SKIPPED'"
        " ? get(tasks['b']?.outputs?.parameters, 'task-id')"
        " : ((get(tasks['c']?.outputs?.parameters, 'task-id') ?? 'SKIPPED') != 'SKIPPED'"
        " ? get(tasks['c']?.outputs?.parameters, 'task-id')"
        " : ('SKIPPED'))"
    )
    assert "?? tasks[" not in expr


def test_foreach_template_task_id_output_uses_guarded_expression(foreach_argo):
    outputs = _template(foreach_argo, "start-foreach-items")["outputs"]["parameters"]
    (task_id,) = [parameter for parameter in outputs if parameter["name"] == "task-id"]

    assert task_id["valueFrom"]["expression"] == foreach_argo._executed_task_id_expr(
        ["b", "c"]
    )


# ── declared output defaults ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "node_name", ["start", "step2", "step3"], ids=["switch", "nested_switch", "linear"]
)
def test_conditional_task_id_declares_skipped_default(chain_skip_argo, node_name):
    """The `default` is what makes a Skipped/Omitted predecessor's task-id
    resolvable on Argo >=3.7.16/>=4.0.7 instead of requeuing forever."""
    node = chain_skip_argo.graph[node_name]

    assert chain_skip_argo._task_id_value_from(node) == {
        "path": "/mnt/out/task_id",
        "default": "SKIPPED",
    }


def test_foreach_join_predecessor_omits_task_id_default(foreach_argo):
    """The last node before a foreach join deliberately declares no default,
    so a branch that never ran surfaces as an Argo error instead of feeding a
    bogus task-id into the join (see _is_foreach_join_predecessor)."""
    branch = foreach_argo.graph["b"]

    assert foreach_argo._is_foreach_join_predecessor(branch)
    assert foreach_argo._task_id_value_from(branch) == {"path": "/mnt/out/task_id"}
    # ... while the switch feeding it, which is not a join predecessor, does
    # declare one.
    assert "default" in foreach_argo._task_id_value_from(foreach_argo.graph["fan"])
