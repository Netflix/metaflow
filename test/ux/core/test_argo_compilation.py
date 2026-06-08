import pytest
from metaflow import Deployer

from .test_utils import _resolve_flow_path, prepare_runner_deployer_args

pytestmark = [pytest.mark.argo_compilation, pytest.mark.scheduler_only]


# ---------------------------------------------------------------------------
# Helpers and Assertion Callbacks
# ---------------------------------------------------------------------------


def _find_duplicate_task_names(workflow_template):
    duplicates = {}
    for template in workflow_template.get("spec", {}).get("templates", []):
        dag = template.get("dag")
        if not dag:
            continue
        task_names = [task["name"] for task in dag.get("tasks", [])]
        duplicate_names = sorted(
            name for name in set(task_names) if task_names.count(name) > 1
        )
        if duplicate_names:
            duplicates[template["name"]] = duplicate_names
    return duplicates


def _assert_only_json_structure(workflow_template, deployed_flow_name):
    """Verify the foundational structure of the generated Argo WorkflowTemplate."""
    assert workflow_template is not None
    assert workflow_template["kind"] == "WorkflowTemplate"
    assert workflow_template["metadata"]["name"] == deployed_flow_name
    assert workflow_template["spec"]["templates"]


def _assert_deduplicated_task_names(workflow_template, deployed_flow_name):
    """Verify that complex DAG topologies do not produce duplicate task names in Argo."""
    assert workflow_template is not None
    assert _find_duplicate_task_names(workflow_template) == {}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flow_name, test_suffix, assertion_fn",
    [
        pytest.param(
            "basic/helloworld.py",
            "only_json_exposes_workflow_template",
            _assert_only_json_structure,
            id="only_json_structure",
        ),
        pytest.param(
            "dag/foreach_split_switch_dedup_flow.py",
            "foreach_split_switch_dedup",
            _assert_deduplicated_task_names,
            id="task_name_deduplication",
        ),
    ],
)
def test_argo_compilation_behaviors(
    exec_mode, decospecs, tag, scheduler_config, flow_name, test_suffix, assertion_fn
):
    """Parametrized test covering Argo JSON compilation outputs and structural integrity."""
    if exec_mode != "deployer":
        pytest.skip("Argo compilation tests require deployer mode")
    if scheduler_config.scheduler_type != "argo-workflows":
        pytest.skip("Argo compilation tests require the argo-workflows scheduler")

    deployed_flow = (
        Deployer(
            flow_file=_resolve_flow_path(flow_name),
            show_output=False,
            **prepare_runner_deployer_args({"decospecs": decospecs}),
        )
        .argo_workflows()
        .create(
            only_json=True,
            tags=tag + [f"test_argo_{test_suffix}"],
            **(scheduler_config.deploy_args or {}),
        )
    )

    assertion_fn(deployed_flow.workflow_template, deployed_flow.name)
