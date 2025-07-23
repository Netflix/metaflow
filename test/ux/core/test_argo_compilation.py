import pytest

pytestmark = [pytest.mark.argo_compilation, pytest.mark.scheduler_only]


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


def _container_template_for_step(workflow_template, step_name):
    for template in workflow_template.get("spec", {}).get("templates", []):
        annotations = template.get("metadata", {}).get("annotations", {})
        if (
            annotations.get("metaflow/step_name") == step_name
            and "container" in template
        ):
            return template
    raise AssertionError(
        "No container template found for step %r in workflow template" % step_name
    )


def test_argo_only_json_exposes_workflow_template(
    exec_mode, decospecs, tag, scheduler_config
):
    if exec_mode != "deployer":
        pytest.skip("Argo compilation tests require deployer mode")
    if scheduler_config.scheduler_type != "argo-workflows":
        pytest.skip("Argo compilation tests require the argo-workflows scheduler")

    from metaflow import Deployer

    from .test_utils import _resolve_flow_path, prepare_runner_deployer_args

    deployed_flow = (
        Deployer(
            flow_file=_resolve_flow_path("basic/helloworld.py"),
            show_output=False,
            **prepare_runner_deployer_args({"decospecs": decospecs}),
        )
        .argo_workflows()
        .create(
            only_json=True,
            tags=tag + ["test_argo_only_json_exposes_workflow_template"],
            **(scheduler_config.deploy_args or {}),
        )
    )

    workflow_template = deployed_flow.workflow_template
    assert workflow_template is not None
    assert workflow_template["kind"] == "WorkflowTemplate"
    assert workflow_template["metadata"]["name"] == deployed_flow.name
    assert workflow_template["spec"]["templates"]


def test_foreach_split_switch_join_task_names_are_deduplicated(
    exec_mode, decospecs, tag, scheduler_config
):
    if exec_mode != "deployer":
        pytest.skip("Argo compilation tests require deployer mode")
    if scheduler_config.scheduler_type != "argo-workflows":
        pytest.skip("Argo compilation tests require the argo-workflows scheduler")

    from metaflow import Deployer

    from .test_utils import _resolve_flow_path, prepare_runner_deployer_args

    deployed_flow = (
        Deployer(
            flow_file=_resolve_flow_path("dag/foreach_split_switch_dedup_flow.py"),
            show_output=False,
            **prepare_runner_deployer_args({"decospecs": decospecs}),
        )
        .argo_workflows()
        .create(
            only_json=True,
            tags=tag + ["test_argo_foreach_split_switch_dedup"],
            **(scheduler_config.deploy_args or {}),
        )
    )

    workflow_template = deployed_flow.workflow_template
    assert workflow_template is not None
    assert _find_duplicate_task_names(workflow_template) == {}


def test_late_attached_kubernetes_mutator_is_reflected_in_argo_template(
    exec_mode, tag, scheduler_config
):
    if exec_mode != "deployer":
        pytest.skip("Argo compilation tests require deployer mode")
    if scheduler_config.scheduler_type != "argo-workflows":
        pytest.skip("Argo compilation tests require the argo-workflows scheduler")

    from metaflow import Deployer

    from .test_utils import _resolve_flow_path, prepare_runner_deployer_args

    deployed_flow = (
        Deployer(
            flow_file=_resolve_flow_path(
                "decorators/late_attached_kubernetes_mutator_flow.py"
            ),
            show_output=False,
            **prepare_runner_deployer_args({}),
        )
        .argo_workflows()
        .create(
            only_json=True,
            tags=tag + ["test_late_attached_kubernetes_mutator"],
            **(scheduler_config.deploy_args or {}),
        )
    )

    workflow_template = deployed_flow.workflow_template
    assert workflow_template is not None

    start_resources = _container_template_for_step(workflow_template, "start")[
        "container"
    ]["resources"]
    end_resources = _container_template_for_step(workflow_template, "end")["container"][
        "resources"
    ]

    assert start_resources["requests"]["cpu"] == "2"
    assert start_resources["requests"]["memory"] == "8192Mi"

    assert end_resources["requests"]["cpu"] == "1"
    assert end_resources["requests"]["memory"] == "4096Mi"
