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
    assert start_resources["requests"]["memory"] == "8192M"

    assert end_resources["requests"]["cpu"] == "1"
    assert end_resources["requests"]["memory"] == "4096M"


def test_argo_error_hook_inherits_node_selector_and_tolerations(
    exec_mode, tag, scheduler_config, monkeypatch
):
    if exec_mode != "deployer":
        pytest.skip("Argo compilation tests require deployer mode")
    if scheduler_config.scheduler_type != "argo-workflows":
        pytest.skip("Argo compilation tests require the argo-workflows scheduler")

    from metaflow import Deployer

    from .test_utils import _resolve_flow_path, prepare_runner_deployer_args

    # Set environment variables for nodeSelector and tolerations
    monkeypatch.setenv(
        "METAFLOW_KUBERNETES_NODE_SELECTOR", '{"disktype": "ssd", "role": "compute"}'
    )
    monkeypatch.setenv(
        "METAFLOW_KUBERNETES_TOLERATIONS",
        '[{"key": "dedicated", "operator": "Equal", "value": "ml", "effect": "NoSchedule"}]',
    )

    deployed_flow = (
        Deployer(
            flow_file=_resolve_flow_path("basic/helloworld.py"),
            show_output=False,
            **prepare_runner_deployer_args({}),
        )
        .argo_workflows()
        .create(
            only_json=True,
            tags=tag + ["test_argo_error_hook_node_selector_tolerations"],
            **(scheduler_config.deploy_args or {}),
        )
    )

    workflow_template = deployed_flow.workflow_template
    assert workflow_template is not None

    # Find the error-msg-capture-hook template
    error_hook_template = None
    start_template = None
    for template in workflow_template.get("spec", {}).get("templates", []):
        if template.get("name") == "error-msg-capture-hook":
            error_hook_template = template
        annotations = template.get("metadata", {}).get("annotations", {})
        if annotations.get("metaflow/step_name") == "start":
            start_template = template

    assert error_hook_template is not None, "error-msg-capture-hook template not found"
    assert start_template is not None, "start step template not found"

    # Verify nodeSelector is present in both error hook and step template
    assert "nodeSelector" in error_hook_template
    assert error_hook_template["nodeSelector"] == {
        "disktype": "ssd",
        "role": "compute",
    }

    assert "nodeSelector" in start_template
    assert start_template["nodeSelector"] == {"disktype": "ssd", "role": "compute"}

    # Verify tolerations are present in both error hook and step template
    assert "tolerations" in error_hook_template
    assert error_hook_template["tolerations"] == [
        {
            "key": "dedicated",
            "operator": "Equal",
            "value": "ml",
            "effect": "NoSchedule",
        }
    ]

    assert "tolerations" in start_template
    assert start_template["tolerations"] == [
        {"key": "dedicated", "operator": "Equal", "value": "ml", "effect": "NoSchedule"}
    ]


def test_argo_error_hook_without_node_selector_and_tolerations(
    exec_mode, tag, scheduler_config, monkeypatch
):
    if exec_mode != "deployer":
        pytest.skip("Argo compilation tests require deployer mode")
    if scheduler_config.scheduler_type != "argo-workflows":
        pytest.skip("Argo compilation tests require the argo-workflows scheduler")

    from metaflow import Deployer

    from .test_utils import _resolve_flow_path, prepare_runner_deployer_args

    # Ensure the env vars are not set
    monkeypatch.delenv("METAFLOW_KUBERNETES_NODE_SELECTOR", raising=False)
    monkeypatch.delenv("METAFLOW_KUBERNETES_TOLERATIONS", raising=False)

    deployed_flow = (
        Deployer(
            flow_file=_resolve_flow_path("basic/helloworld.py"),
            show_output=False,
            **prepare_runner_deployer_args({}),
        )
        .argo_workflows()
        .create(
            only_json=True,
            tags=tag + ["test_argo_error_hook_no_node_selector_tolerations"],
            **(scheduler_config.deploy_args or {}),
        )
    )

    workflow_template = deployed_flow.workflow_template
    assert workflow_template is not None

    # Find the error-msg-capture-hook template
    error_hook_template = None
    for template in workflow_template.get("spec", {}).get("templates", []):
        if template.get("name") == "error-msg-capture-hook":
            error_hook_template = template

    assert error_hook_template is not None, "error-msg-capture-hook template not found"

    # Verify nodeSelector is either empty or not present
    node_selector = error_hook_template.get("nodeSelector", {})
    assert node_selector == {}, f"Expected empty nodeSelector, got {node_selector}"

    # Verify tolerations are not present or None
    tolerations = error_hook_template.get("tolerations")
    assert tolerations is None, f"Expected no tolerations, got {tolerations}"
