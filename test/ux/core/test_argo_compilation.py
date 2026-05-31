import pytest

pytestmark = [pytest.mark.argo_compilation, pytest.mark.scheduler_only]


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
