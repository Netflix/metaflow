import os
import uuid
import pytest

pytestmark = [pytest.mark.triggers, pytest.mark.scheduler_only]
from .test_utils import (
    deploy_flow_to_scheduler,
    disp_test,
    wait_for_deployed_run,
    verify_single_run,
    run_flow_with_env,
)


def _require_trigger_infra(scheduler_config):
    """Skip if the backend lacks event/trigger infrastructure.

    @trigger_on_finish requires:
      - argo-kubernetes: Argo Events (sensor + eventsource CRDs)
      - sfn-batch: EventBridge rules
      - airflow-kubernetes: Airflow sensors
    The devstack CI does not yet include these components.
    """
    sched_type = scheduler_config.scheduler_type
    if sched_type is None:
        pytest.skip("No scheduler configured")

    if "argo" in sched_type:
        webhook_url = os.environ.get("ARGO_EVENTS_WEBHOOK_URL", "")
        if not webhook_url:
            pytest.skip("Argo Events not configured (ARGO_EVENTS_WEBHOOK_URL unset)")
    elif "step-functions" in sched_type:
        pytest.skip("@trigger_on_finish not yet supported on sfn-batch in CI")
    elif "airflow" in sched_type:
        pytest.skip("@trigger_on_finish not yet supported on airflow in CI")


def verify_param(expected, actual, param_name):
    """Verify a single parameter value matches expected."""
    if actual != expected:
        raise RuntimeError(
            f"Expecting trigger with '{param_name}={expected}', "
            f"but got '{param_name}={actual}'"
        )


def verify_event_name(expected, actual):
    """Verify a single event name matches expected."""
    if actual != expected:
        raise RuntimeError(f"Expecting trigger to be '{expected}', but got '{actual}'")


def verify_event_names_set(expected_set, actual_set):
    """Verify a set of event names matches expected."""
    if actual_set != expected_set:
        raise RuntimeError(
            f"Expecting triggers to be '{expected_set}' but got '{actual_set}'"
        )


def verify_pathspec(expected, actual):
    """Verify event id/pathspec matches expected."""
    if actual != expected:
        raise RuntimeError(f"Expecting event id to be '{expected}', but got '{actual}'")


def setup_test_deployment(test_name, exec_mode, decospecs, tag, scheduler_config):
    """Common test setup for deployment tests."""
    _require_trigger_infra(scheduler_config)
    test_unique_tag = f"{test_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]
    disp_test(exec_mode, decospecs, tag, scheduler_config)
    return combined_tags


def create_branch_config(project="dummy_project", branch=None, **extra_config):
    """Create a config dictionary with branch and project."""
    if branch is None:
        branch = str(uuid.uuid4())[:8]
    config = {"project": project, "branch": branch}
    config.update(extra_config)
    return config, branch


def deploy_trigger_flow(
    flow_path,
    config_value,
    combined_tags,
    scheduler_config,
    decospecs,
    compute_env,
    **extra_tl_args,
):
    """Deploy a flow with standard trigger test configuration."""
    tl_args = {
        "decospecs": decospecs,
        "config_value": [("config", config_value)],
        "env": compute_env,
    }
    tl_args.update(extra_tl_args)

    return deploy_flow_to_scheduler(
        flow_name=flow_path,
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags},
        scheduler_type=scheduler_config.scheduler_type,
    )


def test_static_trigger_on_finish_static_dict(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    # Setup
    combined_tags = setup_test_deployment(
        "test_static_trigger_on_finish_static_dict",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    flow_name = "DummyTriggerFlow1"
    config_value, branch = create_branch_config(
        project_branch=f"test.{str(uuid.uuid4())[:8]}"
    )
    config_value["project_branch"] = f"test.{branch}"

    # Deploy upstream flow
    upstream_flow = deploy_trigger_flow(
        "triggers/dummy_trigger_flow1.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Deploy downstream flow
    deploy_trigger_flow(
        "triggers/hello_trigger_on_finish_static_dict.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Trigger downstream flow by running the upstream one
    upstream_run = wait_for_deployed_run(upstream_flow)

    # Verify
    run = verify_single_run("TriggerOnFinishStaticDictFlow", tags=combined_tags)
    all_data = run["start"].task.data
    expected_trigger = f"metaflow.{flow_name}.end"

    # Verify event name
    verify_event_name(expected_trigger, all_data.trigger.event.name)

    # Verify event task pathspec
    expected_pathspec = upstream_run["end"].task.pathspec
    verify_pathspec(expected_pathspec, all_data.trigger.event.id)


def test_trigger_on_finish_dynamic_toplevel_list(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    # Setup
    combined_tags = setup_test_deployment(
        "test_trigger_on_finish_dynamic_toplevel_list",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    config_value, branch = create_branch_config(
        project_branch=f"test.{str(uuid.uuid4())[:8]}"
    )
    config_value["project_branch"] = f"test.{branch}"

    # Deploy upstream flows
    upstream_flow1 = deploy_trigger_flow(
        "triggers/dummy_trigger_flow1.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )
    upstream_flow2 = deploy_trigger_flow(
        "triggers/dummy_trigger_flow2.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Deploy downstream flow
    deploy_trigger_flow(
        "triggers/hello_trigger_on_finish_dynamic_toplevel_list.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Trigger downstream flow by running the upstream ones
    upstream_run1 = wait_for_deployed_run(upstream_flow1)
    upstream_run2 = wait_for_deployed_run(upstream_flow2)

    # Verify
    run = verify_single_run(
        "TriggerOnFinishFlowDynamicTopLevelListFlow", tags=combined_tags
    )
    all_data = run["start"].task.data

    # Verify event names
    expected_triggers_set = {
        "metaflow.DummyTriggerFlow1.end",
        "metaflow.DummyTriggerFlow2.end",
    }
    actual_triggers_set = set([trigger.name for trigger in all_data.trigger.events])
    verify_event_names_set(expected_triggers_set, actual_triggers_set)

    # Verify event task pathspecs
    actual_pathspecs = set([trigger.id for trigger in all_data.trigger.events])
    expected_pathspecs = {
        upstream_run1["end"].task.pathspec,
        upstream_run2["end"].task.pathspec,
    }
    if actual_pathspecs != expected_pathspecs:
        raise RuntimeError(
            f"Expecting event ids to be '{expected_pathspecs}', but got '{actual_pathspecs}'"
        )


def test_trigger_on_finish_dynamic_toplevel_fq_name(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    # Setup
    combined_tags = setup_test_deployment(
        "test_trigger_on_finish_dynamic_toplevel_fq_name",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    flow_name = "DummyTriggerFlow1"
    config_value, branch = create_branch_config(
        project_branch=f"test.{str(uuid.uuid4())[:8]}"
    )
    config_value["project_branch"] = f"test.{branch}"

    # Deploy upstream flow
    upstream_flow = deploy_trigger_flow(
        "triggers/dummy_trigger_flow1.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Deploy downstream flow
    deploy_trigger_flow(
        "triggers/hello_trigger_on_finish_dynamic_toplevel_fq_name.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Trigger downstream flow by running the upstream one
    upstream_run = wait_for_deployed_run(upstream_flow)

    # Verify
    run = verify_single_run(
        "TriggerOnFinishFlowDynamicTopLevelFQNameFlow", tags=combined_tags
    )
    all_data = run["start"].task.data
    expected_trigger = f"metaflow.{flow_name}.end"

    # Verify event name
    verify_event_name(expected_trigger, all_data.trigger.event.name)

    # Verify event task pathspec
    expected_pathspec = upstream_run["end"].task.pathspec
    verify_pathspec(expected_pathspec, all_data.trigger.event.id)


def test_trigger_on_finish_local(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    # Setup
    combined_tags = setup_test_deployment(
        "test_trigger_on_finish_local", exec_mode, decospecs, tag, scheduler_config
    )
    flow_name = "DummyTriggerFlow1"
    config_value, branch = create_branch_config()

    # Deploy upstream flow
    upstream_flow = deploy_trigger_flow(
        "triggers/dummy_trigger_flow1.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Trigger upstream flow and wait for completion
    upstream_run = wait_for_deployed_run(upstream_flow)

    # Run downstream flow directly with trigger parameter
    downstream_run = run_flow_with_env(
        flow_name="triggers/hello_trigger_on_finish_local.py",
        decospecs=decospecs,
        config_value=[("config", config_value)],
        trigger=[f"{flow_name}/{upstream_run.id}"],
        runner_args={
            "tags": combined_tags,
        },
        env=compute_env,
    )

    # Verify
    all_data = downstream_run["start"].task.data
    expected_var = upstream_run["start"].task.data.my_var
    if all_data.fake_flow_var != expected_var:
        raise RuntimeError(
            f"Expected fake_flow_var to be {expected_var}, but got {all_data.fake_flow_var}"
        )


def test_trigger_on_finish_static_project(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    # Setup
    combined_tags = setup_test_deployment(
        "test_trigger_on_finish_static_project",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    flow_name = "DummyTriggerFlow1"
    config_value, branch = create_branch_config(event_name=flow_name)

    # Deploy upstream flow
    upstream_flow = deploy_trigger_flow(
        "triggers/dummy_trigger_flow1.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Deploy downstream flow
    deploy_trigger_flow(
        "triggers/hello_trigger_on_finish_static_project.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Trigger downstream flow by running the upstream one
    upstream_run = wait_for_deployed_run(upstream_flow)

    # Verify
    run = verify_single_run("TriggerOnFinishStaticProjectFlow", tags=combined_tags)
    all_data = run["start"].task.data
    expected_trigger = f"metaflow.{flow_name}.end"

    # Verify event name
    verify_event_name(expected_trigger, all_data.trigger.event.name)

    # Verify event task pathspec
    expected_pathspec = upstream_run["end"].task.pathspec
    verify_pathspec(expected_pathspec, all_data.trigger.event.id)


def test_trigger_on_finish_static_fq_name(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    # Setup
    combined_tags = setup_test_deployment(
        "test_trigger_on_finish_static_fq_name",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    flow_name = "DummyTriggerFlow1"
    config_value, branch = create_branch_config()
    config_value["fq_event_name"] = (
        f"{config_value['project']}.test.{branch}.{flow_name}"
    )

    # Deploy upstream flow
    upstream_flow = deploy_trigger_flow(
        "triggers/dummy_trigger_flow1.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Deploy downstream flow
    deploy_trigger_flow(
        "triggers/hello_trigger_on_finish_static_fq_name.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Trigger downstream flow by running the upstream one
    upstream_run = wait_for_deployed_run(upstream_flow)

    # Verify
    run = verify_single_run("TriggerOnFinishStaticFQNameFlow", tags=combined_tags)
    all_data = run["start"].task.data
    expected_trigger = f"metaflow.{flow_name}.end"

    # Verify event name
    verify_event_name(expected_trigger, all_data.trigger.event.name)

    # Verify event task pathspec
    expected_pathspec = upstream_run["end"].task.pathspec
    verify_pathspec(expected_pathspec, all_data.trigger.event.id)


def test_trigger_on_finish_dynamic_toplevel_dict(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    # Setup
    combined_tags = setup_test_deployment(
        "test_trigger_on_finish_dynamic_toplevel_dict",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    flow_name = "DummyTriggerFlow1"
    config_value, branch = create_branch_config(
        project_branch=f"test.{str(uuid.uuid4())[:8]}"
    )
    config_value["project_branch"] = f"test.{branch}"

    # Deploy upstream flow
    upstream_flow = deploy_trigger_flow(
        "triggers/dummy_trigger_flow1.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Deploy downstream flow
    deploy_trigger_flow(
        "triggers/hello_trigger_on_finish_dynamic_toplevel_dict.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Trigger downstream flow by running the upstream one
    upstream_run = wait_for_deployed_run(upstream_flow)

    # Verify
    run = verify_single_run(
        "TriggerOnFinishFlowDynamicTopLevelDictFlow", tags=combined_tags
    )
    all_data = run["start"].task.data
    expected_trigger = f"metaflow.{flow_name}.end"

    # Verify event name
    verify_event_name(expected_trigger, all_data.trigger.event.name)

    # Verify event task pathspec
    expected_pathspec = upstream_run["end"].task.pathspec
    verify_pathspec(expected_pathspec, all_data.trigger.event.id)
