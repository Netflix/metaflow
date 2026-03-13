import uuid
import pytest

pytestmark = [pytest.mark.triggers, pytest.mark.scheduler_only]
from .test_utils import (
    deploy_flow_to_scheduler,
    disp_test,
    send_event,
    wait_for_deployed_run,
    verify_single_run,
    run_flow_with_env,
)


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


def _require_event_provider(scheduler_config):
    """Skip test if no event provider is configured for the current backend."""
    from metaflow.plugins import EVENT_PROVIDERS

    for provider_class in EVENT_PROVIDERS:
        if hasattr(provider_class, "is_configured") and provider_class.is_configured():
            return  # At least one provider is ready
    pytest.skip(
        "No event provider is configured for the current environment "
        "(need ARGO_EVENTS_WEBHOOK_URL, METAFLOW_SFN_IAM_ROLE, or "
        "METAFLOW_AIRFLOW_WEBSERVER_URL)"
    )


def send_trigger_signals(signals_data):
    """Send multiple trigger signals via the plugin-based send_event().

    signals_data: list of tuples (signal_name, params_dict)
    """
    for signal_name, params in signals_data:
        send_event(signal_name, payload=params)


def generate_test_params(*param_names):
    """Generate UUID-based test parameters."""
    return {name: str(uuid.uuid4())[:8] for name in param_names}


def verify_params_batch(all_data, expected_params):
    """Verify multiple parameters in batch.

    expected_params: dict of {param_name: expected_value}
    """
    for param_name, expected_value in expected_params.items():
        actual_value = getattr(all_data, param_name)
        verify_param(expected_value, actual_value, param_name)


# ---------------------------------------------------------------------------
# Phase 2: @trigger tests (backend-agnostic via send_event plugin system)
# ---------------------------------------------------------------------------


def test_static_triggers(exec_mode, decospecs, compute_env, tag, scheduler_config):
    _require_event_provider(scheduler_config)

    # Setup
    combined_tags = setup_test_deployment(
        "test_static_triggers", exec_mode, decospecs, tag, scheduler_config
    )
    config_value, branch = create_branch_config("static_triggers")

    # Deploy
    deploy_trigger_flow(
        "triggers/hello_static_trigger.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Generate test data
    params = generate_test_params("param1", "param2", "param3")

    # Send signals
    signals = [
        (f"{branch}.HelloStaticTriggerName", {}),
        (f"{branch}.HelloStaticTriggerName1", {"param1": params["param1"]}),
        (
            f"{branch}.HelloStaticTriggerName2",
            {"param2_events_field": params["param2"]},
        ),
        (
            f"{branch}.HelloStaticTriggerName3",
            {"param3_events_field": params["param3"]},
        ),
    ]
    send_trigger_signals(signals)

    # Verify
    run = verify_single_run("HelloStaticTriggerFlow", tags=combined_tags)
    all_data = run["start"].task.data

    # Verify event names
    expected_triggers_set = {signal[0] for signal in signals}
    actual_triggers_set = set([trigger.name for trigger in all_data.trigger.events])
    verify_event_names_set(expected_triggers_set, actual_triggers_set)

    # Verify params
    verify_params_batch(all_data, params)


def test_deploy_time_trigger_toplevel_list(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    _require_event_provider(scheduler_config)

    # Setup
    combined_tags = setup_test_deployment(
        "test_deploy_time_trigger_toplevel_list",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    config_value, branch = create_branch_config()

    # Deploy
    deploy_trigger_flow(
        "triggers/hello_deploy_time_trigger_toplevel_list.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Generate test data
    params = generate_test_params("param1", "param2")

    # Send signals
    signals = [
        (
            f"test.{branch}.HelloDeployTimeTriggerTopLevelListFlow1",
            {"param1": params["param1"]},
        ),
        (
            f"test.{branch}.HelloDeployTimeTriggerTopLevelListFlow2",
            {"param2": params["param2"]},
        ),
    ]
    send_trigger_signals(signals)

    # Verify
    run = verify_single_run(
        "HelloDeployTimeTriggerTopLevelListFlow", tags=combined_tags
    )
    all_data = run["start"].task.data

    # Verify event names
    expected_triggers_set = {signal[0] for signal in signals}
    actual_triggers_set = set([trigger.name for trigger in all_data.trigger.events])
    verify_event_names_set(expected_triggers_set, actual_triggers_set)

    # Verify params
    verify_params_batch(all_data, params)


def test_deploy_time_trigger_toplevel_name(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    _require_event_provider(scheduler_config)

    # Setup
    combined_tags = setup_test_deployment(
        "test_deploy_time_trigger_toplevel_name",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    branch = tag[0]
    config_value = {"branch": branch}

    # Deploy
    deploy_trigger_flow(
        "triggers/hello_deploy_time_trigger_toplevel_name.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
        branch=branch,
    )

    # Generate test data
    params = generate_test_params("param1")

    # Send signals
    signals = [
        (
            f"{branch}.HelloDeployTimeTriggerTopLevelNameFlow",
            {"param1": params["param1"]},
        ),
    ]
    send_trigger_signals(signals)

    # Verify
    run = verify_single_run(
        "HelloDeployTimeTriggerTopLevelNameFlow", tags=combined_tags
    )
    all_data = run["start"].task.data
    expected_trigger = f"{branch}.HelloDeployTimeTriggerTopLevelNameFlow"

    # Verify event name
    verify_event_name(expected_trigger, all_data.trigger.event.name)

    # Verify params (note: checking for default_value as per original test)
    verify_param("default_value", all_data.param1, "param1")


def test_deploy_time_trigger_events_nested(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    _require_event_provider(scheduler_config)

    # Setup
    combined_tags = setup_test_deployment(
        "test_deploy_time_trigger_events_nested",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    config_value, branch = create_branch_config()

    # Deploy
    deploy_trigger_flow(
        "triggers/hello_deploy_time_trigger_events_nested.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Generate test data
    params = generate_test_params("param1", "param2", "param3", "param4")

    # Send signals
    signals = [
        (f"{branch}.HelloDeployTimeTriggerEventsNestedFlow", {}),
        (
            f"test.{branch}.HelloDeployTimeTriggerEventsNestedFlow1",
            {"param1": params["param1"]},
        ),
        (
            f"{branch}.HelloDeployTimeTriggerEventsNestedFlow2",
            {"param2": params["param2"]},
        ),
        (
            f"{branch}.HelloDeployTimeTriggerEventsNestedFlow3",
            {"param3": params["param3"]},
        ),
        (
            f"{branch}.HelloDeployTimeTriggerEventsNestedFlow4",
            {"param_from_trigger": params["param4"]},
        ),
    ]
    send_trigger_signals(signals)

    # Verify
    run = verify_single_run(
        "HelloDeployTimeTriggerEventsNestedFlow", tags=combined_tags
    )
    all_data = run["start"].task.data

    # Verify event names
    expected_triggers_set = {signal[0] for signal in signals}
    actual_triggers_set = set([trigger.name for trigger in all_data.trigger.events])
    verify_event_names_set(expected_triggers_set, actual_triggers_set)

    # Verify params
    verify_params_batch(all_data, params)


def test_deploy_time_trigger_event_nested(
    exec_mode, decospecs, compute_env, tag, scheduler_config
):
    _require_event_provider(scheduler_config)

    # Setup
    combined_tags = setup_test_deployment(
        "test_deploy_time_trigger_event_nested",
        exec_mode,
        decospecs,
        tag,
        scheduler_config,
    )
    config_value, branch = create_branch_config()

    # Deploy
    deploy_trigger_flow(
        "triggers/hello_deploy_time_trigger_event_nested.py",
        config_value,
        combined_tags,
        scheduler_config,
        decospecs,
        compute_env,
    )

    # Generate test data
    param1 = str(uuid.uuid4())[:8]
    param2 = str(uuid.uuid4())[:8]

    # Send signals
    signals = [
        (
            f"test.{branch}.HelloDeployTimeTriggerEventNestedFlow",
            {
                "param": param1,
                "events_field": param2,
            },
        ),
    ]
    send_trigger_signals(signals)

    # Verify
    run = verify_single_run("HelloDeployTimeTriggerEventNestedFlow", tags=combined_tags)
    all_data = run["start"].task.data
    expected_trigger = f"test.{branch}.HelloDeployTimeTriggerEventNestedFlow"

    # Verify event name
    verify_event_name(expected_trigger, all_data.trigger.event.name)

    # Verify params
    verify_param(param1, all_data.param, "param")
    verify_param(param2, all_data.param_field, "param_field")
