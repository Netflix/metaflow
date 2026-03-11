"""
Trigger tests — verifies @trigger and @trigger_on_finish across all orchestrators.

These tests deploy trigger-enabled flows and send events via the orchestrator's
native eventing mechanism. The actual event-sending is delegated to
`send_event()` in test_utils.py, which each orchestrator branch implements.

If send_event() raises NotImplementedError for a given scheduler_type, the
test is skipped — this is expected on the shared branch where no orchestrator-
specific implementation exists yet.

Run with:
    pytest test/ux/core/test_triggers.py -m triggers -v
"""

import pytest

pytestmark = [pytest.mark.triggers, pytest.mark.scheduler_only]

from .test_utils import (
    deploy_flow_to_scheduler,
    send_event,
    wait_for_deployed_run,
)


@pytest.mark.triggers
@pytest.mark.scheduler_only
def test_static_trigger(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Deploy flow with @trigger, send event, verify parameter received."""
    if exec_mode != "deployer":
        pytest.skip("trigger tests require deployer mode")

    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("trigger tests require a scheduler")

    test_unique_tag = f"test_static_trigger_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {"decospecs": decospecs, "env": compute_env}

    try:
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="triggers/hello_static_trigger.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_type,
        )
    except RuntimeError as e:
        if "not supported" in str(e).lower():
            pytest.skip(f"@trigger not supported by {scheduler_type}: {e}")
        raise

    try:
        send_event(
            scheduler_type=scheduler_type,
            event_name="test_event",
            payload={"event_alpha": "hello_from_test"},
            scheduler_config=scheduler_config,
        )
    except NotImplementedError:
        pytest.skip(f"send_event not implemented for {scheduler_type}")

    run = wait_for_deployed_run(deployed_flow, timeout=600)
    assert run.successful, "Triggered run was not successful"
    assert (
        run["start"].task.data.received_alpha == "hello_from_test"
    ), "Event parameter 'alpha' was not mapped correctly from event payload"


@pytest.mark.triggers
@pytest.mark.scheduler_only
def test_deploy_time_trigger(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Deploy flow with deploy-time trigger (callable event spec)."""
    if exec_mode != "deployer":
        pytest.skip("trigger tests require deployer mode")

    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("trigger tests require a scheduler")

    test_unique_tag = f"test_deploy_time_trigger_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {"decospecs": decospecs, "env": compute_env}

    # Deploy-time triggers resolve the callable at create() time.
    # If the orchestrator doesn't support deploy-time triggers, create() will fail.
    try:
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="triggers/hello_deploy_time_trigger.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_type,
        )
    except RuntimeError as e:
        if "not supported" in str(e).lower():
            pytest.skip(f"@trigger not supported by {scheduler_type}: {e}")
        raise

    try:
        send_event(
            scheduler_type=scheduler_type,
            event_name="deploy_time_event",
            payload={"event_beta": "deploy_time_value"},
            scheduler_config=scheduler_config,
        )
    except NotImplementedError:
        pytest.skip(f"send_event not implemented for {scheduler_type}")

    run = wait_for_deployed_run(deployed_flow, timeout=600)
    assert run.successful, "Deploy-time triggered run was not successful"
    assert (
        run["start"].task.data.received_beta == "deploy_time_value"
    ), "Deploy-time trigger parameter was not mapped correctly"


@pytest.mark.triggers
@pytest.mark.scheduler_only
def test_trigger_on_finish(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Deploy upstream + downstream flows, run upstream, verify downstream triggers."""
    if exec_mode != "deployer":
        pytest.skip("trigger tests require deployer mode")

    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("trigger tests require a scheduler")

    test_unique_tag = f"test_trigger_on_finish_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {"decospecs": decospecs, "env": compute_env}

    # Deploy the downstream flow first (it listens for upstream completion)
    try:
        downstream = deploy_flow_to_scheduler(
            flow_name="triggers/hello_trigger_on_finish.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_type,
        )
    except RuntimeError as e:
        if "not supported" in str(e).lower():
            pytest.skip(f"@trigger_on_finish not supported by {scheduler_type}: {e}")
        raise

    # Deploy and trigger the upstream flow
    upstream = deploy_flow_to_scheduler(
        flow_name="triggers/dummy_trigger_flow1.py",
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_type,
    )

    upstream_run = wait_for_deployed_run(upstream, timeout=600)
    assert upstream_run.successful, "Upstream flow run was not successful"

    # Wait for the downstream flow to be triggered by upstream completion
    downstream_run = wait_for_deployed_run(downstream, timeout=600)
    assert downstream_run.successful, "Downstream triggered run was not successful"
    assert downstream_run["start"].task.data.message == "triggered by upstream"
