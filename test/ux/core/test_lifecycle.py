"""
Deployer lifecycle tests — deploy, trigger, status, undeploy.

Verifies the full lifecycle of deployed flows across all scheduler backends.
These are deployer-only tests (no runner mode).

Run with:
    pytest test/ux/core/test_lifecycle.py -m lifecycle -v
"""

import pytest

pytestmark = [pytest.mark.lifecycle, pytest.mark.scheduler_only]

from .test_utils import deploy_flow_to_scheduler, wait_for_deployed_run


@pytest.mark.lifecycle
@pytest.mark.scheduler_only
def test_schedule_deploy(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Deploy a @schedule flow, verify deployment succeeds."""
    if exec_mode != "deployer":
        pytest.skip("lifecycle tests require deployer mode")

    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("lifecycle tests require a scheduler")

    test_unique_tag = f"test_schedule_deploy_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {"decospecs": decospecs, "env": compute_env}

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="lifecycle/schedule_flow.py",
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_type,
    )

    assert deployed_flow is not None, "Deployment returned None"
    assert deployed_flow.name, "Deployed flow has no name"


@pytest.mark.lifecycle
@pytest.mark.scheduler_only
def test_deployed_flow_status(exec_mode, decospecs, compute_env, tag, scheduler_config):
    """Deploy, trigger, verify status, then check run completed."""
    if exec_mode != "deployer":
        pytest.skip("lifecycle tests require deployer mode")

    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("lifecycle tests require a scheduler")

    test_unique_tag = f"test_deployed_status_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {"decospecs": decospecs, "env": compute_env}

    # Use a simple flow for lifecycle testing
    deployed_flow = deploy_flow_to_scheduler(
        flow_name="basic/helloworld.py",
        tl_args=tl_args,
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
        scheduler_type=scheduler_type,
    )

    run = wait_for_deployed_run(deployed_flow)
    assert run.successful, "Run was not successful"
    assert run.finished, "Run did not finish"
