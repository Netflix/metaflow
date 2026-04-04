import pytest
from metaflow import Deployer
from .payloads import PAYLOADS
from ..utils import (
    wait_for_result,
    wait_for_run,
    wait_for_run_to_finish,
    wait_for_runs_after_ts,
)
import os

ROOTPATH = os.path.dirname(__file__)


@pytest.fixture
def test_tags(test_id):
    return ["argo_workflows_tests", "parameters_tests", test_id]


def test_events(test_tags, test_id):
    try:
        deployed_event_flow = (
            Deployer(flow_file=os.path.join(ROOTPATH, "eventflow.py"))
            .argo_workflows()
            .create(tags=test_tags)
        )
        deployed_trigger_flow = (
            Deployer(flow_file=os.path.join(ROOTPATH, "triggering_flow.py"))
            .argo_workflows()
            .create(tags=test_tags)
        )

        # run the flow that sends an event.
        triggered_run = deployed_trigger_flow.trigger()

        run = wait_for_result(triggered_run, timeout=240)
        assert run.successful

        # Await the event triggered flows.
        current_ts = (
            run.created_at
        )  # rough estimate on when event triggered flows should start kicking off.

        runs = wait_for_runs_after_ts(
            deployed_event_flow.flow_name,
            ns=test_id,
            after_ts=current_ts,
            expected_runs=len(PAYLOADS),
            timeout=600,
        )

        finished_runs = [wait_for_run_to_finish(run, timeout=1200) for run in runs]

        for run in finished_runs:
            assert run.successful
    finally:
        if deployed_trigger_flow is not None:
            deployed_trigger_flow.delete()
        if deployed_event_flow is not None:
            deployed_event_flow.delete()


def test_cron(test_tags, test_id):
    try:
        deployed_cron_flow = (
            Deployer(flow_file=os.path.join(ROOTPATH, "cronflow.py"))
            .argo_workflows()
            .create(tags=test_tags)
        )

        run = wait_for_run(
            flow_name=deployed_cron_flow.flow_name,
            ns=test_id,
            timeout=240 + 10,  # 4 minutes for the test cron flow.
        )
        run = wait_for_run_to_finish(run)
        assert run.successful
    finally:
        deployed_cron_flow.delete()


def test_base_params(test_tags):
    try:
        deployed_flow = (
            Deployer(flow_file=os.path.join(ROOTPATH, "paramflow.py"))
            .argo_workflows()
            .create(tags=test_tags)
        )
        triggered_run = deployed_flow.trigger()

        run = wait_for_result(triggered_run)
        assert run.successful

        # Also test supplying json-string parameter value through CLI
        # This should _not_ work!
        triggered_run = deployed_flow.trigger(param_b='["a"]', param_c='{"b": 123}')

        raised = True
        try:
            # This will raise errors from the flow run if any.
            fail_run = wait_for_result(triggered_run, timeout=180)
            raised = False
        except Exception as ex:
            # We expect an error to be raised.
            assert "is of the wrong type" in str(ex)

        if not raised:
            raise Exception(
                "Run was supposed to fail when passing jsonstring parameters through deployer, as they are unsupported."
            )

    finally:
        deployed_flow.delete()
