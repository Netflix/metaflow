import json
import pytest
from unittest.mock import patch, MagicMock


# -- Fixtures: mock state machine definitions ----------------------------------

def _make_state_machine_definition(flow_name, owner, production_token):
    """Build a minimal SFN definition that mirrors what Metaflow embeds."""
    return json.dumps(
        {
            "States": {
                "start": {
                    "Parameters": {
                        "Parameters": {
                            "metaflow.flow_name": flow_name,
                            "metaflow.owner": owner,
                            "metaflow.production_token": production_token,
                        },
                        "ContainerOverrides": {
                            "Environment": [
                                {"Name": "METAFLOW_FLOW_NAME", "Value": flow_name},
                                {"Name": "METAFLOW_OWNER", "Value": owner},
                            ]
                        },
                    }
                }
            }
        }
    )


def _make_describe_response(name, flow_name, owner, token):
    return {
        "stateMachineArn": f"arn:aws:states:us-east-1:123456789:stateMachine:{name}",
        "name": name,
        "definition": _make_state_machine_definition(flow_name, owner, token),
        "status": "ACTIVE",
    }


NON_METAFLOW_DEFINITION = json.dumps({"States": {"Init": {"Type": "Pass"}}})


# ==============================================================================
# Tests for StepFunctionsClient.list_state_machines
# ==============================================================================

class TestStepFunctionsClientListStateMachines:

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_client.get_aws_client",
        create=True,
    )
    def test_list_state_machines_yields_names(self, _):
        from metaflow.plugins.aws.step_functions.step_functions_client import (
            StepFunctionsClient,
        )

        client = StepFunctionsClient.__new__(StepFunctionsClient)
        mock_boto = MagicMock()
        client._client = mock_boto

        mock_paginator = MagicMock()
        mock_boto.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "stateMachines": [
                    {"name": "sm-one", "stateMachineArn": "arn:one"},
                    {"name": "sm-two", "stateMachineArn": "arn:two"},
                ]
            },
            {
                "stateMachines": [
                    {"name": "sm-three", "stateMachineArn": "arn:three"},
                ]
            },
        ]

        names = list(client.list_state_machines())
        assert names == ["sm-one", "sm-two", "sm-three"]
        mock_boto.get_paginator.assert_called_once_with("list_state_machines")

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_client.get_aws_client",
        create=True,
    )
    def test_list_state_machines_empty(self, _):
        from metaflow.plugins.aws.step_functions.step_functions_client import (
            StepFunctionsClient,
        )

        client = StepFunctionsClient.__new__(StepFunctionsClient)
        mock_boto = MagicMock()
        client._client = mock_boto

        mock_paginator = MagicMock()
        mock_boto.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"stateMachines": []}]

        names = list(client.list_state_machines())
        assert names == []

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_client.AWS_SANDBOX_ENABLED",
        True,
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_client.get_aws_client",
        create=True,
    )
    def test_list_state_machines_raises_in_sandbox(self, _):
        from metaflow.exception import MetaflowException
        from metaflow.plugins.aws.step_functions.step_functions_client import (
            StepFunctionsClient,
        )

        client = StepFunctionsClient.__new__(StepFunctionsClient)
        client._client = MagicMock()

        with pytest.raises(MetaflowException, match="sandbox"):
            list(client.list_state_machines())


# ==============================================================================
# Tests for StepFunctions.get_deployment_metadata
# ==============================================================================

class TestGetDeploymentMetadata:

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_returns_metadata_for_metaflow_sm(self, MockClient):
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.get.return_value = _make_describe_response(
            "my-flow", "MyFlow", "testuser", "tok-abc123"
        )

        result = StepFunctions.get_deployment_metadata("my-flow")
        assert result == ("MyFlow", "testuser", "tok-abc123")
        mock_instance.get.assert_called_once_with("my-flow")

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_returns_none_for_nonexistent_sm(self, MockClient):
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.get.return_value = None

        result = StepFunctions.get_deployment_metadata("no-such-sm")
        assert result is None

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_returns_none_for_non_metaflow_sm(self, MockClient):
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.get.return_value = {
            "stateMachineArn": "arn:aws:states:us-east-1:123:stateMachine:other",
            "name": "other",
            "definition": NON_METAFLOW_DEFINITION,
        }

        result = StepFunctions.get_deployment_metadata("other")
        assert result is None


# ==============================================================================
# Tests for StepFunctions.list_templates
# ==============================================================================

class TestListTemplates:

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_list_all_templates(self, MockClient):
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value

        # list_state_machines returns three names
        mock_paginator = MagicMock()
        mock_instance._client = MagicMock()
        mock_instance.list_state_machines.return_value = iter(
            ["flow-a", "flow-b", "non-metaflow"]
        )

        # get() returns metaflow definitions for the first two, non-metaflow for the third
        def fake_get(name):
            if name == "flow-a":
                return _make_describe_response("flow-a", "FlowA", "user1", "tok1")
            elif name == "flow-b":
                return _make_describe_response("flow-b", "FlowB", "user2", "tok2")
            else:
                return {
                    "definition": NON_METAFLOW_DEFINITION,
                    "name": name,
                    "stateMachineArn": f"arn:{name}",
                }

        mock_instance.get.side_effect = fake_get

        names = list(StepFunctions.list_templates())
        assert names == ["flow-a", "flow-b"]

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_list_templates_filtered_by_flow_name(self, MockClient):
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.list_state_machines.return_value = iter(
            ["flow-a", "flow-b"]
        )

        def fake_get(name):
            if name == "flow-a":
                return _make_describe_response("flow-a", "FlowA", "user1", "tok1")
            elif name == "flow-b":
                return _make_describe_response("flow-b", "FlowB", "user2", "tok2")
            return None

        mock_instance.get.side_effect = fake_get

        names = list(StepFunctions.list_templates(flow_name="FlowA"))
        assert names == ["flow-a"]

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_list_templates_reuses_single_client(self, MockClient):
        """StepFunctionsClient must be instantiated exactly once per list_templates call."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.list_state_machines.return_value = iter(
            ["flow-a", "flow-b", "flow-c"]
        )

        def fake_get(name):
            return _make_describe_response(name, name.replace("-", ""), "user", "tok")

        mock_instance.get.side_effect = fake_get

        list(StepFunctions.list_templates())
        MockClient.assert_called_once()


# ==============================================================================
# Tests for StepFunctionsDeployedFlow.from_deployment
# ==============================================================================

class TestFromDeployment:

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.get_metadata"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_from_deployment_success(self, MockSF, mock_get_metadata, MockDeployer):
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.get_deployment_metadata.return_value = (
            "MyFlow",
            "testuser",
            "tok-abc",
        )
        mock_get_metadata.return_value = "local@latest"

        # Mock the Deployer chain: Deployer(file).step_functions(name=...)
        mock_deployer_instance = MagicMock()
        mock_sfn_deployer = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = mock_sfn_deployer

        result = StepFunctionsDeployedFlow.from_deployment("my-state-machine")

        assert isinstance(result, StepFunctionsDeployedFlow)

        # Verify deployer attributes were set
        assert mock_sfn_deployer.name == "my-state-machine"
        assert mock_sfn_deployer.flow_name == "MyFlow"
        assert mock_sfn_deployer.metadata == "local@latest"

        # Verify Deployer was called with METAFLOW_USER env
        MockDeployer.assert_called_once()
        call_kwargs = MockDeployer.call_args
        assert call_kwargs[1]["env"] == {"METAFLOW_USER": "testuser"}

        # Verify step_functions was called with the identifier as name
        mock_deployer_instance.step_functions.assert_called_once_with(
            name="my-state-machine"
        )

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.get_metadata"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_from_deployment_with_explicit_metadata(
        self, MockSF, mock_get_metadata, MockDeployer
    ):
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.get_deployment_metadata.return_value = (
            "MyFlow",
            "testuser",
            "tok-abc",
        )

        mock_deployer_instance = MagicMock()
        mock_sfn_deployer = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = mock_sfn_deployer

        result = StepFunctionsDeployedFlow.from_deployment(
            "my-sm", metadata="service@https://metadata.example.com"
        )

        assert mock_sfn_deployer.metadata == "service@https://metadata.example.com"
        # get_metadata should NOT have been called since we provided metadata
        mock_get_metadata.assert_not_called()

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_from_deployment_not_found_raises(self, MockSF):
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )
        from metaflow.exception import MetaflowException

        MockSF.get_deployment_metadata.return_value = None

        with pytest.raises(MetaflowException, match="No deployed flow found for"):
            StepFunctionsDeployedFlow.from_deployment("nonexistent-sm")


# ==============================================================================
# Tests for StepFunctionsDeployedFlow.get_triggered_run
# ==============================================================================

class TestGetTriggeredRun:

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.get_metadata"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_get_triggered_run_success(self, MockSF, mock_get_metadata, MockDeployer):
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
            StepFunctionsTriggeredRun,
        )

        MockSF.get_deployment_metadata.return_value = (
            "MyFlow",
            "testuser",
            "tok-abc",
        )
        mock_get_metadata.return_value = "local@latest"

        mock_deployer_instance = MagicMock()
        mock_sfn_deployer = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = mock_sfn_deployer
        # Set flow_name so pathspec can be built
        mock_sfn_deployer.flow_name = "MyFlow"
        mock_sfn_deployer.metadata = "local@latest"

        result = StepFunctionsDeployedFlow.get_triggered_run(
            "my-sm", "sfn-abc123", metadata="local@latest"
        )

        assert isinstance(result, StepFunctionsTriggeredRun)
        assert result.pathspec == "MyFlow/sfn-abc123"
        assert result.name == "sfn-abc123"


# ==============================================================================
# Tests for StepFunctionsDeployedFlow.list_deployed_flows
# ==============================================================================

class TestListDeployedFlows:

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.get_metadata"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_list_deployed_flows_all(self, MockSF, mock_get_metadata, MockDeployer):
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.list_templates.return_value = iter(["sm-a", "sm-b"])

        # get_deployment_metadata is called by from_deployment
        def fake_metadata(name):
            return {
                "sm-a": ("FlowA", "user1", "tok1"),
                "sm-b": ("FlowB", "user2", "tok2"),
            }.get(name)

        MockSF.get_deployment_metadata.side_effect = fake_metadata
        mock_get_metadata.return_value = "local@latest"

        mock_deployer_instance = MagicMock()
        mock_sfn_deployer = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = mock_sfn_deployer

        flows = list(StepFunctionsDeployedFlow.list_deployed_flows())

        assert len(flows) == 2
        MockSF.list_templates.assert_called_once_with(flow_name=None)

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.get_metadata"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_list_deployed_flows_skips_failures(
        self, MockSF, mock_get_metadata, MockDeployer
    ):
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.list_templates.return_value = iter(["sm-good", "sm-bad"])

        def fake_metadata(name):
            if name == "sm-good":
                return ("FlowGood", "user1", "tok1")
            # sm-bad returns None which will cause from_deployment to raise
            return None

        MockSF.get_deployment_metadata.side_effect = fake_metadata
        mock_get_metadata.return_value = "local@latest"

        mock_deployer_instance = MagicMock()
        mock_sfn_deployer = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = mock_sfn_deployer

        flows = list(StepFunctionsDeployedFlow.list_deployed_flows())

        # Only the good one should come through; the bad one is skipped
        assert len(flows) == 1
