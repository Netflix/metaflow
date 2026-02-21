import json
import tempfile
import pytest
from unittest.mock import patch, MagicMock, call


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

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_returns_none_for_empty_string_flow_name(self, MockClient):
        """flow_name="" in params dict is treated the same as missing."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        definition = json.dumps({
            "States": {"start": {"Parameters": {"Parameters": {
                "metaflow.flow_name": "",
                "metaflow.owner": "user",
                "metaflow.production_token": "tok",
            }}}}
        })
        MockClient.return_value.get.return_value = {
            "name": "sm", "stateMachineArn": "arn:sm", "definition": definition
        }

        assert StepFunctions.get_deployment_metadata("sm") is None

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_returns_none_for_explicitly_null_flow_name(self, MockClient):
        """flow_name=null in JSON params (parsed as None) is treated the same as missing."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        definition = json.dumps({
            "States": {"start": {"Parameters": {"Parameters": {
                "metaflow.flow_name": None,
                "metaflow.owner": "user",
            }}}}
        })
        MockClient.return_value.get.return_value = {
            "name": "sm", "stateMachineArn": "arn:sm", "definition": definition
        }

        assert StepFunctions.get_deployment_metadata("sm") is None

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_returns_none_for_malformed_json_definition(self, MockClient):
        """Malformed JSON in the definition field is handled gracefully."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        MockClient.return_value.get.return_value = {
            "name": "sm", "stateMachineArn": "arn:sm",
            "definition": "{ this is not valid json !!",
        }

        assert StepFunctions.get_deployment_metadata("sm") is None

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_returns_none_for_missing_nested_keys(self, MockClient):
        """JSON that lacks any part of States.start.Parameters.Parameters returns None."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        for broken_def in [
            json.dumps({}),                                        # no States
            json.dumps({"States": {}}),                            # no start
            json.dumps({"States": {"start": {}}}),                 # no Parameters
            json.dumps({"States": {"start": {"Parameters": {}}}}), # no inner Parameters
        ]:
            MockClient.return_value.get.return_value = {
                "name": "sm", "stateMachineArn": "arn:sm", "definition": broken_def
            }
            assert StepFunctions.get_deployment_metadata("sm") is None, \
                f"Expected None for definition: {broken_def}"

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_uses_provided_client_not_new_one(self, MockClass):
        """When _client is provided, no new StepFunctionsClient is instantiated."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        provided_client = MagicMock()
        provided_client.get.return_value = _make_describe_response(
            "sm", "MyFlow", "user", "tok"
        )

        result = StepFunctions.get_deployment_metadata("sm", _client=provided_client)

        assert result == ("MyFlow", "user", "tok")
        provided_client.get.assert_called_once_with("sm")
        MockClass.assert_not_called()  # no new client was created

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_owner_and_token_can_be_none(self, MockClient):
        """Missing owner/production_token are allowed — only flow_name is required."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        definition = json.dumps({
            "States": {"start": {"Parameters": {"Parameters": {
                "metaflow.flow_name": "MyFlow",
                # owner and production_token intentionally absent
            }}}}
        })
        MockClient.return_value.get.return_value = {
            "name": "sm", "stateMachineArn": "arn:sm", "definition": definition
        }

        result = StepFunctions.get_deployment_metadata("sm")
        assert result == ("MyFlow", None, None)


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

        results = list(StepFunctions.list_templates())
        assert [(name, meta[0]) for name, meta in results] == [
            ("flow-a", "FlowA"),
            ("flow-b", "FlowB"),
        ]

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

        results = list(StepFunctions.list_templates(flow_name="FlowA"))
        assert [name for name, _ in results] == ["flow-a"]

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

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_list_templates_excludes_sm_with_missing_flow_name(self, MockClient):
        """State machines with matching nested structure but no metaflow.flow_name are excluded."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions
        import json

        mock_instance = MockClient.return_value
        mock_instance.list_state_machines.return_value = iter(["edge-case-sm"])

        # SM has the right nested structure but metaflow.flow_name is absent
        missing_flow_name_def = json.dumps({
            "States": {
                "start": {
                    "Parameters": {
                        "Parameters": {
                            "metaflow.owner": "someone",
                            "metaflow.production_token": "tok",
                        }
                    }
                }
            }
        })
        mock_instance.get.return_value = {
            "stateMachineArn": "arn:edge",
            "name": "edge-case-sm",
            "definition": missing_flow_name_def,
        }

        results = list(StepFunctions.list_templates())
        assert results == []

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_yields_empty_when_no_state_machines(self, MockClient):
        """Empty account — list_state_machines yields nothing."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        MockClient.return_value.list_state_machines.return_value = iter([])

        assert list(StepFunctions.list_templates()) == []

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_yields_empty_when_flow_name_filter_has_no_matches(self, MockClient):
        """Filter by a flow name that doesn't exist among deployed SMs."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.list_state_machines.return_value = iter(["sm-a"])
        mock_instance.get.return_value = _make_describe_response(
            "sm-a", "FlowA", "user", "tok"
        )

        results = list(StepFunctions.list_templates(flow_name="DoesNotExist"))
        assert results == []

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_flow_name_filter_is_exact_not_partial(self, MockClient):
        """'MyFlow' filter must not match 'MyFlowV2' (exact equality, not substring)."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.list_state_machines.return_value = iter(["sm-v2"])
        mock_instance.get.return_value = _make_describe_response(
            "sm-v2", "MyFlowV2", "user", "tok"
        )

        results = list(StepFunctions.list_templates(flow_name="MyFlow"))
        assert results == []

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_yields_full_metadata_tuple_values(self, MockClient):
        """Verify all three fields of the yielded metadata tuple are correct."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.list_state_machines.return_value = iter(["my-sm"])
        mock_instance.get.return_value = _make_describe_response(
            "my-sm", "TargetFlow", "alice", "prod-token-xyz"
        )

        results = list(StepFunctions.list_templates())
        assert len(results) == 1
        name, (flow_name, owner, token) = results[0]
        assert name == "my-sm"
        assert flow_name == "TargetFlow"
        assert owner == "alice"
        assert token == "prod-token-xyz"

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_sandbox_exception_propagates_through_list_templates(self, MockClient):
        """MetaflowException from list_state_machines in sandbox propagates up."""
        from metaflow.exception import MetaflowException
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        MockClient.return_value.list_state_machines.side_effect = MetaflowException(
            "sandbox"
        )

        with pytest.raises(MetaflowException, match="sandbox"):
            list(StepFunctions.list_templates())

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions.StepFunctionsClient"
    )
    def test_yields_multiple_sms_for_same_flow_name(self, MockClient):
        """Multiple state machines with same flow_name (e.g. prod + staging) are both yielded."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        mock_instance = MockClient.return_value
        mock_instance.list_state_machines.return_value = iter(["sm-prod", "sm-staging"])

        def fake_get(name):
            return _make_describe_response(name, "SharedFlow", "user", "tok")

        mock_instance.get.side_effect = fake_get

        results = list(StepFunctions.list_templates(flow_name="SharedFlow"))
        assert [name for name, _ in results] == ["sm-prod", "sm-staging"]


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

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.get_metadata"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_from_deployment_uses_provided_deployment_metadata_skips_api(
        self, MockSF, mock_get_metadata, MockDeployer
    ):
        """When _deployment_metadata is passed, get_deployment_metadata is NOT called."""
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        mock_get_metadata.return_value = "local@latest"
        mock_deployer_instance = MagicMock()
        mock_sfn_deployer = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = mock_sfn_deployer

        result = StepFunctionsDeployedFlow.from_deployment(
            "my-sm",
            _deployment_metadata=("PreFetchedFlow", "prefetched_user", "tok"),
        )

        MockSF.get_deployment_metadata.assert_not_called()
        assert mock_sfn_deployer.flow_name == "PreFetchedFlow"
        call_kwargs = MockDeployer.call_args
        assert call_kwargs[1]["env"] == {"METAFLOW_USER": "prefetched_user"}

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.os.unlink"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.generate_fake_flow_file_contents"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_from_deployment_temp_file_cleaned_up_on_deployer_failure(
        self, MockSF, MockDeployer, mock_gen, mock_unlink
    ):
        """Temp file is unlinked even when Deployer().step_functions() raises."""
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.get_deployment_metadata.return_value = ("MyFlow", "user", "tok")
        mock_gen.return_value = "class MyFlow(FlowSpec): pass\n"
        MockDeployer.return_value.step_functions.side_effect = RuntimeError(
            "deploy exploded"
        )

        with pytest.raises(RuntimeError, match="deploy exploded"):
            StepFunctionsDeployedFlow.from_deployment("my-sm")

        mock_unlink.assert_called_once()

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.os.unlink"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.generate_fake_flow_file_contents"
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
    def test_from_deployment_temp_file_cleaned_up_on_success(
        self, MockSF, mock_get_metadata, MockDeployer, mock_gen, mock_unlink
    ):
        """Temp file is unlinked on the success path too."""
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.get_deployment_metadata.return_value = ("MyFlow", "user", "tok")
        mock_gen.return_value = "class MyFlow(FlowSpec): pass\n"
        mock_get_metadata.return_value = "local@latest"
        mock_deployer_instance = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = MagicMock()

        StepFunctionsDeployedFlow.from_deployment("my-sm")

        mock_unlink.assert_called_once()

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_from_deployment_not_found_raises_with_identifier_in_message(self, MockSF):
        """Error message includes the identifier so users know which SM failed."""
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )
        from metaflow.exception import MetaflowException

        MockSF.get_deployment_metadata.return_value = None

        with pytest.raises(MetaflowException, match="specific-sm-name"):
            StepFunctionsDeployedFlow.from_deployment("specific-sm-name")


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

        # list_templates now yields (name, metadata) tuples
        MockSF.list_templates.return_value = iter([
            ("sm-a", ("FlowA", "user1", "tok1")),
            ("sm-b", ("FlowB", "user2", "tok2")),
        ])
        mock_get_metadata.return_value = "local@latest"

        mock_deployer_instance = MagicMock()
        mock_sfn_deployer = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = mock_sfn_deployer

        flows = list(StepFunctionsDeployedFlow.list_deployed_flows())

        assert len(flows) == 2
        MockSF.list_templates.assert_called_once_with(flow_name=None)
        # get_deployment_metadata should NOT be called — metadata comes from list_templates
        MockSF.get_deployment_metadata.assert_not_called()

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

        # sm-bad has valid metadata but Deployer() will raise for it
        MockSF.list_templates.return_value = iter([
            ("sm-good", ("FlowGood", "user1", "tok1")),
            ("sm-bad", ("FlowBad", "user2", "tok2")),
        ])
        mock_get_metadata.return_value = "local@latest"

        mock_deployer_instance = MagicMock()
        mock_sfn_deployer = MagicMock()
        MockDeployer.return_value = mock_deployer_instance

        call_count = 0

        def sfn_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("Deployer init failed")
            return mock_sfn_deployer

        mock_deployer_instance.step_functions.side_effect = sfn_side_effect

        flows = list(StepFunctionsDeployedFlow.list_deployed_flows())

        # Only the good one should come through; the bad one is skipped
        assert len(flows) == 1

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_list_deployed_flows_empty_when_no_templates(self, MockSF):
        """No state machines → generator yields nothing without error."""
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.list_templates.return_value = iter([])

        flows = list(StepFunctionsDeployedFlow.list_deployed_flows())
        assert flows == []

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.get_metadata"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_list_deployed_flows_all_fail_yields_nothing(
        self, MockSF, mock_get_metadata, MockDeployer
    ):
        """When every from_deployment call raises, generator yields nothing."""
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.list_templates.return_value = iter([
            ("sm-a", ("FlowA", "user", "tok")),
            ("sm-b", ("FlowB", "user", "tok")),
        ])
        mock_get_metadata.return_value = "local@latest"
        MockDeployer.return_value.step_functions.side_effect = RuntimeError("always fails")

        flows = list(StepFunctionsDeployedFlow.list_deployed_flows())
        assert flows == []

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.Deployer"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.get_metadata"
    )
    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_list_deployed_flows_passes_flow_name_filter_to_list_templates(
        self, MockSF, mock_get_metadata, MockDeployer
    ):
        """flow_name param is forwarded to list_templates."""
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.list_templates.return_value = iter([
            ("sm-target", ("TargetFlow", "user", "tok")),
        ])
        mock_get_metadata.return_value = "local@latest"
        mock_deployer_instance = MagicMock()
        MockDeployer.return_value = mock_deployer_instance
        mock_deployer_instance.step_functions.return_value = MagicMock()

        list(StepFunctionsDeployedFlow.list_deployed_flows(flow_name="TargetFlow"))

        MockSF.list_templates.assert_called_once_with(flow_name="TargetFlow")

    @patch(
        "metaflow.plugins.aws.step_functions.step_functions_deployer_objects.StepFunctions"
    )
    def test_list_deployed_flows_propagates_list_templates_exception(self, MockSF):
        """Exceptions from list_templates (e.g. sandbox error) propagate — not swallowed."""
        from metaflow.exception import MetaflowException
        from metaflow.plugins.aws.step_functions.step_functions_deployer_objects import (
            StepFunctionsDeployedFlow,
        )

        MockSF.list_templates.side_effect = MetaflowException("sandbox")

        with pytest.raises(MetaflowException, match="sandbox"):
            list(StepFunctionsDeployedFlow.list_deployed_flows())
