"""Tests for --enable-schedule / --no-enable-schedule CLI flag."""

import pytest
from unittest.mock import patch


class TestIsScheduleDisabledArgo:
    """Test _is_schedule_disabled in argo_workflows_cli."""

    def _get_fn(self):
        from metaflow.plugins.argo.argo_workflows_cli import _is_schedule_disabled

        return _is_schedule_disabled

    def test_cli_flag_enable_overrides_env(self):
        fn = self._get_fn()
        with patch("metaflow.plugins.argo.argo_workflows_cli.SCHEDULE_DISABLED", True):
            # --enable-schedule should override env var
            assert fn(True) is False

    def test_cli_flag_disable_overrides_env(self):
        fn = self._get_fn()
        with patch("metaflow.plugins.argo.argo_workflows_cli.SCHEDULE_DISABLED", False):
            # --no-enable-schedule should override env var
            assert fn(False) is True

    def test_env_var_disabled(self):
        fn = self._get_fn()
        with patch("metaflow.plugins.argo.argo_workflows_cli.SCHEDULE_DISABLED", True):
            # No CLI flag, env var says disabled
            assert fn(None) is True

    def test_env_var_enabled(self):
        fn = self._get_fn()
        with patch("metaflow.plugins.argo.argo_workflows_cli.SCHEDULE_DISABLED", False):
            # No CLI flag, env var says enabled (default)
            assert fn(None) is False


class TestIsScheduleDisabledSFN:
    """Test _is_schedule_disabled in step_functions_cli."""

    def _get_fn(self):
        from metaflow.plugins.aws.step_functions.step_functions_cli import (
            _is_schedule_disabled,
        )

        return _is_schedule_disabled

    def test_cli_flag_enable_overrides_env(self):
        fn = self._get_fn()
        with patch(
            "metaflow.plugins.aws.step_functions.step_functions_cli.SCHEDULE_DISABLED",
            True,
        ):
            assert fn(True) is False

    def test_cli_flag_disable_overrides_env(self):
        fn = self._get_fn()
        with patch(
            "metaflow.plugins.aws.step_functions.step_functions_cli.SCHEDULE_DISABLED",
            False,
        ):
            assert fn(False) is True

    def test_env_var_disabled(self):
        fn = self._get_fn()
        with patch(
            "metaflow.plugins.aws.step_functions.step_functions_cli.SCHEDULE_DISABLED",
            True,
        ):
            assert fn(None) is True

    def test_env_var_enabled(self):
        fn = self._get_fn()
        with patch(
            "metaflow.plugins.aws.step_functions.step_functions_cli.SCHEDULE_DISABLED",
            False,
        ):
            assert fn(None) is False


class TestArgoScheduleDisabled:
    """Test that ArgoWorkflows.schedule() respects schedule_disabled."""

    def test_schedule_passes_none_when_disabled(self):
        """When schedule_disabled=True, schedule and timezone should be None."""
        from unittest.mock import MagicMock

        from metaflow.plugins.argo.argo_workflows import ArgoWorkflows

        aw = object.__new__(ArgoWorkflows)
        aw.name = "test-workflow"
        aw._schedule = "0 * * * *"
        aw._timezone = "UTC"
        aw._sensor = None

        mock_client = MagicMock()
        with patch(
            "metaflow.plugins.argo.argo_workflows.ArgoClient",
            return_value=mock_client,
        ):
            aw.schedule(schedule_disabled=True)

        mock_client.schedule_workflow_template.assert_called_once_with(
            "test-workflow", None, None
        )

    def test_schedule_passes_values_when_enabled(self):
        """When schedule_disabled=False, schedule and timezone should pass through."""
        from unittest.mock import MagicMock

        from metaflow.plugins.argo.argo_workflows import ArgoWorkflows

        aw = object.__new__(ArgoWorkflows)
        aw.name = "test-workflow"
        aw._schedule = "0 * * * *"
        aw._timezone = "UTC"
        aw._sensor = None

        mock_client = MagicMock()
        with patch(
            "metaflow.plugins.argo.argo_workflows.ArgoClient",
            return_value=mock_client,
        ):
            aw.schedule(schedule_disabled=False)

        mock_client.schedule_workflow_template.assert_called_once_with(
            "test-workflow", "0 * * * *", "UTC"
        )


class TestSFNScheduleDisabled:
    """Test that StepFunctions.schedule() respects schedule_disabled."""

    def test_schedule_skipped_when_disabled(self):
        """When schedule_disabled=True, EventBridge schedule should not be created."""
        from unittest.mock import MagicMock

        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        sf = object.__new__(StepFunctions)
        sf.name = "test-state-machine"
        sf._cron = "0 * * * ? *"

        with patch(
            "metaflow.plugins.aws.step_functions.step_functions.EventBridgeClient"
        ) as mock_eb:
            sf.schedule(schedule_disabled=True)

        # EventBridgeClient should not be called at all
        mock_eb.assert_not_called()

    def test_schedule_created_when_enabled(self):
        """When schedule_disabled=False, EventBridge schedule should be created normally."""
        from unittest.mock import MagicMock

        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        sf = object.__new__(StepFunctions)
        sf.name = "test-state-machine"
        sf._cron = "0 * * * ? *"
        sf._state_machine_arn = "arn:aws:states:us-east-1:123:stateMachine:test"

        mock_eb_instance = MagicMock()
        mock_eb_instance.cron.return_value = mock_eb_instance
        mock_eb_instance.role_arn.return_value = mock_eb_instance
        mock_eb_instance.state_machine_arn.return_value = mock_eb_instance
        mock_eb_instance.schedule.return_value = "test-state-machine"

        with patch(
            "metaflow.plugins.aws.step_functions.step_functions.EventBridgeClient",
            return_value=mock_eb_instance,
        ), patch(
            "metaflow.plugins.aws.step_functions.step_functions.EVENTS_SFN_ACCESS_IAM_ROLE",
            "arn:aws:iam::123:role/test",
        ):
            sf.schedule(schedule_disabled=False)

        mock_eb_instance.cron.assert_called_once_with("0 * * * ? *")
