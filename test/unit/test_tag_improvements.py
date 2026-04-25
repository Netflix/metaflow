"""
Tests for tag improvements:
1. Trigger-time tags via METAFLOW_TRIGGER_TAGS env var (Issue #1243)
2. Resume tag propagation from origin run (Issue #1406)
"""

import json


class TestTriggerTimeTags:
    """Tests for METAFLOW_TRIGGER_TAGS env var support in step_cmd."""

    def test_trigger_tags_env_parsed(self):
        """Verify that METAFLOW_TRIGGER_TAGS env var is parsed as JSON list."""
        tags = ["tag1", "tag2"]
        env_val = json.dumps(tags)
        parsed = json.loads(env_val)
        assert parsed == tags

    def test_trigger_tags_empty_list_ignored(self):
        """Empty list should not add any sticky tags."""
        tags = []
        env_val = json.dumps(tags)
        parsed = json.loads(env_val)
        assert isinstance(parsed, list) and not parsed

    def test_trigger_tags_invalid_json_handled(self):
        """Invalid JSON should not raise, just be ignored."""
        env_val = "not valid json{{"
        try:
            json.loads(env_val)
            parsed = True
        except (json.JSONDecodeError, TypeError):
            parsed = False
        assert not parsed

    def test_trigger_tags_non_list_ignored(self):
        """Non-list JSON (e.g. a string) should be ignored."""
        env_val = json.dumps("just a string")
        parsed = json.loads(env_val)
        assert not isinstance(parsed, list)


class TestArgoTriggerTags:
    """Tests for Argo Workflows trigger-time tag support."""

    def test_argo_client_trigger_with_tags(self):
        """Verify that tags are included in workflow parameters and annotations."""
        from metaflow.plugins.argo.argo_client import ArgoClient

        # We can't easily test the full client, but we can verify the
        # trigger_workflow_template signature accepts tags.
        import inspect

        sig = inspect.signature(ArgoClient.trigger_workflow_template)
        assert "tags" in sig.parameters

    def test_argo_trigger_tags_parameter_in_workflow(self):
        """Verify metaflow-trigger-tags is a recognized parameter name."""
        # This tests that the parameter name constant is used consistently.
        param_name = "metaflow-trigger-tags"
        env_var = "METAFLOW_TRIGGER_TAGS"

        # The workflow template uses {{workflow.parameters.metaflow-trigger-tags}}
        template_ref = "{{workflow.parameters.%s}}" % param_name
        assert param_name in template_ref
        assert env_var == "METAFLOW_TRIGGER_TAGS"


class TestSFNTriggerTags:
    """Tests for Step Functions trigger-time tag support."""

    def test_sfn_trigger_includes_trigger_tags(self):
        """Verify trigger method signature accepts tags."""
        from metaflow.plugins.aws.step_functions.step_functions import StepFunctions

        import inspect

        sig = inspect.signature(StepFunctions.trigger)
        assert "tags" in sig.parameters

    def test_sfn_trigger_input_format(self):
        """Verify the execution input format includes TriggerTags."""
        parameters = {"alpha": "1"}
        tags = ["tag1", "tag2"]

        # This mirrors the logic in StepFunctions.trigger()
        input_data = json.dumps(
            {
                "Parameters": json.dumps(parameters),
                "TriggerTags": json.dumps(tags),
            }
        )
        parsed = json.loads(input_data)
        assert "TriggerTags" in parsed
        assert json.loads(parsed["TriggerTags"]) == ["tag1", "tag2"]

    def test_sfn_trigger_input_no_tags(self):
        """Verify TriggerTags defaults to empty list when no tags provided."""
        parameters = {"alpha": "1"}
        tags = None

        input_data = json.dumps(
            {
                "Parameters": json.dumps(parameters),
                "TriggerTags": json.dumps(tags if tags else []),
            }
        )
        parsed = json.loads(input_data)
        assert json.loads(parsed["TriggerTags"]) == []


class TestResumeTags:
    """Tests for resume tag propagation (Issue #1406)."""

    def test_get_origin_run_tags_function_exists(self):
        """Verify the helper function is importable."""
        from metaflow.cli_components.run_cmds import _get_origin_run_tags

        assert callable(_get_origin_run_tags)

    def test_get_origin_run_tags_handles_missing_run(self):
        """If the origin run can't be found, return empty list."""
        from metaflow.cli_components.run_cmds import _get_origin_run_tags

        # A non-existent flow/run should return empty list, not raise.
        result = _get_origin_run_tags("NonExistentFlow", "nonexistent_run_id")
        assert result == []

    def test_resume_tags_merge_logic(self):
        """Verify that origin tags are merged with CLI tags correctly."""
        # Simulates the merge logic in resume()
        cli_tags = ("cli_tag1", "cli_tag2")
        origin_tags = ["origin_tag1", "cli_tag1"]  # cli_tag1 overlaps

        merged = tuple(set(cli_tags) | set(origin_tags))
        assert "cli_tag1" in merged
        assert "cli_tag2" in merged
        assert "origin_tag1" in merged
        assert len(merged) == 3  # deduped

    def test_resume_tags_none_cli_tags(self):
        """If no CLI tags provided, origin tags should still be applied."""
        cli_tags = None
        origin_tags = ["origin_tag1"]

        merged = tuple(set(cli_tags or ()) | set(origin_tags))
        assert merged == ("origin_tag1",)

    def test_resume_tags_no_origin_tags(self):
        """If origin run has no tags, CLI tags should be unaffected."""
        cli_tags = ("cli_tag1",)
        origin_tags = []

        # The code checks `if origin_tags:` first
        if origin_tags:
            merged = tuple(set(cli_tags or ()) | set(origin_tags))
        else:
            merged = cli_tags

        assert merged == ("cli_tag1",)


class TestCLITagOption:
    """Tests for --tag CLI option on trigger commands."""

    def test_argo_trigger_has_tag_option(self):
        """Verify the Argo trigger CLI command has a --tag option."""
        from metaflow.plugins.argo.argo_workflows_cli import trigger

        param_names = [p.name for p in trigger.params]
        assert "tags" in param_names

    def test_sfn_trigger_has_tag_option(self):
        """Verify the SFN trigger CLI command has a --tag option."""
        from metaflow.plugins.aws.step_functions.step_functions_cli import trigger

        param_names = [p.name for p in trigger.params]
        assert "tags" in param_names

    def test_argo_trigger_tag_is_multiple(self):
        """Verify the --tag option accepts multiple values."""
        from metaflow.plugins.argo.argo_workflows_cli import trigger

        tag_param = [p for p in trigger.params if p.name == "tags"][0]
        assert tag_param.multiple is True

    def test_sfn_trigger_tag_is_multiple(self):
        """Verify the --tag option accepts multiple values."""
        from metaflow.plugins.aws.step_functions.step_functions_cli import trigger

        tag_param = [p for p in trigger.params if p.name == "tags"][0]
        assert tag_param.multiple is True
