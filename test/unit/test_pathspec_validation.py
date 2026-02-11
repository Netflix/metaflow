"""
Unit tests for pathspec validation in MetaflowObject.

Tests the validation added in issue #948 to ensure pathspecs follow the expected format:
- Flow: FlowName
- Run: FlowName/RunID
- Step: FlowName/RunID/StepName
- Task: FlowName/RunID/StepName/TaskID
- DataArtifact: FlowName/RunID/StepName/TaskID/ArtifactName
"""

import unittest
from metaflow.exception import MetaflowInvalidPathspec
from metaflow.client.core import MetaflowObject


class TestPathspecValidation(unittest.TestCase):
    """Test pathspec format validation."""

    def test_flow_valid_pathspecs(self):
        """Test that valid flow pathspecs are accepted."""
        valid_pathspecs = [
            "MyFlow",
            "my_flow",
            "MyFlow123",
            "_private_flow",
            "Flow_With_Underscores",
        ]
        for pathspec in valid_pathspecs:
            with self.subTest(pathspec=pathspec):
                result = MetaflowObject._validate_pathspec_format(pathspec, "flow")
                self.assertEqual(result, [pathspec])

    def test_flow_invalid_pathspecs(self):
        """Test that invalid flow pathspecs are rejected."""
        invalid_cases = [
            ("", "empty"),
            ("/MyFlow", "leading slash"),
            ("MyFlow/", "trailing slash"),
            ("123Flow", "starts with number"),
            ("My-Flow", "contains dash"),
            ("My Flow", "contains space"),
            ("My/Flow", "contains slash"),
        ]
        for pathspec, description in invalid_cases:
            with self.subTest(pathspec=pathspec, reason=description):
                with self.assertRaises(MetaflowInvalidPathspec):
                    MetaflowObject._validate_pathspec_format(pathspec, "flow")

    def test_run_valid_pathspecs(self):
        """Test that valid run pathspecs are accepted."""
        valid_pathspecs = [
            "MyFlow/123",
            "my_flow/456",
            "MyFlow123/789",
            "_private_flow/1",
        ]
        for pathspec in valid_pathspecs:
            with self.subTest(pathspec=pathspec):
                result = MetaflowObject._validate_pathspec_format(pathspec, "run")
                self.assertEqual(len(result), 2)

    def test_run_invalid_pathspecs(self):
        """Test that invalid run pathspecs are rejected."""
        invalid_cases = [
            ("MyFlow", "too few components"),
            ("MyFlow/123/extra", "too many components"),
            ("MyFlow/abc", "non-numeric run ID"),
            ("MyFlow//123", "empty component"),
            ("MyFlow/123/", "trailing slash"),
            ("/MyFlow/123", "leading slash"),
            ("123Flow/123", "invalid flow name"),
            ("MyFlow/12.3", "decimal run ID"),
            ("MyFlow/-123", "negative run ID"),
        ]
        for pathspec, description in invalid_cases:
            with self.subTest(pathspec=pathspec, reason=description):
                with self.assertRaises(MetaflowInvalidPathspec):
                    MetaflowObject._validate_pathspec_format(pathspec, "run")

    def test_step_valid_pathspecs(self):
        """Test that valid step pathspecs are accepted."""
        valid_pathspecs = [
            "MyFlow/123/start",
            "my_flow/456/end",
            "MyFlow/789/my_step",
            "_private_flow/1/_private_step",
        ]
        for pathspec in valid_pathspecs:
            with self.subTest(pathspec=pathspec):
                result = MetaflowObject._validate_pathspec_format(pathspec, "step")
                self.assertEqual(len(result), 3)

    def test_step_invalid_pathspecs(self):
        """Test that invalid step pathspecs are rejected."""
        invalid_cases = [
            ("MyFlow/123", "too few components"),
            ("MyFlow/123/start/extra", "too many components"),
            ("MyFlow/abc/start", "non-numeric run ID"),
            ("MyFlow/123/123step", "step name starts with number"),
            ("MyFlow/123/my-step", "step name contains dash"),
            ("MyFlow//start", "empty run ID"),
            ("MyFlow/123//", "empty step name"),
        ]
        for pathspec, description in invalid_cases:
            with self.subTest(pathspec=pathspec, reason=description):
                with self.assertRaises(MetaflowInvalidPathspec):
                    MetaflowObject._validate_pathspec_format(pathspec, "step")

    def test_task_valid_pathspecs(self):
        """Test that valid task pathspecs are accepted."""
        valid_pathspecs = [
            "MyFlow/123/start/1",
            "my_flow/456/end/999",
            "MyFlow/789/my_step/42",
            "_private_flow/1/_private_step/0",
        ]
        for pathspec in valid_pathspecs:
            with self.subTest(pathspec=pathspec):
                result = MetaflowObject._validate_pathspec_format(pathspec, "task")
                self.assertEqual(len(result), 4)

    def test_task_invalid_pathspecs(self):
        """Test that invalid task pathspecs are rejected."""
        invalid_cases = [
            ("MyFlow/123/start", "too few components"),
            ("MyFlow/123/start/1/extra", "too many components"),
            ("MyFlow/abc/start/1", "non-numeric run ID"),
            ("MyFlow/123/start/abc", "non-numeric task ID"),
            ("MyFlow/123/start/1.5", "decimal task ID"),
            ("MyFlow/123///1", "empty step name"),
        ]
        for pathspec, description in invalid_cases:
            with self.subTest(pathspec=pathspec, reason=description):
                with self.assertRaises(MetaflowInvalidPathspec):
                    MetaflowObject._validate_pathspec_format(pathspec, "task")

    def test_artifact_valid_pathspecs(self):
        """Test that valid artifact pathspecs are accepted."""
        valid_pathspecs = [
            "MyFlow/123/start/1/my_artifact",
            "my_flow/456/end/999/result",
            "MyFlow/789/my_step/42/_private_var",
            "_private_flow/1/_private_step/0/data",
        ]
        for pathspec in valid_pathspecs:
            with self.subTest(pathspec=pathspec):
                result = MetaflowObject._validate_pathspec_format(
                    pathspec, "artifact"
                )
                self.assertEqual(len(result), 5)

    def test_artifact_invalid_pathspecs(self):
        """Test that invalid artifact pathspecs are rejected."""
        invalid_cases = [
            ("MyFlow/123/start/1", "too few components"),
            ("MyFlow/123/start/1/artifact/extra", "too many components"),
            ("MyFlow/abc/start/1/artifact", "non-numeric run ID"),
            ("MyFlow/123/start/abc/artifact", "non-numeric task ID"),
            ("MyFlow/123/start/1/123artifact", "artifact name starts with number"),
            ("MyFlow/123/start/1/my-artifact", "artifact name contains dash"),
            ("MyFlow/123/start//artifact", "empty task ID"),
        ]
        for pathspec, description in invalid_cases:
            with self.subTest(pathspec=pathspec, reason=description):
                with self.assertRaises(MetaflowInvalidPathspec):
                    MetaflowObject._validate_pathspec_format(pathspec, "artifact")

    def test_empty_components(self):
        """Test that pathspecs with empty components are rejected."""
        invalid_pathspecs = [
            ("//", "flow"),
            ("Flow//123", "run"),
            ("Flow/123//", "step"),
            ("Flow//step/1", "run"),
            ("Flow/123/step//", "task"),
        ]
        for pathspec, object_type in invalid_pathspecs:
            with self.subTest(pathspec=pathspec, object_type=object_type):
                with self.assertRaises(MetaflowInvalidPathspec) as cm:
                    MetaflowObject._validate_pathspec_format(pathspec, object_type)
                self.assertIn("empty", str(cm.exception).lower())

    def test_leading_trailing_slashes(self):
        """Test that pathspecs with leading or trailing slashes are rejected."""
        invalid_pathspecs = [
            ("/MyFlow", "flow"),
            ("MyFlow/", "flow"),
            ("/MyFlow/123", "run"),
            ("MyFlow/123/", "run"),
            ("/MyFlow/123/start", "step"),
            ("MyFlow/123/start/", "step"),
        ]
        for pathspec, object_type in invalid_pathspecs:
            with self.subTest(pathspec=pathspec, object_type=object_type):
                with self.assertRaises(MetaflowInvalidPathspec) as cm:
                    MetaflowObject._validate_pathspec_format(pathspec, object_type)
                self.assertIn("cannot start or end", str(cm.exception).lower())

    def test_error_messages_are_helpful(self):
        """Test that error messages provide helpful information."""
        # Test invalid flow name
        with self.assertRaises(MetaflowInvalidPathspec) as cm:
            MetaflowObject._validate_pathspec_format("123Flow", "flow")
        error_msg = str(cm.exception)
        self.assertIn("123Flow", error_msg)
        self.assertIn("flow name", error_msg.lower())

        # Test invalid run ID
        with self.assertRaises(MetaflowInvalidPathspec) as cm:
            MetaflowObject._validate_pathspec_format("MyFlow/abc", "run")
        error_msg = str(cm.exception)
        self.assertIn("abc", error_msg)
        self.assertIn("run ID", error_msg)
        self.assertIn("numeric", error_msg.lower())

        # Test wrong number of components
        with self.assertRaises(MetaflowInvalidPathspec) as cm:
            MetaflowObject._validate_pathspec_format("MyFlow", "run")
        error_msg = str(cm.exception)
        self.assertIn("Run('FlowName/RunID')", error_msg)


if __name__ == "__main__":
    unittest.main()
