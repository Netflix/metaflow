import os
import tempfile

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from metaflow._vendor.click.testing import CliRunner

from metaflow.cmd.resume import cli, resume


class TestMetaflowResume:
    """Tests for the top-level 'metaflow resume' command."""

    def _make_mock_run(self, script_name="myflow.py", has_code=True):
        """Create a mock Run with a mock code package."""
        mock_run = MagicMock()
        if has_code:
            mock_code = MagicMock()
            mock_code.script_name = script_name

            # Make extract() return a TemporaryDirectory with a script file
            tmp = tempfile.TemporaryDirectory()
            script_path = os.path.join(tmp.name, script_name)
            with open(script_path, "w") as f:
                f.write("# mock flow script\n")
            mock_code.extract.return_value = tmp
            mock_run.code = mock_code
        else:
            mock_run.code = None
        return mock_run

    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "--help"])
        assert result.exit_code == 0
        assert "RUN_PATHSPEC" in result.output
        assert "--step" in result.output

    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_invalid_pathspec(self, mock_run_cls, mock_ns):
        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "InvalidPathspec"])
        assert result.exit_code != 0

    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_run_not_found(self, mock_run_cls, mock_ns):
        mock_run_cls.side_effect = Exception("Run not found")
        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])
        assert result.exit_code == 1

    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_no_code_package(self, mock_run_cls, mock_ns):
        mock_run = self._make_mock_run(has_code=False)
        mock_run_cls.return_value = mock_run

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])
        assert result.exit_code == 1

    @patch("metaflow.cmd.resume.subprocess.run")
    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_basic_resume(self, mock_run_cls, mock_ns, mock_subprocess):
        mock_run = self._make_mock_run(script_name="myflow.py")
        mock_run_cls.return_value = mock_run
        mock_subprocess.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])

        # Verify subprocess was called with the right arguments
        mock_subprocess.assert_called_once()
        cmd = mock_subprocess.call_args[0][0]
        assert "myflow.py" in cmd[1]
        assert "resume" in cmd
        assert "--origin-run-id" in cmd
        assert "123" in cmd

    @patch("metaflow.cmd.resume.subprocess.run")
    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_resume_with_step(self, mock_run_cls, mock_ns, mock_subprocess):
        mock_run = self._make_mock_run(script_name="myflow.py")
        mock_run_cls.return_value = mock_run
        mock_subprocess.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123", "--step", "train"])

        mock_subprocess.assert_called_once()
        cmd = mock_subprocess.call_args[0][0]
        assert "train" in cmd

    @patch("metaflow.cmd.resume.subprocess.run")
    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_resume_passthrough_args(self, mock_run_cls, mock_ns, mock_subprocess):
        mock_run = self._make_mock_run(script_name="myflow.py")
        mock_run_cls.return_value = mock_run
        mock_subprocess.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(
            cli, ["resume", "MyFlow/123", "--", "--max-workers", "4", "--tag", "test"]
        )

        mock_subprocess.assert_called_once()
        cmd = mock_subprocess.call_args[0][0]
        assert "--max-workers" in cmd
        assert "4" in cmd
        assert "--tag" in cmd
        assert "test" in cmd

    @patch("metaflow.cmd.resume.subprocess.run")
    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_resume_propagates_exit_code(self, mock_run_cls, mock_ns, mock_subprocess):
        mock_run = self._make_mock_run(script_name="myflow.py")
        mock_run_cls.return_value = mock_run
        mock_subprocess.return_value = MagicMock(returncode=42)

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])
        assert result.exit_code == 42

    @patch("metaflow.cmd.resume.subprocess.run")
    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_resume_cleans_up_temp_dir(self, mock_run_cls, mock_ns, mock_subprocess):
        mock_run = self._make_mock_run(script_name="myflow.py")
        mock_run_cls.return_value = mock_run
        mock_subprocess.return_value = MagicMock(returncode=0)

        # Capture the tmp_path used
        tmp_path = mock_run.code.extract.return_value.name

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])

        # Temp directory should be cleaned up
        assert not os.path.exists(tmp_path)

    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_namespace_set_to_none(self, mock_run_cls, mock_ns):
        """Verify namespace is set to None so any run can be found."""
        mock_run = self._make_mock_run(has_code=False)
        mock_run_cls.return_value = mock_run

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])

        mock_ns.assert_called_once_with(None)
