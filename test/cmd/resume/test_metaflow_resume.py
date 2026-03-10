import os
import sys
import tempfile

import pytest
from unittest.mock import MagicMock, patch

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
        # Run should never be instantiated for a bad pathspec
        mock_run_cls.assert_not_called()

    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_run_not_found(self, mock_run_cls, mock_ns):
        mock_run_cls.side_effect = Exception("Run not found")
        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])
        assert result.exit_code == 1
        mock_run_cls.assert_called_once_with("MyFlow/123", _namespace_check=False)

    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_no_code_package(self, mock_run_cls, mock_ns):
        mock_run = self._make_mock_run(has_code=False)
        mock_run_cls.return_value = mock_run

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])
        assert result.exit_code == 1

    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_code_package_extract_fails(self, mock_run_cls, mock_ns):
        """When code.extract() raises, exit 1 with error message."""
        mock_run = MagicMock()
        mock_run.code.script_name = "myflow.py"
        mock_run.code.extract.side_effect = Exception("Download failed: 404")
        mock_run_cls.return_value = mock_run

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cli, ["resume", "MyFlow/999"])
        assert result.exit_code == 1
        assert "Failed to download code package" in result.stderr

    @patch("metaflow.cmd.resume.subprocess.run")
    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_basic_resume(self, mock_run_cls, mock_ns, mock_subprocess):
        mock_run = self._make_mock_run(script_name="myflow.py")
        mock_run_cls.return_value = mock_run
        mock_subprocess.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])

        mock_subprocess.assert_called_once()
        cmd = mock_subprocess.call_args[0][0]
        # Verify exact command structure: python <script> resume --origin-run-id <id>
        assert cmd[0] == sys.executable
        assert cmd[1].endswith("myflow.py")
        assert cmd[2] == "resume"
        assert cmd[3] == "--origin-run-id"
        assert cmd[4] == "123"
        assert len(cmd) == 5  # no extra args

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
        assert cmd[2] == "resume"
        assert cmd[3] == "--origin-run-id"
        assert cmd[4] == "123"
        assert cmd[5] == "train"

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
    def test_nonzero_exit_prints_error(self, mock_run_cls, mock_ns, mock_subprocess):
        mock_run = self._make_mock_run(script_name="myflow.py")
        mock_run_cls.return_value = mock_run
        mock_subprocess.return_value = MagicMock(returncode=42)

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cli, ["resume", "MyFlow/123"])
        assert result.exit_code == 42
        assert "failed with exit code 42" in result.stderr

    @patch("metaflow.cmd.resume.subprocess.run")
    @patch("metaflow.cmd.resume.namespace")
    @patch("metaflow.cmd.resume.Run")
    def test_resume_cleans_up_temp_dir(self, mock_run_cls, mock_ns, mock_subprocess):
        mock_run = self._make_mock_run(script_name="myflow.py")
        mock_run_cls.return_value = mock_run
        mock_subprocess.return_value = MagicMock(returncode=0)

        tmp_path = mock_run.code.extract.return_value.name

        runner = CliRunner()
        result = runner.invoke(cli, ["resume", "MyFlow/123"])

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
