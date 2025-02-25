import os
import pytest
import tempfile
from subprocess import PIPE
from unittest.mock import MagicMock, patch

from metaflow.cmd.code import (
    extract_code_package,
    op_diff,
    op_patch,
    op_pull,
    perform_diff,
    run_op,
)


class TestMetaflowDiff:

    @patch("metaflow.cmd.code.Run")
    def test_extract_code_package(self, mock_run):
        mock_run.return_value.code.tarball.getmembers.return_value = []
        mock_run.return_value.code.tarball.extractall = MagicMock()
        runspec = "HelloFlow/3"

        with patch(
            "tempfile.TemporaryDirectory", return_value=tempfile.TemporaryDirectory()
        ) as mock_tmp:
            tmp = extract_code_package(runspec)

        mock_run.assert_called_once_with(runspec, _namespace_check=False)
        assert os.path.exists(tmp.name)

    @pytest.mark.parametrize("use_tty", [True, False])
    @patch("metaflow.cmd.code.run")
    @patch("sys.stdout.isatty")
    def test_perform_diff_output_false(self, mock_isatty, mock_run, use_tty):
        mock_isatty.return_value = use_tty

        mock_process = MagicMock()
        mock_process.returncode = 1
        mock_process.stdout = (
            "--- a/file.txt\n"
            "+++ b/file.txt\n"
            "@@ -1 +1 @@\n"
            "-source content\n"
            "+target content\n"
        )
        mock_run.return_value = mock_process

        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
            source_file = os.path.join(source_dir, "file.txt")
            target_file = os.path.join(target_dir, "file.txt")
            with open(source_file, "w") as f:
                f.write("source content")
            with open(target_file, "w") as f:
                f.write("target content")

            perform_diff(source_dir, target_dir, output=False)

            # if output=False, run should be called twice:
            # 1. git diff
            # 2. less -R
            assert mock_run.call_count == 2

            mock_run.assert_any_call(
                [
                    "git",
                    "diff",
                    "--no-index",
                    "--exit-code",
                    "--color" if use_tty else "--no-color",
                    "./file.txt",
                    source_file,
                ],
                text=True,
                stdout=PIPE,
                cwd=target_dir,
            )

            mock_run.assert_any_call(
                ["less", "-R"], input=mock_process.stdout, text=True
            )

    @patch("metaflow.cmd.code.run")
    def test_perform_diff_output_true(self, mock_run):
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
            source_file = os.path.join(source_dir, "file.txt")
            target_file = os.path.join(target_dir, "file.txt")
            with open(source_file, "w") as f:
                f.write("source content")
            with open(target_file, "w") as f:
                f.write("target content")

            perform_diff(source_dir, target_dir, output=True)

            assert mock_run.call_count == 1

            mock_run.assert_called_once_with(
                [
                    "git",
                    "diff",
                    "--no-index",
                    "--exit-code",
                    "--no-color",
                    "./file.txt",
                    source_file,
                ],
                text=True,
                stdout=PIPE,
                cwd=target_dir,
            )

    @patch("shutil.rmtree")
    @patch("metaflow.cmd.code.extract_code_package")
    @patch("metaflow.cmd.code.op_diff")
    def test_run_op(self, mock_op_diff, mock_extract_code_package, mock_rmtree):
        mock_tmp = tempfile.TemporaryDirectory()
        mock_extract_code_package.return_value = mock_tmp
        runspec = "HelloFlow/3"

        run_op(runspec, mock_op_diff)

        mock_extract_code_package.assert_called_once_with(runspec)
        mock_op_diff.assert_called_once_with(mock_tmp.name)

        mock_rmtree.assert_any_call(mock_tmp.name)

    @patch("metaflow.cmd.code.perform_diff")
    def test_op_patch(self, mock_perform_diff):
        mock_perform_diff.return_value = ["diff --git a/file.txt b/file.txt\n"]

        with tempfile.TemporaryDirectory() as tmpdir:
            patch_file = os.path.join(tmpdir, "patch.patch")

            op_patch(tmpdir, patch_file)

            mock_perform_diff.assert_called_once_with(tmpdir, output=True)
            with open(patch_file, "r") as f:
                content = f.read()
            assert "diff --git a/file.txt b/file.txt\n" in content


if __name__ == "__main__":
    pytest.main([__file__])
