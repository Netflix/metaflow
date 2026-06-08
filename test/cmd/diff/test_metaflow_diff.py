import os
import pytest
from subprocess import PIPE

from metaflow.cmd.code import (
    extract_code_package,
    op_diff,
    op_patch,
    op_pull,
    perform_diff,
    run_op,
)


def test_extract_code_package_creates_temp_dir(mocker):
    """Test that extract_code_package safely unpacks the tarball into a temporary directory."""

    mock_run = mocker.patch("metaflow.cmd.code.Run")
    mock_run.return_value.code.tarball.getmembers.return_value = []
    mock_run.return_value.code.tarball.extractall = mocker.MagicMock()

    mock_tmp = mocker.patch("tempfile.TemporaryDirectory")
    mock_tmp.return_value.name = "/fake/tmp/dir"

    runspec = "HelloFlow/3"

    # Act
    tmp = extract_code_package(runspec)

    # Assert
    mock_run.assert_called_once_with(runspec, _namespace_check=False)
    assert tmp.name == "/fake/tmp/dir"


@pytest.mark.parametrize(
    "use_tty", [True, False], ids=["with_tty_colors", "without_tty_colors"]
)
def test_perform_diff_triggers_git_and_less_when_output_false(
    mocker, tmp_path, use_tty
):
    """Test that perform_diff executes git diff and pipes stdout to less when output is False."""
    mock_isatty = mocker.patch("sys.stdout.isatty", return_value=use_tty)
    mock_run = mocker.patch("metaflow.cmd.code.run")

    # Setup: Create a fake git diff process output
    mock_process = mocker.MagicMock()
    mock_process.returncode = 1
    mock_process.stdout = (
        "--- a/file.txt\n"
        "+++ b/file.txt\n"
        "@@ -1 +1 @@\n"
        "-source content\n"
        "+target content\n"
    )
    mock_run.return_value = mock_process

    # Setup: Create physical files using pytest's tmp_path fixture
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    source_file = source_dir / "file.txt"
    target_file = target_dir / "file.txt"
    source_file.write_text("source content")
    target_file.write_text("target content")

    # Act
    perform_diff(str(source_dir), str(target_dir), output=False)

    # Assert: git diff and less should both be called sequentially
    assert mock_run.call_count == 2
    mock_run.assert_any_call(
        [
            "git",
            "diff",
            "--no-index",
            "--exit-code",
            "--color" if use_tty else "--no-color",
            "./file.txt",
            str(source_file),
        ],
        text=True,
        stdout=PIPE,
        cwd=str(target_dir),
    )
    mock_run.assert_any_call(["less", "-R"], input=mock_process.stdout, text=True)


def test_perform_diff_returns_early_when_output_true(mocker, tmp_path):
    """Test that perform_diff only runs git diff and skips less when output is True."""
    mock_run = mocker.patch("metaflow.cmd.code.run")

    # Setup: Create physical files
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    source_file = source_dir / "file.txt"
    target_file = target_dir / "file.txt"
    source_file.write_text("source content")
    target_file.write_text("target content")

    # Act
    perform_diff(str(source_dir), str(target_dir), output=True)

    # Assert: Only git diff is called
    assert mock_run.call_count == 1
    mock_run.assert_called_once_with(
        [
            "git",
            "diff",
            "--no-index",
            "--exit-code",
            "--no-color",
            "./file.txt",
            str(source_file),
        ],
        text=True,
        stdout=PIPE,
        cwd=str(target_dir),
    )


def test_run_op_cleans_up_temporary_directory_after_execution(mocker):
    """Test that run_op delegates to op_diff and correctly tears down the temp directory."""
    mock_rmtree = mocker.patch("shutil.rmtree")
    mock_extract = mocker.patch("metaflow.cmd.code.extract_code_package")
    mock_op_diff = mocker.MagicMock()

    # Setup: Mock the temporary directory object returned by extract_code_package
    mock_tmp = mocker.MagicMock()
    mock_tmp.name = "/fake/tmp/dir"
    mock_extract.return_value = mock_tmp

    runspec = "HelloFlow/3"

    # Act
    run_op(runspec, mock_op_diff)

    # Assert
    mock_extract.assert_called_once_with(runspec)
    mock_op_diff.assert_called_once_with("/fake/tmp/dir")
    mock_rmtree.assert_any_call("/fake/tmp/dir")


def test_op_patch_writes_diff_output_to_file(mocker, tmp_path):
    """Test that op_patch successfully writes the generated diff to the specified patch file."""
    mock_perform_diff = mocker.patch("metaflow.cmd.code.perform_diff")
    mock_perform_diff.return_value = ["diff --git a/file.txt b/file.txt\n"]

    patch_file = tmp_path / "patch.patch"

    # Act
    op_patch(str(tmp_path), str(patch_file))

    # Assert
    mock_perform_diff.assert_called_once_with(str(tmp_path), output=True)
    assert patch_file.read_text() == "diff --git a/file.txt b/file.txt\n"
