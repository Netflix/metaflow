"""Test that S3 client works when current directory is not writeable."""

import os
from pathlib import Path
import subprocess
import sys
import textwrap


S3_SUBPROCESS_SCRIPT = textwrap.dedent(
    """
    import os
    import sys
    import tempfile
    from types import ModuleType

    # Keep this test independent of the optional boto3 dependency.
    boto3 = ModuleType("boto3")
    boto3_s3 = ModuleType("boto3.s3")
    transfer = ModuleType("boto3.s3.transfer")

    class TransferConfig:
        multipart_threshold = 8 * 1024 * 1024

    transfer.TransferConfig = TransferConfig
    boto3.s3 = boto3_s3
    boto3_s3.transfer = transfer
    sys.modules.update(
        {
            "boto3": boto3,
            "boto3.s3": boto3_s3,
            "boto3.s3.transfer": transfer,
        }
    )

    import metaflow
    from metaflow.plugins.datatools import S3 as DatatoolsS3
    from metaflow.plugins.datatools.s3 import S3
    from metaflow.plugins.datastores.s3_storage import S3 as StorageS3

    assert metaflow.S3 is DatatoolsS3 is S3 is StorageS3

    s3_client = S3()
    try:
        expected_tempdir = os.environ.get("TEST_EXPECTED_METAFLOW_TEMPDIR")
        if expected_tempdir is None:
            assert s3_client._tmproot == tempfile.gettempdir()
            assert s3_client._tmpdir.startswith(tempfile.gettempdir())
            assert not s3_client._tmpdir.startswith(os.getcwd())
        else:
            assert s3_client._tmproot == expected_tempdir
            assert os.path.dirname(s3_client._tmpdir) == expected_tempdir
    finally:
        s3_client.close()
    """
)


def _run_s3_subprocess(cwd, env, expected_tempdir=None):
    """Run an S3 configuration scenario in a fresh Python interpreter."""
    repo_root = str(Path(__file__).resolve().parents[2])
    env = env.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        path for path in (repo_root, env.get("PYTHONPATH")) if path
    )
    if expected_tempdir is not None:
        env["TEST_EXPECTED_METAFLOW_TEMPDIR"] = str(expected_tempdir)
    else:
        env.pop("TEST_EXPECTED_METAFLOW_TEMPDIR", None)

    import metaflow

    s3_class_before = metaflow.S3
    result = subprocess.run(
        [sys.executable, "-c", S3_SUBPROCESS_SCRIPT],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert (
        result.returncode == 0
    ), "S3 subprocess failed with exit code %d\nstdout:\n%s\nstderr:\n%s" % (
        result.returncode,
        result.stdout,
        result.stderr,
    )
    assert metaflow.S3 is s3_class_before


def test_s3_client_readonly_cwd(tmp_path):
    """Test that S3 client doesn't require writable CWD (issue #854)."""
    readonly_dir = tmp_path / "readonly"
    readonly_dir.mkdir()
    os.chmod(readonly_dir, 0o555)

    env = os.environ.copy()
    env.pop("METAFLOW_TEMPDIR", None)
    env["METAFLOW_HOME"] = str(tmp_path / "empty_metaflow_home")
    try:
        _run_s3_subprocess(readonly_dir, env)
    finally:
        # Make directory writable again so pytest can clean it up.
        os.chmod(readonly_dir, 0o755)


def test_s3_client_respects_metaflow_tempdir(tmp_path):
    """Test that METAFLOW_TEMPDIR overrides the system temporary directory."""
    configured_dir = tmp_path / "configured"
    configured_dir.mkdir()

    env = os.environ.copy()
    env["METAFLOW_TEMPDIR"] = str(configured_dir)
    env["METAFLOW_HOME"] = str(tmp_path / "empty_metaflow_home")
    _run_s3_subprocess(tmp_path, env, configured_dir)
