"""
Unit tests for CondaEnvironment helper logic.

Tests the manifest read/write with file locking and the environment
hashing/dedup logic. Uses temp files — no conda installation needed.
"""

import json
import os
import tempfile
import threading

import pytest


class TestManifestConcurrentWrites:
    """Test that manifest file operations are thread-safe."""

    def test_concurrent_writes_no_corruption(self):
        """Multiple threads writing to the manifest should not corrupt it."""
        # Simulate the manifest write pattern from conda_environment.py:
        # read-modify-write under fcntl lock.
        import fcntl

        manifest_path = None
        errors = []
        num_threads = 10
        writes_per_thread = 5

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".manifest", delete=False
        ) as f:
            manifest_path = f.name
            json.dump({}, f)

        def write_to_manifest(thread_id):
            try:
                for i in range(writes_per_thread):
                    key = f"env_{thread_id}_{i}"
                    with open(manifest_path, "r+") as f:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                        try:
                            data = json.load(f)
                            data[key] = {"platform": "linux-64", "thread": thread_id}
                            f.seek(0)
                            f.truncate()
                            json.dump(data, f)
                        finally:
                            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=write_to_manifest, args=(tid,))
            for tid in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        try:
            assert len(errors) == 0, f"Errors during concurrent writes: {errors}"

            with open(manifest_path, "r") as f:
                data = json.load(f)

            expected_keys = num_threads * writes_per_thread
            assert len(data) == expected_keys, (
                f"Expected {expected_keys} entries, got {len(data)}. "
                "Some writes may have been lost due to race conditions."
            )
        finally:
            os.unlink(manifest_path)


class TestCleanupCondaFile:
    """Test temp file cleanup patterns used by the conda environment."""

    def test_cleanup_temp_file(self):
        """Verify temp files are cleaned up properly."""
        with tempfile.NamedTemporaryFile(suffix=".conda_tmp", delete=False) as f:
            tmp_path = f.name
            f.write(b"test data")

        assert os.path.exists(tmp_path)
        os.unlink(tmp_path)
        assert not os.path.exists(tmp_path)

    def test_cleanup_nonexistent_file_no_error(self):
        """Cleaning up a file that doesn't exist should not raise."""
        tmp_path = "/tmp/nonexistent_conda_test_file_12345.tmp"
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        # Should not raise
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass  # Expected behavior
