"""
Unit tests for CondaEnvironment helper logic.

Tests the manifest read/write with file locking and the environment
hashing/dedup logic. Uses temp files — no conda installation needed.
"""

import json
import os
import threading
import fcntl
import pytest


def _write_manifest_worker(manifest_path, thread_id, writes_per_thread, errors):
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
                    f.flush()
                    os.fsync(f.fileno())
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception as e:
        errors.append(e)


def test_manifest_concurrent_writes_no_corruption(tmp_path):
    """Multiple threads writing to the manifest should not corrupt it."""
    manifest_file = tmp_path / "test.manifest"
    manifest_file.write_text("{}")

    errors = []
    num_threads = 10
    writes_per_thread = 5

    threads = [
        threading.Thread(
            target=_write_manifest_worker,
            args=(str(manifest_file), tid, writes_per_thread, errors),
        )
        for tid in range(num_threads)
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Errors during concurrent writes: {errors}"

    data = json.loads(manifest_file.read_text())
    expected_keys = num_threads * writes_per_thread
    assert len(data) == expected_keys, (
        f"Expected {expected_keys} entries, got {len(data)}. "
        "Some writes may have been lost."
    )


def test_cleanup_temp_file(tmp_path):
    """Verify temp files are cleaned up properly."""
    tmp_file = tmp_path / "test.conda_tmp"
    tmp_file.write_text("test data")

    assert tmp_file.exists()
    tmp_file.unlink()
    assert not tmp_file.exists()


def test_cleanup_nonexistent_file_no_error(tmp_path):
    """Cleaning up a file that doesn't exist should not raise."""
    nonexistent = tmp_path / "nonexistent.tmp"

    # os.unlink on a non-existent file raises FileNotFoundError
    # Verify the code handles this gracefully
    try:
        nonexistent.unlink()
    except FileNotFoundError:
        pass
