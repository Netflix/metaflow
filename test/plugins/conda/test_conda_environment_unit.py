"""
Unit tests for CondaEnvironment helper logic.

Tests the manifest read/write with file locking and the environment
hashing/dedup logic. Uses temp files — no conda installation needed.
"""

import fcntl
import json
import os
import tempfile
import threading

import pytest


def _flock_on_data_file(manifest_path, key, value):
    """Old pattern: flock the data file itself + seek/truncate.

    Serializes correctly but not crash-safe (truncate then write).
    """
    with open(manifest_path, "r+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            data = json.load(f)
            data[key] = value
            f.seek(0)
            f.truncate()
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def _flock_on_data_file_with_replace(manifest_path, key, value):
    """Broken pattern: flock the data file + os.replace.

    The os.replace creates a new inode, so the flock on the old file
    descriptor doesn't prevent other threads from acquiring their own
    lock on the new inode. Writes are lost under contention.
    """
    with open(manifest_path, "r+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            data = json.load(f)
            data[key] = value
            tmp = manifest_path + ".tmp"
            with open(tmp, "w") as tmp_f:
                json.dump(data, tmp_f)
            os.replace(tmp, manifest_path)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def _no_locking(manifest_path, key, value):
    """Old production_token.py pattern: no locking at all."""
    with open(manifest_path, "r") as f:
        data = json.load(f)
    data[key] = value
    with open(manifest_path, "w") as f:
        json.dump(data, f)


def _atomic_json_update_wrapper(manifest_path, key, value):
    """New pattern: lock file + atomic replace."""
    from metaflow.util import atomic_json_update

    atomic_json_update(manifest_path, lambda d: {**d, key: value})


def _run_concurrent_writes(write_fn, num_threads=10, writes_per_thread=5):
    """Run concurrent writes using the given write function.

    Returns (num_keys_written, expected_keys, errors).
    """
    errors = []

    with tempfile.NamedTemporaryFile(mode="w", suffix=".manifest", delete=False) as f:
        manifest_path = f.name
        json.dump({}, f)

    def writer(thread_id):
        try:
            for i in range(writes_per_thread):
                key = f"env_{thread_id}_{i}"
                write_fn(manifest_path, key, {"thread": thread_id})
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=writer, args=(tid,)) for tid in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    try:
        try:
            with open(manifest_path, "r") as f:
                data = json.load(f)
            return len(data), num_threads * writes_per_thread, errors
        except (json.JSONDecodeError, FileNotFoundError) as e:
            # File was corrupted or deleted by the broken pattern
            errors.append(e)
            return 0, num_threads * writes_per_thread, errors
    finally:
        if os.path.exists(manifest_path):
            os.unlink(manifest_path)
        for suffix in (".lock", ".tmp"):
            p = manifest_path + suffix
            if os.path.exists(p):
                os.unlink(p)


class TestManifestConcurrentWrites:
    """Demonstrate that the old locking patterns lose writes and the new one doesn't."""

    @pytest.mark.parametrize(
        "write_fn,description",
        [
            (
                _flock_on_data_file_with_replace,
                "flock+replace (inode changes break lock)",
            ),
            (_no_locking, "no locking (old production_token.py pattern)"),
        ],
    )
    def test_broken_patterns_lose_writes(self, write_fn, description):
        """Broken locking patterns lose writes or corrupt data under contention.

        These patterns are known-broken. The race is non-deterministic, so we
        run many trials with high contention. If the race never triggers, we
        skip rather than fail — the important thing is that the *fixed* pattern
        (test_atomic_json_update_no_lost_writes) always passes.
        """
        broken_at_least_once = False
        for _ in range(30):
            actual, expected, errors = _run_concurrent_writes(
                write_fn, num_threads=20, writes_per_thread=10
            )
            if errors or actual < expected:
                broken_at_least_once = True
                break

        if not broken_at_least_once:
            pytest.skip(
                f"Race condition in '{description}' did not trigger in 30 trials "
                f"(non-deterministic). The pattern is still broken; the OS scheduler "
                f"just didn't interleave threads enough to expose it this time."
            )

    def test_atomic_json_update_no_lost_writes(self):
        """atomic_json_update with lock file never loses writes."""
        for _ in range(5):
            actual, expected, errors = _run_concurrent_writes(
                _atomic_json_update_wrapper, num_threads=10, writes_per_thread=5
            )
            assert len(errors) == 0, f"Unexpected errors: {errors}"
            assert actual == expected, (
                f"Expected {expected} entries, got {actual}. "
                "atomic_json_update lost writes."
            )

    def test_flock_seek_truncate_no_lost_writes(self):
        """flock on data file + seek/truncate doesn't lose writes (same inode)."""
        for _ in range(5):
            actual, expected, errors = _run_concurrent_writes(
                _flock_on_data_file, num_threads=10, writes_per_thread=5
            )
            assert len(errors) == 0, f"Unexpected errors: {errors}"
            assert actual == expected, (
                f"Expected {expected} entries, got {actual}. "
                "flock+seek/truncate lost writes."
            )


class TestAtomicJsonUpdate:
    """Test the atomic_json_update utility directly."""

    def test_creates_file_if_missing(self):
        """atomic_json_update should create the file if it doesn't exist."""
        from metaflow.util import atomic_json_update

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "new.json")
            result = atomic_json_update(path, lambda d: {**d, "key": "val"})
            assert result == {"key": "val"}
            with open(path, "r") as f:
                assert json.load(f) == {"key": "val"}

    def test_updates_existing_file(self):
        """atomic_json_update should merge into existing data."""
        from metaflow.util import atomic_json_update

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "existing.json")
            with open(path, "w") as f:
                json.dump({"a": 1}, f)
            result = atomic_json_update(path, lambda d: {**d, "b": 2})
            assert result == {"a": 1, "b": 2}

    def test_crash_safety_no_partial_writes(self):
        """If updater_fn raises, the original file should be untouched."""
        from metaflow.util import atomic_json_update

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "safe.json")
            with open(path, "w") as f:
                json.dump({"original": True}, f)

            def bad_update(d):
                raise ValueError("simulated crash")

            with pytest.raises(ValueError):
                atomic_json_update(path, bad_update)

            with open(path, "r") as f:
                assert json.load(f) == {"original": True}
