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
    """Test that concurrent manifest writes don't lose data.

    Before the fix, conda_environment.py used flock on the data file with
    seek/truncate (not crash-safe) and production_token.py had no locking
    at all. Both patterns lost writes under contention.

    After the fix, both use atomic_json_update() which serializes via a
    separate lock file and writes atomically via temp file + os.replace.
    """

    def test_concurrent_writes_no_corruption(self):
        """50 threads x 20 writes each — all 1000 writes must survive."""
        from metaflow.util import atomic_json_update

        errors = []
        num_threads = 50
        writes_per_thread = 20

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".manifest", delete=False
        ) as f:
            manifest_path = f.name
            json.dump({}, f)

        def write_to_manifest(thread_id):
            try:
                for i in range(writes_per_thread):
                    key = f"env_{thread_id}_{i}"
                    atomic_json_update(
                        manifest_path,
                        lambda d, k=key, t=thread_id: {
                            **d,
                            k: {"platform": "linux-64", "thread": t},
                        },
                    )
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
                "Writes were lost due to race conditions."
            )
        finally:
            os.unlink(manifest_path)
            lock_path = manifest_path + ".lock"
            if os.path.exists(lock_path):
                os.unlink(lock_path)


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
