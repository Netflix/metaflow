#!/usr/bin/env python3
"""
End-to-end benchmark of ContentAddressedStore save_blobs / load_blobs
using the local filesystem storage backend.

Tests the full pipeline: SHA1 hash -> dedup check -> compress -> write -> read -> decompress.

Usage (from repo root):
    pip install -e .
    python benchmarks/bench_cas_e2e.py
"""
import json
import os
import shutil
import tempfile
import time

from metaflow.datastore.content_addressed_store import ContentAddressedStore
from metaflow.plugins.datastores.local_storage import LocalStorage

BLOB_SIZES = {
    "1KB": 1_000,
    "100KB": 100_000,
    "1MB": 1_000_000,
    "10MB": 10_000_000,
}

ITERATIONS = {
    "1KB": 200,
    "100KB": 100,
    "1MB": 20,
    "10MB": 5,
}


def make_blob(size):
    """Deterministic blob with repeating byte pattern (compressible, like pickled data)."""
    pattern = bytes(range(256)) * (size // 256 + 1)
    return pattern[:size]


def main():
    results = {}

    for label, size in BLOB_SIZES.items():
        iters = ITERATIONS[label]
        blob = make_blob(size)
        print(f"\n=== {label} blob ({len(blob)} bytes, {iters} iterations) ===")

        save_times = []
        load_times = []

        for i in range(iters):
            tmpdir = tempfile.mkdtemp(prefix="cas_bench_")
            try:
                storage = LocalStorage(tmpdir)
                cas = ContentAddressedStore("cas", storage)

                # Append index to avoid dedup short-circuit
                unique_blob = blob + i.to_bytes(4, "big")

                # Save
                start = time.perf_counter()
                result = cas.save_blobs(iter([unique_blob]))
                save_elapsed = time.perf_counter() - start
                save_times.append(save_elapsed)

                key = result[0].key

                # Load
                start = time.perf_counter()
                loaded = list(cas.load_blobs([key]))
                load_elapsed = time.perf_counter() - start
                load_times.append(load_elapsed)

                # Verify round-trip correctness
                assert loaded[0][1] == unique_blob, "Data mismatch!"
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)

        avg_save = sum(save_times) / len(save_times)
        avg_load = sum(load_times) / len(load_times)
        print(f"  save: {avg_save*1000:.3f} ms/op")
        print(f"  load: {avg_load*1000:.3f} ms/op")
        print(f"  total: {(avg_save+avg_load)*1000:.3f} ms/op")

        results[label] = {
            "blob_bytes": len(blob),
            "iterations": iters,
            "save_ms": round(avg_save * 1000, 4),
            "load_ms": round(avg_load * 1000, 4),
            "total_ms": round((avg_save + avg_load) * 1000, 4),
        }

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
