#!/usr/bin/env python3
"""
End-to-end CAS benchmark with realistic metaflow artifact payloads.

Tests pickled Python objects that resemble actual ML/data science artifacts:
- Small dict (step metadata, config) ~233B pickled
- Medium dict (feature stats, metrics) ~52KB pickled
- Numpy float64 array (embeddings, features) ~800KB pickled
- Large numpy array (model weights) ~8MB pickled
- Random bytes (opaque model blob) ~5MB pickled

Usage (from repo root):
    pip install -e .
    pip install numpy
    python benchmarks/bench_cas_realistic.py
"""
import json
import pickle
import shutil
import tempfile
import time

import numpy as np

from metaflow.datastore.content_addressed_store import ContentAddressedStore
from metaflow.plugins.datastores.local_storage import LocalStorage


def make_payloads():
    rng = np.random.default_rng(42)
    payloads = {}

    # Small dict -- step metadata, configs, parameters
    payloads["small_dict (1KB)"] = pickle.dumps({
        "learning_rate": 0.001,
        "batch_size": 32,
        "epochs": 100,
        "model_name": "transformer_v2",
        "metrics": {"accuracy": 0.934, "loss": 0.187, "f1": 0.921},
        "tags": ["production", "v2.1", "gpu"],
        "timestamp": "2026-04-10T12:00:00Z",
    })

    # Medium dict -- feature statistics, aggregated metrics
    payloads["metrics_dict (50KB)"] = pickle.dumps({
        f"feature_{i}": {
            "mean": float(rng.normal()),
            "std": float(rng.exponential()),
            "min": float(rng.normal() - 3),
            "max": float(rng.normal() + 3),
            "nulls": int(rng.integers(0, 100)),
            "histogram": rng.normal(size=50).tolist(),
        }
        for i in range(100)
    })

    # Numpy float64 array -- embeddings, feature vectors
    payloads["numpy_f64 (800KB)"] = pickle.dumps(
        rng.standard_normal((100, 1000), dtype=np.float64)
    )

    # Large numpy array -- model weights, wide dataframe
    payloads["numpy_large (8MB)"] = pickle.dumps(
        rng.standard_normal((1000, 1000), dtype=np.float64)
    )

    # Random bytes -- simulates opaque model blob (e.g. ONNX, SavedModel)
    payloads["random_bytes (5MB)"] = pickle.dumps(
        rng.bytes(5_000_000)
    )

    return payloads


def bench_cas(payloads, iterations_map):
    results = {}

    for label, blob in payloads.items():
        iters = iterations_map.get(label, 10)
        print(f"\n=== {label} ({len(blob)} bytes pickled, {iters} iterations) ===")

        save_times = []
        load_times = []

        for i in range(iters):
            tmpdir = tempfile.mkdtemp(prefix="cas_bench_")
            try:
                storage = LocalStorage(tmpdir)
                cas = ContentAddressedStore("cas", storage)

                # Append index to avoid dedup
                unique_blob = blob + i.to_bytes(4, "big")

                start = time.perf_counter()
                result = cas.save_blobs(iter([unique_blob]))
                save_elapsed = time.perf_counter() - start
                save_times.append(save_elapsed)

                key = result[0].key

                start = time.perf_counter()
                loaded = list(cas.load_blobs([key]))
                load_elapsed = time.perf_counter() - start
                load_times.append(load_elapsed)

                assert loaded[0][1] == unique_blob, "Data mismatch!"
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)

        avg_save = sum(save_times) / len(save_times)
        avg_load = sum(load_times) / len(load_times)
        print(f"  pickled size: {len(blob)} bytes")
        print(f"  save: {avg_save*1000:.3f} ms/op")
        print(f"  load: {avg_load*1000:.3f} ms/op")
        print(f"  total: {(avg_save+avg_load)*1000:.3f} ms/op")

        results[label] = {
            "pickled_bytes": len(blob),
            "iterations": iters,
            "save_ms": round(avg_save * 1000, 4),
            "load_ms": round(avg_load * 1000, 4),
            "total_ms": round((avg_save + avg_load) * 1000, 4),
        }

    return results


def main():
    payloads = make_payloads()

    iterations_map = {
        "small_dict (1KB)": 200,
        "metrics_dict (50KB)": 100,
        "numpy_f64 (800KB)": 50,
        "numpy_large (8MB)": 10,
        "random_bytes (5MB)": 10,
    }

    results = bench_cas(payloads, iterations_map)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
