#!/usr/bin/env python3
"""
Microbenchmark for content_addressed_store compression and hashing.

Compares gzip (current) vs lz4 (proposed) at various blob sizes.
Also benchmarks SHA1 vs xxhash and zlib as reference points.

Usage (from repo root):
    pip install -e .
    pip install lz4        # required for lz4 benchmarks
    pip install xxhash     # optional, for hash comparison
    python benchmarks/bench_cas.py
"""
import gzip
import hashlib
import json
import time
from io import BytesIO

BLOB_SIZES = {
    "1KB": 1_000,
    "10KB": 10_000,
    "100KB": 100_000,
    "1MB": 1_000_000,
    "10MB": 10_000_000,
}

ITERATIONS = {
    "1KB": 5000,
    "10KB": 2000,
    "100KB": 500,
    "1MB": 50,
    "10MB": 10,
}


def make_blob(size):
    """Deterministic blob with repeating byte pattern (compressible, like pickled data)."""
    pattern = bytes(range(256)) * (size // 256 + 1)
    return pattern[:size]


def bench_sha1(blob, iterations):
    start = time.perf_counter()
    for _ in range(iterations):
        hashlib.sha1(blob).hexdigest()
    elapsed = time.perf_counter() - start
    return elapsed / iterations


def bench_gzip_compress(blob, iterations, level=3):
    start = time.perf_counter()
    for _ in range(iterations):
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level) as f:
            f.write(blob)
        buf.seek(0)
        _ = buf.read()
    elapsed = time.perf_counter() - start
    return elapsed / iterations


def bench_gzip_decompress(compressed, iterations):
    start = time.perf_counter()
    for _ in range(iterations):
        with gzip.GzipFile(fileobj=BytesIO(compressed), mode="rb") as f:
            f.read()
    elapsed = time.perf_counter() - start
    return elapsed / iterations


def bench_zlib_compress(blob, iterations, level=3):
    import zlib

    start = time.perf_counter()
    for _ in range(iterations):
        zlib.compress(blob, level)
    elapsed = time.perf_counter() - start
    return elapsed / iterations


def bench_zlib_decompress(compressed, iterations):
    import zlib

    start = time.perf_counter()
    for _ in range(iterations):
        zlib.decompress(compressed)
    elapsed = time.perf_counter() - start
    return elapsed / iterations


def main():
    import zlib

    results = {}

    for label, size in BLOB_SIZES.items():
        iters = ITERATIONS[label]
        blob = make_blob(size)
        print(f"\n=== {label} blob ({len(blob)} bytes, {iters} iterations) ===")

        sha1_time = bench_sha1(blob, iters)
        print(f"  SHA1:              {sha1_time*1000:.3f} ms/op")

        gzip_c_time = bench_gzip_compress(blob, iters, level=3)
        print(f"  gzip compress L3:  {gzip_c_time*1000:.3f} ms/op")

        gzip_c1_time = bench_gzip_compress(blob, iters, level=1)
        print(f"  gzip compress L1:  {gzip_c1_time*1000:.3f} ms/op")

        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=3) as f:
            f.write(blob)
        compressed = buf.getvalue()
        ratio = len(compressed) / len(blob)
        print(f"  compression ratio: {ratio:.3f} ({len(compressed)} bytes)")

        gzip_d_time = bench_gzip_decompress(compressed, iters)
        print(f"  gzip decompress:   {gzip_d_time*1000:.3f} ms/op")

        zlib_c_time = bench_zlib_compress(blob, iters, level=3)
        print(f"  zlib compress L3:  {zlib_c_time*1000:.3f} ms/op")

        zlib_compressed = zlib.compress(blob, 3)
        zlib_d_time = bench_zlib_decompress(zlib_compressed, iters)
        print(f"  zlib decompress:   {zlib_d_time*1000:.3f} ms/op")

        results[label] = {
            "blob_bytes": len(blob),
            "iterations": iters,
            "sha1_ms": round(sha1_time * 1000, 4),
            "gzip_compress_L3_ms": round(gzip_c_time * 1000, 4),
            "gzip_compress_L1_ms": round(gzip_c1_time * 1000, 4),
            "gzip_decompress_ms": round(gzip_d_time * 1000, 4),
            "gzip_compressed_bytes": len(compressed),
            "gzip_ratio": round(ratio, 4),
            "zlib_compress_L3_ms": round(zlib_c_time * 1000, 4),
            "zlib_decompress_ms": round(zlib_d_time * 1000, 4),
        }

    try:
        import xxhash

        print("\n=== xxhash available ===")
        for label, size in BLOB_SIZES.items():
            iters = ITERATIONS[label]
            blob = make_blob(size)
            start = time.perf_counter()
            for _ in range(iters):
                xxhash.xxh64(blob).hexdigest()
            elapsed = time.perf_counter() - start
            xxh_time = elapsed / iters
            results[label]["xxh64_ms"] = round(xxh_time * 1000, 4)
            sha1_ms = results[label]["sha1_ms"]
            speedup = sha1_ms / (xxh_time * 1000) * 1000
            print(
                f"  {label}: xxh64={xxh_time*1000:.3f} ms vs sha1={sha1_ms:.3f} ms ({speedup:.1f}x faster)"
            )
    except ImportError:
        print("\n  xxhash not installed, skipping")

    try:
        import lz4.frame

        print("\n=== lz4 available ===")
        for label, size in BLOB_SIZES.items():
            iters = ITERATIONS[label]
            blob = make_blob(size)
            start = time.perf_counter()
            for _ in range(iters):
                lz4.frame.compress(blob)
            elapsed = time.perf_counter() - start
            lz4_c_time = elapsed / iters

            lz4_compressed = lz4.frame.compress(blob)
            start = time.perf_counter()
            for _ in range(iters):
                lz4.frame.decompress(lz4_compressed)
            elapsed = time.perf_counter() - start
            lz4_d_time = elapsed / iters

            lz4_ratio = len(lz4_compressed) / len(blob)
            results[label]["lz4_compress_ms"] = round(lz4_c_time * 1000, 4)
            results[label]["lz4_decompress_ms"] = round(lz4_d_time * 1000, 4)
            results[label]["lz4_ratio"] = round(lz4_ratio, 4)
            gzip_ms = results[label]["gzip_compress_L3_ms"]
            print(
                f"  {label}: lz4={lz4_c_time*1000:.3f} ms vs gzip={gzip_ms:.3f} ms "
                f"(ratio: lz4={lz4_ratio:.3f} vs gzip={results[label]['gzip_ratio']:.3f})"
            )
    except ImportError:
        print("\n  lz4 not installed, skipping")

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
