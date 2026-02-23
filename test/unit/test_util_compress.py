"""
Unit tests for compress_list / decompress_list in metaflow.util.

These functions are called from every cloud orchestrator's task execution path
(step_cmd.py, argo/conditional_input_paths.py, step_functions.py) and previously
had zero test coverage.

See: https://github.com/Netflix/metaflow/issues/2908
"""

import pytest
from math import inf

from metaflow.util import compress_list, decompress_list


class TestDecompressListRoundTrip:
    """Verify compress_list -> decompress_list round-trips correctly."""

    @pytest.mark.parametrize(
        "paths",
        [
            # Simple paths with common prefix
            ["run123/step_a/task1", "run123/step_a/task2", "run123/step_a/task3"],
            # Single item
            ["run123/step_a/task1"],
            # Two items
            ["run123/step_a/task1", "run123/step_a/task2"],
            # No common prefix
            ["alpha/task1", "beta/task2", "gamma/task3"],
            # Identical prefix exhausts all characters
            ["abc", "abd"],
            # Long paths (realistic Metaflow pathspecs)
            [
                "sfn-abc123/train/task_001",
                "sfn-abc123/train/task_002",
                "sfn-abc123/train/task_003",
                "sfn-abc123/train/task_004",
                "sfn-abc123/train/task_005",
            ],
        ],
        ids=[
            "simple_common_prefix",
            "single_item",
            "two_items",
            "no_common_prefix",
            "partial_prefix",
            "long_pathspecs",
        ],
    )
    def test_round_trip(self, paths):
        compressed = compress_list(paths)
        result = decompress_list(compressed)
        assert result == paths


class TestDecompressListMultiColon:
    """
    Verify decompress_list handles strings with multiple colons.

    This is the core bug: compress_list uses ':' as rangedelim between
    the prefix and suffixes. If the prefix itself contains colons
    (e.g. Airflow run-ids), the old code crashed with ValueError.
    """

    @pytest.mark.parametrize(
        "compressed, expected",
        [
            # Single colon (normal case — always worked)
            (
                "sfn-abc123/train/:task_001,task_002,task_003",
                [
                    "sfn-abc123/train/task_001",
                    "sfn-abc123/train/task_002",
                    "sfn-abc123/train/task_003",
                ],
            ),
            # Multiple colons — Airflow-style run-id with timestamps
            (
                "sfn-manual__2022-03-15T01:26:41:/step_a/task1,/step_b/task2",
                [
                    "sfn-manual__2022-03-15T01:26:41/step_a/task1",
                    "sfn-manual__2022-03-15T01:26:41/step_b/task2",
                ],
            ),
            # Double colon — prefix ends with the delimiter char
            (
                "job:task::A,B",
                ["job:task:A", "job:task:B"],
            ),
            # Triple colon — extreme case
            (
                "a:b:c::x,y",
                ["a:b:c:x", "a:b:c:y"],
            ),
        ],
        ids=[
            "single_colon_normal",
            "airflow_timestamp_colons",
            "double_colon_prefix_ends_with_delim",
            "triple_colon_extreme",
        ],
    )
    def test_multi_colon_input(self, compressed, expected):
        result = decompress_list(compressed)
        assert result == expected


class TestDecompressListEdgeCases:
    """Edge cases and defensive behavior."""

    def test_empty_string_returns_empty_list(self):
        """Empty string should return [] instead of IndexError."""
        assert decompress_list("") == []

    def test_single_item_no_delimiter(self):
        """A single path with no colon or comma."""
        assert decompress_list("run123/step_a/task1") == ["run123/step_a/task1"]

    def test_comma_separated_no_prefix(self):
        """Plain comma-separated list without prefix encoding."""
        result = decompress_list("alpha,beta,gamma")
        assert result == ["alpha", "beta", "gamma"]


class TestCompressListValidation:
    """Verify compress_list rejects items containing delimiter characters."""

    def test_rejects_item_with_separator(self):
        with pytest.raises(Exception, match="delimiter character"):
            compress_list(["good_path", "bad,path"])

    def test_rejects_item_with_rangedelim(self):
        with pytest.raises(Exception, match="delimiter character"):
            compress_list(["good_path", "bad:path"])

    def test_rejects_item_with_zlibmarker(self):
        with pytest.raises(Exception, match="delimiter character"):
            compress_list(["good_path", "bad!path"])


class TestCompressListZlib:
    """Verify zlib compression mode for large lists."""

    def test_large_list_triggers_zlib(self):
        """Lists exceeding zlibmin should be zlib-compressed."""
        paths = ["run123/step_a/task_%04d" % i for i in range(200)]
        compressed = compress_list(paths, zlibmin=500)
        # zlib-compressed strings start with '!'
        assert compressed.startswith("!")
        # Round-trip must still work
        result = decompress_list(compressed)
        assert result == paths

    def test_zlibmin_inf_prevents_compression(self):
        """Setting zlibmin=inf should prevent zlib compression."""
        paths = ["run123/step_a/task_%04d" % i for i in range(200)]
        compressed = compress_list(paths, zlibmin=inf)
        assert not compressed.startswith("!")
        result = decompress_list(compressed)
        assert result == paths
