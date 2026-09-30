"""Tests for compress_list / decompress_list round-trip correctness.

Regression test for https://github.com/Netflix/metaflow/issues/3017:
decompress_list("") must return [] instead of raising IndexError.
"""

import pytest

from metaflow.util import compress_list, decompress_list


def test_compress_empty_list():
    assert compress_list([]) == ""


def test_decompress_empty_string():
    """The bug: lststr[0] crashes on empty string."""
    assert decompress_list("") == []


def test_roundtrip_empty():
    assert decompress_list(compress_list([])) == []


@pytest.mark.parametrize(
    "items",
    [
        pytest.param(["x"], id="single"),
        pytest.param(["a", "b", "c"], id="simple"),
        pytest.param(
            ["run/step/task/1", "run/step/task/2", "run/step/task/3"],
            id="common-prefix",
        ),
    ],
)
def test_roundtrip(items):
    assert decompress_list(compress_list(items)) == items


def test_roundtrip_zlib():
    """Force the zlib-compressed path by setting zlibmin=0."""
    items = ["a", "b"]
    compressed = compress_list(items, zlibmin=0)
    assert compressed.startswith("!")
    assert decompress_list(compressed) == items
