"""
Unit tests for compress_list / decompress_list in metaflow/util.py

Bug: decompress_list("") raised IndexError on lststr[0] because there
was no guard for an empty input string.  compress_list([]) legitimately
produces "" (joining an empty list), so the round-trip
    decompress_list(compress_list([]))
crashed instead of returning [].

Fix: add `if not lststr: return []` before the index access.
"""

import sys
from unittest.mock import MagicMock

# fcntl is Linux/macOS-only.  Stub it out so the test runs on Windows too.
if "fcntl" not in sys.modules:
    sys.modules["fcntl"] = MagicMock()
if "metaflow.sidecar.sidecar_subprocess" not in sys.modules:
    sys.modules["metaflow.sidecar.sidecar_subprocess"] = MagicMock()

from metaflow.util import compress_list, decompress_list  # noqa: E402

# ---------------------------------------------------------------------------
# Core regression tests
# ---------------------------------------------------------------------------


def test_compress_empty_list_returns_empty_string():
    """compress_list([]) must produce '' (join of zero elements)."""
    assert compress_list([]) == ""


def test_decompress_empty_string_returns_empty_list():
    """
    decompress_list("") must return [] without raising.

    This is the direct crash case: before the fix, lststr[0] on an
    empty string raised IndexError: string index out of range.
    Fails without the fix, passes with it.
    """
    result = decompress_list("")
    assert result == []


def test_roundtrip_empty_list():
    """
    decompress_list(compress_list([])) must return [].

    This is the end-to-end round-trip that exposed the bug: the output
    of compress_list is fed straight into decompress_list, so an empty
    list must survive the round-trip unharmed.
    Fails without the fix, passes with it.
    """
    assert decompress_list(compress_list([])) == []


# ---------------------------------------------------------------------------
# Regression guard: existing non-empty round-trips must still work
# ---------------------------------------------------------------------------


def test_roundtrip_single_item():
    """A single-element list survives the round-trip."""
    items = ["abc"]
    assert decompress_list(compress_list(items)) == items


def test_roundtrip_multiple_items_with_common_prefix():
    """
    Lists with a long common prefix use the prefix-encoding path;
    verify the round-trip is correct after the guard was added.
    """
    items = ["MyFlow/1234/start/1", "MyFlow/1234/start/2", "MyFlow/1234/start/3"]
    assert decompress_list(compress_list(items)) == items


def test_roundtrip_items_without_common_prefix():
    """Lists with no common prefix use the plain comma-separated path."""
    items = ["alpha", "beta", "gamma"]
    assert decompress_list(compress_list(items)) == items
