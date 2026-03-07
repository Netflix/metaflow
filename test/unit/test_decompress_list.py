"""
Unit tests for metaflow.util.decompress_list

Covers:
- Normal round-trip with compress_list (regression guard)
- Empty string input
- Multi-colon inputs (e.g. "1::3")
- Leading / trailing rangedelim
- Leading / trailing separator
- zlib-compressed round-trip
"""

import pytest

from metaflow.util import compress_list, decompress_list


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def roundtrip(lst):
    """Compress then decompress a list and return the result."""
    return decompress_list(compress_list(lst))


# ---------------------------------------------------------------------------
# Regression tests – existing valid behaviour must not change
# ---------------------------------------------------------------------------


class TestNormalBehaviour:
    def test_plain_list(self):
        """Mode 1: plain comma-separated list."""
        assert decompress_list("a,b,c") == ["a", "b", "c"]

    def test_prefix_suffixes(self):
        """Mode 2: prefix + comma-separated suffixes."""
        assert decompress_list("prefix:s1,s2") == ["prefixs1", "prefixs2"]

    def test_single_element_plain(self):
        assert decompress_list("abc") == ["abc"]

    def test_single_element_prefix(self):
        # "1:3" → prefix="1", suffixes="3" → ["13"]
        assert decompress_list("1:3") == ["13"]

    def test_roundtrip_short(self):
        lst = ["abc/1/start/1", "abc/1/start/2", "abc/1/start/3"]
        assert roundtrip(lst) == lst

    def test_roundtrip_long_uses_zlib(self):
        """Lists long enough to trigger zlib compression must still roundtrip."""
        lst = ["abc/1/start/%d" % i for i in range(200)]
        assert roundtrip(lst) == lst


# ---------------------------------------------------------------------------
# Bug fixes – edge cases that previously crashed or returned wrong results
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_string_returns_empty_list(self):
        """Bug 1 – empty string must not raise IndexError."""
        assert decompress_list("") == []

    def test_multi_colon_does_not_raise(self):
        """Bug 2 – multiple colons must not raise ValueError (too many values to unpack).

        With split(':', 1): prefix="1", suffixes=":3"
        suffixes.split(',') → [":3"]  → result ["1:3"]
        (The trailing colon-3 is kept verbatim as part of the suffix block.)
        """
        result = decompress_list("1::3")
        # Must not raise; the result should be a list
        assert isinstance(result, list)
        # prefix="1", suffixes=":3" → no empty suffix → ["1" + ":3"] = ["1:3"]
        assert result == ["1:3"]

    def test_leading_rangedelim(self):
        """Leading colon: prefix="" – suffixes expand with empty prefix."""
        # ":s1,s2" → prefix="", suffixes="s1,s2" → ["s1", "s2"]
        assert decompress_list(":s1,s2") == ["s1", "s2"]

    def test_trailing_rangedelim_produces_prefix_only(self):
        """Trailing colon: prefix is non-empty, suffix is empty string.

        With the fixed split(rangedelim, 1):
          prefix="prefix", suffixes=""
          suffixes.split(",") -> [""]
          filter: '' or 'prefix' -> truthy -> kept
          result: ["prefix" + ""] = ["prefix"]

        This is semantically correct: a trailing colon with an empty suffix
        expands to just the prefix.
        """
        assert decompress_list("prefix:") == ["prefix"]

    def test_leading_separator_plain_list(self):
        """Leading comma in mode-1 – empty leading token must be dropped."""
        assert decompress_list(",a,b") == ["a", "b"]

    def test_trailing_separator_plain_list(self):
        """Trailing comma in mode-1 – empty trailing token must be dropped."""
        assert decompress_list("a,b,") == ["a", "b"]

    def test_only_separator_returns_empty_list(self):
        """Input is just the separator character."""
        assert decompress_list(",") == []

    def test_only_rangedelim_returns_empty_list(self):
        """Input is just the range delimiter."""
        # ":" → prefix="", suffixes="" → filtered → []
        assert decompress_list(":") == []
