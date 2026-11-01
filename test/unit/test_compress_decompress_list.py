"""Tests for metaflow.util.compress_list and decompress_list.

Covers the bug fix for issue #3017: decompress_list("") must return []
instead of raising IndexError, preserving the round-trip invariant
decompress_list(compress_list(x)) == x for all valid x.
"""

from metaflow.util import compress_list, decompress_list


class TestCompressListEmptyInput:
    """compress_list([]) should return an empty string."""

    def test_compress_empty_list_returns_empty_string(self):
        assert compress_list([]) == ""


class TestDecompressListEmptyInput:
    """decompress_list('') must return [] (the fix for #3017)."""

    def test_decompress_empty_string_returns_empty_list(self):
        assert decompress_list("") == []

    def test_roundtrip_empty_list(self):
        assert decompress_list(compress_list([])) == []


class TestRoundTripSimpleList:
    """Round-trip through comma-separated mode (mode 1)."""

    def test_single_element(self):
        lst = ["x"]
        assert decompress_list(compress_list(lst)) == lst

    def test_multiple_elements(self):
        lst = ["a", "b", "c"]
        assert decompress_list(compress_list(lst)) == lst


class TestRoundTripPrefixEncoded:
    """Round-trip through prefix+suffix mode (mode 2, using ':' delimiter)."""

    def test_common_prefix_list(self):
        lst = ["step/1", "step/2", "step/3"]
        compressed = compress_list(lst)
        # Verify prefix encoding is actually used
        assert ":" in compressed
        assert decompress_list(compressed) == lst

    def test_long_common_prefix(self):
        lst = ["MyFlow/12345/start/100", "MyFlow/12345/start/200"]
        compressed = compress_list(lst)
        assert ":" in compressed
        assert decompress_list(compressed) == lst


class TestRoundTripZlibCompressed:
    """Round-trip through zlib-compressed mode (mode 3, '!' marker)."""

    def test_forced_zlib_compression(self):
        lst = ["item_%d" % i for i in range(20)]
        # Force zlib by setting zlibmin=0 so any non-empty result is compressed
        compressed = compress_list(lst, zlibmin=0)
        assert compressed.startswith("!")
        assert decompress_list(compressed) == lst

    def test_zlib_with_prefix_encoding(self):
        lst = ["run/step/%d" % i for i in range(50)]
        compressed = compress_list(lst, zlibmin=0)
        assert compressed.startswith("!")
        assert decompress_list(compressed) == lst


class TestCustomDelimiters:
    """Ensure custom separator/rangedelim/zlibmarker work correctly."""

    def test_custom_separator(self):
        lst = ["a", "b", "c"]
        sep = ";"
        compressed = compress_list(lst, separator=sep)
        assert decompress_list(compressed, separator=sep) == lst

    def test_custom_rangedelim(self):
        lst = ["prefix_1", "prefix_2"]
        rd = "|"
        compressed = compress_list(lst, rangedelim=rd)
        assert decompress_list(compressed, rangedelim=rd) == lst

    def test_empty_string_with_custom_delimiters(self):
        """Empty string guard must work regardless of delimiter values."""
        assert decompress_list("", separator=";", rangedelim="|", zlibmarker="#") == []
