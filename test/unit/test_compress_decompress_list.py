from metaflow.util import compress_list, decompress_list
from metaflow.util import compress_list, decompress_list


def test_compress_empty_list():
    """compress_list([]) should return empty string"""
    result = compress_list([])
    assert result == ""


def test_decompress_empty_string():
    """decompress_list("") should return empty list - this tests the fix for issue #3017"""
    result = decompress_list("")
    assert result == []


def test_roundtrip_empty():
    """Round-trip: decompress(compress([])) should return [] - tests the invariant"""
    result = decompress_list(compress_list([]))
    assert result == []


def test_roundtrip_simple_list():
    """Test round-trip with simple list"""
    original = ["task1", "task2"]
    compressed = compress_list(original)
    decompressed = decompress_list(compressed)
    assert decompressed == original


def test_roundtrip_single_element():
    """Test round-trip with single element"""
    original = ["single_task"]
    compressed = compress_list(original)
    decompressed = decompress_list(compressed)
    assert decompressed == original
