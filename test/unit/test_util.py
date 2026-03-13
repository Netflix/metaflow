from metaflow.util import compress_list, decompress_list

def test_compress_decompress_empty_list():
    # round-trip must not raise and must return []
    assert decompress_list(compress_list([])) == []

def test_decompress_empty_string():
    # direct empty-string input
    assert decompress_list("") == []

def test_compress_decompress_single_element():
    lst = ["abc"]
    assert decompress_list(compress_list(lst)) == lst

def test_compress_decompress_plain_csv():
    lst = ["a", "b", "c"]
    assert decompress_list(compress_list(lst)) == lst

def test_compress_decompress_prefix_encoded():
    # Test with a longer list that triggers prefix encoding if applicable
    # or just test with a manual prefix string
    # Prefix encoding (Mode 2)
    lst = ["test_1", "test_2", "test_3"]
    # compress_list should produce a prefix-encoded string
    compressed = compress_list(lst)
    assert ":" in compressed
    assert decompress_list(compressed) == lst

def test_compress_decompress_zlib():
    # Test with a very long list to trigger zlib compression (Mode 3)
    lst = [str(i) for i in range(1000)]
    compressed = compress_list(lst)
    assert compressed.startswith("!")
    assert decompress_list(compressed) == lst

def test_compress_empty_string_element_ambiguity():
    # Document the current behavior/limitation:
    # both [] and [""] compress to ""
    assert compress_list([]) == ""
    assert compress_list([""]) == ""
    # decompressing "" returns []
    assert decompress_list("") == []
