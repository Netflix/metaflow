from metaflow.util import compress_list, decompress_list

def test_compress_decompress_empty_list():
    # round-trip must not raise and must return []
    assert decompress_list(compress_list([])) == []

def test_decompress_empty_string():
    # direct empty-string input
    assert decompress_list("") == []
