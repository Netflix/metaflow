from metaflow.util import compress_list, decompress_list


def test_decompress_list_returns_empty_for_empty_input():
    assert decompress_list("") == []


def test_decompress_list_handles_multi_colon_prefix():
    assert decompress_list("job:task::A,B") == ["job:task:A", "job:task:B"]


def test_decompress_list_with_step_functions_style_input_paths():
    compressed = "sfn-manual__2022-03-15T01:26:41:/step_a/task1,/step_b/task2"
    assert decompress_list(compressed) == [
        "sfn-manual__2022-03-15T01:26:41/step_a/task1",
        "sfn-manual__2022-03-15T01:26:41/step_b/task2",
    ]


def test_compress_decompress_round_trip_plain_mode():
    paths = [
        "sfn-1234567890abc/start/task_1",
        "sfn-1234567890abc/start/task_2",
        "sfn-1234567890abc/start/task_3",
    ]
    assert decompress_list(compress_list(paths)) == paths


def test_compress_decompress_round_trip_zlib_mode():
    paths = [
        "run-1234567890abcdef/very_long_step_name/task_%03d" % i for i in range(80)
    ]
    compressed = compress_list(paths, zlibmin=1)
    assert compressed[0] == "!"
    assert decompress_list(compressed) == paths
