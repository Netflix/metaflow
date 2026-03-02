from metaflow.util import decompress_list


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
