import pytest

from metaflow.multicore_utils import MulticoreException, parallel_imap_unordered, parallel_map


def test_parallel_map_basic():
    assert parallel_map(lambda s: s.upper(), ["a", "b", "c", "d", "e", "f"]) == [
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
    ]


def test_parallel_map_preserves_order():
    result = parallel_map(lambda x: x * 2, range(10))
    assert result == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]


def test_parallel_map_empty_input():
    assert parallel_map(lambda x: x, []) == []


def test_parallel_map_single_element():
    assert parallel_map(lambda x: x + 1, [41]) == [42]


def test_parallel_map_with_max_parallel():
    result = parallel_map(lambda x: x**2, range(5), max_parallel=2)
    assert result == [0, 1, 4, 9, 16]


def test_parallel_map_with_large_dataset():
    result = parallel_map(lambda x: x * 3, range(100))
    assert result == [x * 3 for x in range(100)]


def test_parallel_imap_unordered_basic():
    results = list(parallel_imap_unordered(lambda x: x * 2, range(4)))
    assert sorted(results) == [0, 2, 4, 6]


def test_parallel_imap_unordered_empty():
    assert list(parallel_imap_unordered(lambda x: x, [])) == []


def test_parallel_imap_unordered_with_max_parallel():
    results = list(parallel_imap_unordered(lambda x: x + 1, range(3), max_parallel=1))
    assert sorted(results) == [1, 2, 3]


def test_parallel_map_raises_on_child_failure():
    def failing_func(x):
        if x == 2:
            raise ValueError("Child process failure")
        return x

    with pytest.raises(MulticoreException, match="Child failed"):
        parallel_map(failing_func, [1, 2, 3])


def test_parallel_map_returns_complex_objects():
    def make_dict(x):
        return {"key": x, "nested": {"value": x * 2}}

    result = parallel_map(make_dict, [1, 2, 3])
    assert result == [
        {"key": 1, "nested": {"value": 2}},
        {"key": 2, "nested": {"value": 4}},
        {"key": 3, "nested": {"value": 6}},
    ]


def test_parallel_map_with_named_function():
    def square(x):
        return x * x

    result = parallel_map(square, [1, 2, 3, 4, 5])
    assert result == [1, 4, 9, 16, 25]
