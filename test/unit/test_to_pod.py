"""Tests for metaflow.util.to_pod.

Ensures POD conversion handles all the types we expect, including callables
(used by DAGNode.node_info serialization for extensions like FunctionSpec).
"""

import pytest
from metaflow.util import to_pod

# ---------------------------------------------------------------------------
# Dummy Callables for Testing
# ---------------------------------------------------------------------------


def _top_level_fn():
    pass


class _Wrapper:
    @staticmethod
    def static_method():
        pass

    def instance_method(self):
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val, expected",
    [
        ("abc", "abc"),
        (42, 42),
        (3.14, 3.14),
    ],
    ids=["string", "integer", "float"],
)
def test_to_pod_converts_primitives_unchanged(input_val, expected):
    """Test that basic primitives pass through POD conversion unchanged."""
    assert to_pod(input_val) == expected


@pytest.mark.parametrize(
    "input_val, expected",
    [
        ([1, 2, 3], [1, 2, 3]),
        ((1, 2, 3), [1, 2, 3]),
        ({1, 2, 3}, [1, 2, 3]),
    ],
    ids=["list", "tuple", "set"],
)
def test_to_pod_converts_iterable_collections_to_lists(input_val, expected):
    """Test that lists, tuples, and sets are converted to standard lists."""
    result = to_pod(input_val)

    # Sets are unordered, so we must sort the result before comparing
    if isinstance(input_val, set):
        assert sorted(result) == expected
    else:
        assert result == expected


def test_to_pod_converts_dicts_unchanged():
    """Test that simple dictionaries pass through unchanged."""
    assert to_pod({"a": 1, "b": 2}) == {"a": 1, "b": 2}


def test_to_pod_handles_nested_structures():
    """Test that to_pod recursively converts nested collections."""
    value = {"outer": [{"inner": (1, 2)}], "other": {"k": "v"}}
    expected = {
        "outer": [{"inner": [1, 2]}],
        "other": {"k": "v"},
    }
    assert to_pod(value) == expected


def test_to_pod_converts_callable_to_qualname():
    """Callables serialize to their __qualname__ for _graph_info persistence."""
    result = to_pod(_top_level_fn)
    assert result == "_top_level_fn"


def test_to_pod_converts_callables_inside_dictionaries():
    """Simulates DAGNode.node_info with function references (FunctionSpec use case)."""
    result = to_pod({"init_func": _top_level_fn, "call_func": _Wrapper.static_method})

    assert result["init_func"] == "_top_level_fn"
    assert result["call_func"] == "_Wrapper.static_method"


def test_to_pod_converts_lambda_to_qualname_string():
    """Lambdas have __qualname__ like 'test_to_pod_converts_lambda_to_qualname_string.<locals>.<lambda>'."""
    fn = lambda x: x  # noqa: E731
    result = to_pod(fn)

    assert "<lambda>" in result
