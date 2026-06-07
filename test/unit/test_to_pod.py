"""Tests for metaflow.util.to_pod.

Ensures POD conversion handles all the types we expect, including callables
(used by DAGNode.node_info serialization for extensions like FunctionSpec).
"""

from metaflow.util import to_pod


def _top_level_fn():
    pass


class _Wrapper:
    @staticmethod
    def static_method():
        pass

    def instance_method(self):
        pass


def test_to_pod_primitives():
    assert to_pod("abc") == "abc"
    assert to_pod(42) == 42
    assert to_pod(3.14) == 3.14


def test_to_pod_list_set_tuple():
    assert to_pod([1, 2, 3]) == [1, 2, 3]
    assert sorted(to_pod({1, 2, 3})) == [1, 2, 3]
    assert to_pod((1, 2, 3)) == [1, 2, 3]


def test_to_pod_dict():
    assert to_pod({"a": 1, "b": 2}) == {"a": 1, "b": 2}


def test_to_pod_nested():
    value = {"outer": [{"inner": (1, 2)}], "other": {"k": "v"}}
    assert to_pod(value) == {
        "outer": [{"inner": [1, 2]}],
        "other": {"k": "v"},
    }


def test_to_pod_callable_uses_qualname():
    """Callables serialize to their __qualname__ for _graph_info persistence."""
    result = to_pod(_top_level_fn)
    assert result == "_top_level_fn"


def test_to_pod_callable_in_dict():
    """Simulates DAGNode.node_info with function references (FunctionSpec use case)."""
    result = to_pod({"init_func": _top_level_fn, "call_func": _Wrapper.static_method})
    assert result["init_func"] == "_top_level_fn"
    assert result["call_func"] == "_Wrapper.static_method"


def test_to_pod_lambda_uses_qualname():
    """Lambdas have __qualname__ like 'test_to_pod_lambda_uses_qualname.<locals>.<lambda>'."""
    fn = lambda x: x  # noqa: E731
    result = to_pod(fn)
    assert "<lambda>" in result
