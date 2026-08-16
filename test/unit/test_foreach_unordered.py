import pytest
from metaflow.flowspec import FlowSpec, InvalidNextException


class _FakeFlowSpec(FlowSpec):
    pass


def make_flow():
    flow = _FakeFlowSpec.__new__(_FakeFlowSpec)
    object.__setattr__(flow, "_datastore", None)
    return flow


def test_set_raises():
    """foreach with a set should raise InvalidNextException mentioning 'set'."""
    flow = make_flow()
    with pytest.raises(InvalidNextException, match="set"):
        flow._validate_foreach_type({"a", "b", "c"}, "items", "my_step")


def test_frozenset_raises():
    """foreach with a frozenset should raise InvalidNextException mentioning 'frozenset'."""
    flow = make_flow()
    with pytest.raises(InvalidNextException, match="frozenset"):
        flow._validate_foreach_type(frozenset(["x", "y"]), "items", "my_step")


def test_error_message_contains_fix_hint():
    """Error message should tell user to wrap in list()."""
    flow = make_flow()
    with pytest.raises(InvalidNextException, match=r"list\(\)"):
        flow._validate_foreach_type({"a", "b"}, "items", "my_step")


def test_list_does_not_raise():
    """foreach with a list should not raise."""
    flow = make_flow()
    flow._validate_foreach_type(["a", "b", "c"], "items", "my_step")


def test_tuple_does_not_raise():
    """foreach with a tuple should not raise."""
    flow = make_flow()
    flow._validate_foreach_type(("a", "b", "c"), "items", "my_step")
