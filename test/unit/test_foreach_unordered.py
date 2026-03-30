import pytest
from metaflow.flowspec import InvalidNextException


def validate_foreach_type(foreach_iter, foreach, step):
    """Mirrors _validate_foreach_type logic for isolated testing."""
    if isinstance(foreach_iter, (set, frozenset)):
        msg = (
            "Foreach variable *self.{var}* in step *{step}* "
            "is a {typ}, which has no guaranteed iteration order. "
            "This can cause tasks to run on wrong inputs or some inputs "
            "to be skipped entirely. "
            "Wrap it in list() first, e.g.: self.{var} = list(self.{var})".format(
                step=step, var=foreach, typ=type(foreach_iter).__name__
            )
        )
        raise InvalidNextException(msg)


def test_set_raises():
    """foreach with a set should raise InvalidNextException mentioning 'set'."""
    with pytest.raises(InvalidNextException, match="set"):
        validate_foreach_type({"a", "b", "c"}, "items", "my_step")


def test_frozenset_raises():
    """foreach with a frozenset should raise InvalidNextException mentioning 'frozenset'."""
    with pytest.raises(InvalidNextException, match="frozenset"):
        validate_foreach_type(frozenset(["x", "y"]), "items", "my_step")


def test_error_message_contains_fix_hint():
    """Error message should tell user to wrap in list()."""
    with pytest.raises(InvalidNextException, match=r"list\(\)"):
        validate_foreach_type({"a", "b"}, "items", "my_step")


def test_list_does_not_raise():
    """foreach with a list should not raise."""
    validate_foreach_type(["a", "b", "c"], "items", "my_step")


def test_tuple_does_not_raise():
    """foreach with a tuple should not raise."""
    validate_foreach_type(("a", "b", "c"), "items", "my_step")
