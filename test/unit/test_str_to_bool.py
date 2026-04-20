import pytest

from metaflow.util import str_to_bool


@pytest.mark.parametrize("v", ["1", "true", "TRUE", "Yes", "on", True])
def test_truthy(v):
    assert str_to_bool(v) is True


@pytest.mark.parametrize("v", ["0", "false", "no", "off", "", False])
def test_falsy(v):
    assert str_to_bool(v) is False


def test_none_returns_default():
    assert str_to_bool(None) is None
    assert str_to_bool(None, default=True) is True


def test_unknown_non_strict_is_truthy():
    # Preserves historical lax behavior: anything we don't recognize as falsy
    # is treated as truthy.
    assert str_to_bool("maybe") is True
    assert str_to_bool("enabled") is True


def test_unknown_strict_raises():
    with pytest.raises(ValueError):
        str_to_bool("maybe", strict=True)
