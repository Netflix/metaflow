import json

import pytest

from metaflow.user_configs.config_parameters import ConfigValue

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_dict():
    """Provides a basic, flat dictionary."""
    return {"a": 1, "b": 2}


@pytest.fixture
def nested_dict():
    """Provides a complex, deeply nested dictionary containing lists and tuples."""
    return {
        "a": 1,
        "b": [1, 2, 3],
        "c": {"d": 4},
        "e": {"f": [{"g": 5}]},
        "h": ({"i": 6},),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_config_value_is_dict_instance(simple_dict):
    """Ensure ConfigValue correctly registers as a subclass/instance of dict."""
    c_value = ConfigValue(simple_dict)
    assert isinstance(c_value, dict)


def test_todict_returns_original_data(simple_dict, nested_dict):
    """Verify that to_dict() reconstructs the original standard dictionary exactly."""
    assert ConfigValue(simple_dict).to_dict() == simple_dict
    assert ConfigValue(nested_dict).to_dict() == nested_dict


def test_container_preserves_config_value_type_internally(nested_dict):
    """Verify that nested dictionaries within lists/tuples are correctly wrapped as ConfigValues."""
    c_value = ConfigValue(nested_dict)

    # Dot-notation access works deeply
    assert c_value.e.f[0].g == 5

    # Nested dicts become ConfigValues
    assert isinstance(c_value.c, ConfigValue)
    assert isinstance(c_value.e, ConfigValue)

    # Lists remain lists, but their dict elements become ConfigValues
    assert isinstance(c_value.e.f, list)
    assert isinstance(c_value.e.f[0], ConfigValue)

    # Tuples remain tuples, but their dict elements become ConfigValues
    assert isinstance(c_value.h, tuple)
    assert isinstance(c_value.h[0], ConfigValue)


@pytest.mark.parametrize(
    "operation",
    [
        lambda c: c.__setitem__("d", 4),
        lambda c: c.popitem(),
        lambda c: c.pop("a", 5),
        lambda c: c.clear(),
        lambda c: c.update({"e": 6}),
        lambda c: c.setdefault("f", 7),
        lambda c: c.__delitem__("b"),
    ],
    ids=[
        "setitem",
        "popitem",
        "pop",
        "clear",
        "update",
        "setdefault",
        "delitem",
    ],
)
def test_config_value_is_non_modifiable(simple_dict, operation):
    """Ensure that all standard dict mutation methods raise a TypeError."""
    # Expand simple_dict slightly so pop/del have valid targets if needed
    extended_dict = {**simple_dict, "c": 3}
    c_value = ConfigValue(extended_dict)

    with pytest.raises(TypeError):
        operation(c_value)

    # Ensure the underlying structure was not secretly mutated
    assert c_value.to_dict() == extended_dict


def test_json_dumpable(nested_dict):
    """Ensure the custom ConfigValue dictionary behaves natively with the json module."""
    c_value = ConfigValue(nested_dict)

    # Compare the serialized outputs
    assert json.loads(json.dumps(c_value)) == json.loads(json.dumps(nested_dict))


def test_dict_like_iteration_and_access(nested_dict):
    """Verify standard dictionary iteration, membership, and length behaviors work."""
    c_value = ConfigValue(nested_dict)

    assert "a" in c_value
    assert "d" not in c_value
    assert len(c_value) == 5

    assert c_value.keys() == nested_dict.keys()

    for k, v in c_value.items():
        assert v == nested_dict[k]

    for k in c_value.keys():
        assert k in nested_dict

    for v in c_value.values():
        assert v in nested_dict.values()
