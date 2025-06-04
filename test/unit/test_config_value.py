import json

import pytest

from metaflow.user_configs.config_parameters import ConfigValue


def test_isinstance():
    orig_dict = {"a": 1, "b": 2}
    c_value = ConfigValue(orig_dict)
    assert isinstance(c_value, dict)


def test_todict():
    orig_dict = {"a": 1, "b": 2}
    c_value = ConfigValue(orig_dict)
    assert c_value.to_dict() == orig_dict

    orig_dict = {"a": 1, "b": [1, 2, 3], "c": {"d": 4}, "e": {"f": [{"g": 5}]}}
    c_value = ConfigValue(orig_dict)
    assert c_value.to_dict() == orig_dict


def test_container_has_config_value():
    orig_dict = {
        "a": 1,
        "b": [1, 2, 3],
        "c": {"d": 4},
        "e": {"f": [{"g": 5}]},
        "h": ({"i": 6},),
    }
    c_value = ConfigValue(orig_dict)
    assert c_value.e.f[0].g == 5
    assert isinstance(c_value.c, ConfigValue)
    assert isinstance(c_value.e, ConfigValue)
    assert isinstance(c_value.e.f, list)
    assert isinstance(c_value.e.f[0], ConfigValue)
    assert isinstance(c_value.h, tuple)
    assert isinstance(c_value.h[0], ConfigValue)


def test_non_modifiable():
    orig_dict = {"a": 1, "b": 2, "c": 3}
    c_value = ConfigValue(orig_dict)
    with pytest.raises(TypeError):
        c_value["d"] = 4
    with pytest.raises(TypeError):
        c_value.popitem()
    with pytest.raises(TypeError):
        c_value.pop("a", 5)
    with pytest.raises(TypeError):
        c_value.clear()
    with pytest.raises(TypeError):
        c_value.update({"e": 6})
    with pytest.raises(TypeError):
        c_value.setdefault("f", 7)
    with pytest.raises(TypeError):
        del c_value["b"]

    assert c_value.to_dict() == orig_dict


def test_json_dumpable():
    orig_dict = {
        "a": 1,
        "b": [1, 2, 3],
        "c": {"d": 4},
        "e": {"f": [{"g": 5}]},
        "h": ({"i": 6},),
    }
    c_value = ConfigValue(orig_dict)
    assert json.loads(json.dumps(c_value)) == json.loads(json.dumps(orig_dict))


def test_dict_like_behavior():
    orig_dict = {
        "a": 1,
        "b": [1, 2, 3],
        "c": {"d": 4},
        "e": {"f": [{"g": 5}]},
        "h": ({"i": 6},),
    }
    c_value = ConfigValue(orig_dict)
    assert "a" in c_value
    assert "d" not in c_value
    assert len(c_value) == 5
    assert c_value.keys() == orig_dict.keys()
    for k, v in c_value.items():
        assert v == orig_dict[k]

    for k in c_value.keys():
        assert k in orig_dict

    for v in c_value.values():
        assert v in orig_dict.values()
