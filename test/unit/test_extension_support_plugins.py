"""Unit tests for ``metaflow.extension_support.plugins``.

Covers the plugin-system utility functions: list merging, category naming
helpers, extension plugin (de)serialization helpers, relative path resolution,
plugin loading, plugin resolution (including the +/- toggle syntax) and the
trampoline CLI name registry.
"""

import pytest

from metaflow.extension_support import plugins as plugins_module
from metaflow.extension_support.plugins import (
    _dict_for_category,
    _get_ext_plugins,
    _list_for_category,
    _resolve_relative_paths,
    _set_ext_plugins,
    get_plugin,
    get_plugin_name,
    get_trampoline_cli_names,
    merge_lists,
    resolve_plugins,
)


class _Named:
    """Minimal stand-in for a plugin class carrying a ``name`` attribute."""

    def __init__(self, name):
        self.name = name


class _Typed:
    """Minimal stand-in for a plugin class carrying a ``TYPE`` attribute."""

    TYPE = "dummy_datastore"


class _MismatchedType:
    TYPE = "some_other_name"


class _Item:
    """Minimal stand-in for an object carrying an arbitrary merge attribute."""

    def __init__(self, key, val=None):
        self.key = key
        self.val = val


def _with_plugin_globals(monkeypatch, **values):
    """Temporarily override globals of the plugins module (reverted after test)."""
    for key, value in values.items():
        monkeypatch.setitem(plugins_module.__dict__, key, value)


# ---------------------------------------------------------------------------
# merge_lists
# ---------------------------------------------------------------------------


def test_merge_lists_override_wins_on_attr_match():
    base = [_Item("a", 1), _Item("b", 2)]
    merge_lists(base, [_Item("b", 20), _Item("c", 30)], "key")
    assert [(i.key, i.val) for i in base] == [("b", 20), ("c", 30), ("a", 1)]


def test_merge_lists_no_overlap_appends_base_after_overrides():
    base = [_Item("a"), _Item("b")]
    merge_lists(base, [_Item("c")], "key")
    assert [i.key for i in base] == ["c", "a", "b"]


def test_merge_lists_mutates_base_in_place():
    base = [_Item("a")]
    merge_lists(base, [], "key")
    assert [i.key for i in base] == ["a"]


def test_merge_lists_empty_base_and_overrides():
    base = []
    merge_lists(base, [], "key")
    assert base == []


def test_merge_lists_does_not_duplicate_override_attr():
    base = [_Item("a", 1), _Item("a", 2)]
    merge_lists(base, [_Item("a", 3)], "key")
    assert [(i.key, i.val) for i in base] == [("a", 3)]


# ---------------------------------------------------------------------------
# _list_for_category / _dict_for_category
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "category, expected",
    [
        ("cli", "_all_clis"),
        ("datastore", "_all_datastores"),
        ("sidecar", "_all_sidecars"),
    ],
    ids=["cli", "datastore", "sidecar"],
)
def test_list_for_category_naming(category, expected):
    assert _list_for_category(category) == expected


@pytest.mark.parametrize(
    "category, expected",
    [
        ("cli", "_all_clis_dict"),
        ("datastore", "_all_datastores_dict"),
        ("sidecar", "_all_sidecars_dict"),
    ],
    ids=["cli", "datastore", "sidecar"],
)
def test_dict_for_category_naming(category, expected):
    assert _dict_for_category(category) == expected


# ---------------------------------------------------------------------------
# _get_ext_plugins / _set_ext_plugins
# ---------------------------------------------------------------------------


def test_get_ext_plugins_defaults_to_empty_list():
    assert _get_ext_plugins({}, "datastore") == []
    assert _get_ext_plugins({"OTHER_DESC": []}, "datastore") == []


def test_set_and_get_ext_plugins_roundtrip():
    module_globals = {}
    plugins = [("myds", "some.module.Class")]
    _set_ext_plugins(module_globals, "datastore", plugins)
    assert module_globals["DATASTORES_DESC"] == plugins
    assert _get_ext_plugins(module_globals, "datastore") == plugins


def test_set_ext_plugins_uses_uppercase_plural_key():
    module_globals = {}
    _set_ext_plugins(module_globals, "step_decorator", [("x", "y.Z")])
    assert "STEP_DECORATORS_DESC" in module_globals


# ---------------------------------------------------------------------------
# get_plugin_name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "category, plugin, expected",
    [
        ("step_decorator", _Named("my_step"), "my_step"),
        ("datastore", _Typed(), "dummy_datastore"),
    ],
    ids=["name-extractor", "type-extractor"],
)
def test_get_plugin_name_uses_category_extractor(category, plugin, expected):
    assert get_plugin_name(category, plugin) == expected


@pytest.mark.parametrize(
    "category", ["sidecar", "tl_plugin"], ids=["sidecar", "tl_plugin"]
)
def test_get_plugin_name_returns_none_for_categories_without_extractor(category):
    assert get_plugin_name(category, _Named("whatever")) is None


def test_get_plugin_name_unknown_category_raises_key_error():
    with pytest.raises(KeyError):
        get_plugin_name("not_a_category", object())


# ---------------------------------------------------------------------------
# _resolve_relative_paths
# ---------------------------------------------------------------------------


def _module_globals(package, **descs):
    module_globals = {"__package__": package}
    module_globals.update(descs)
    return module_globals


@pytest.mark.parametrize(
    "package, class_path, expected",
    [
        (
            "metaflow.plugins",
            ".datastore.LocalDatastore",
            "metaflow.plugins.datastore.LocalDatastore",
        ),
        (
            "metaflow.plugins.foo",
            "..datastore.LocalDatastore",
            "metaflow.plugins.datastore.LocalDatastore",
        ),
        (
            "metaflow.plugins",
            "other.package.LocalDatastore",
            "other.package.LocalDatastore",
        ),
    ],
    ids=["single-dot", "double-dot", "absolute-untouched"],
)
def test_resolve_relative_paths(package, class_path, expected):
    module_globals = _module_globals(package, DATASTORES_DESC=[("local", class_path)])
    _resolve_relative_paths(module_globals)
    assert module_globals["DATASTORES_DESC"] == [("local", expected)]


def test_resolve_relative_paths_escaping_package_raises():
    module_globals = _module_globals(
        "metaflow", DATASTORES_DESC=[("local", "..datastore.LocalDatastore")]
    )
    with pytest.raises(ValueError, match="exits out of Metaflow module"):
        _resolve_relative_paths(module_globals)


def test_resolve_relative_paths_handles_trampoline_clis():
    module_globals = _module_globals(
        "metaflow.plugins",
        TRAMPOLINE_CLIS_DESC=[("mycli", ".mymodule.MyCli")],
    )
    _resolve_relative_paths(module_globals)
    assert module_globals["TRAMPOLINE_CLIS_DESC"] == [
        ("mycli", "metaflow.plugins.mymodule.MyCli")
    ]


def test_resolve_relative_paths_without_trampoline_clis():
    # Should not fail when TRAMPOLINE_CLIS_DESC is absent
    _resolve_relative_paths(_module_globals("metaflow.plugins"))


# ---------------------------------------------------------------------------
# get_plugin
# ---------------------------------------------------------------------------


def test_get_plugin_loads_class_from_path():
    from collections import OrderedDict

    assert get_plugin("sidecar", "collections.OrderedDict", "my_sidecar") is OrderedDict


def test_get_plugin_missing_module_raises_value_error():
    with pytest.raises(ValueError, match="Cannot locate sidecar plugin"):
        get_plugin("sidecar", "no.such.module.Klass", "x")


def test_get_plugin_missing_class_raises_value_error():
    with pytest.raises(ValueError, match="Cannot locate 'NoSuchClass' class"):
        get_plugin("sidecar", "collections.NoSuchClass", "x")


def test_get_plugin_name_mismatch_raises_value_error():
    with pytest.raises(ValueError, match="expected to be named"):
        get_plugin("datastore", __name__ + "._MismatchedType", "expected_name")


def test_get_plugin_name_match_returns_class():
    assert get_plugin("datastore", __name__ + "._Typed", "dummy_datastore") is _Typed


# ---------------------------------------------------------------------------
# resolve_plugins
# ---------------------------------------------------------------------------


def test_resolve_plugins_toggle_syntax(monkeypatch):
    from collections import Counter, OrderedDict

    _with_plugin_globals(
        monkeypatch,
        ENABLED_SIDECAR=["a", "+b", "-c"],
        _all_sidecars_dict={
            "a": "collections.OrderedDict",
            "b": "collections.Counter",
            "c": "collections.deque",
        },
    )
    assert resolve_plugins("sidecar") == {"a": OrderedDict, "b": Counter}


def test_resolve_plugins_toggle_off_removes_plugin(monkeypatch):
    _with_plugin_globals(
        monkeypatch,
        ENABLED_SIDECAR=["-a"],
        _all_sidecars_dict={"a": "collections.OrderedDict"},
    )
    assert resolve_plugins("sidecar") == {}


def test_resolve_plugins_unknown_plugin_raises_value_error(monkeypatch):
    _with_plugin_globals(
        monkeypatch,
        ENABLED_SIDECAR=["ghost"],
        _all_sidecars_dict={},
    )
    with pytest.raises(ValueError, match="no such plugin is available"):
        resolve_plugins("sidecar")


def test_resolve_plugins_path_only_returns_class_paths(monkeypatch):
    class_path = "collections.OrderedDict"
    _with_plugin_globals(
        monkeypatch,
        ENABLED_SIDECAR=["a"],
        _all_sidecars_dict={"a": class_path},
    )
    assert resolve_plugins("sidecar", path_only=True) == {"a": class_path}


def test_resolve_plugins_with_name_extractor_returns_list(monkeypatch):
    _with_plugin_globals(
        monkeypatch,
        ENABLED_DATASTORE=["dummy_datastore"],
        _all_datastores_dict={"dummy_datastore": __name__ + "._Typed"},
    )
    assert resolve_plugins("datastore") == [_Typed]


# ---------------------------------------------------------------------------
# get_trampoline_cli_names
# ---------------------------------------------------------------------------


def test_get_trampoline_cli_names_returns_frozenset_snapshot(monkeypatch):
    _with_plugin_globals(monkeypatch, _trampoline_cli_names={"a", "b"})
    names = get_trampoline_cli_names()
    assert names == frozenset({"a", "b"})
    assert isinstance(names, frozenset)
