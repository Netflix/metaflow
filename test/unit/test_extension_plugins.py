import pytest

from metaflow.extension_support import plugins as plugins_module
from metaflow.extension_support.plugins import (
    get_plugin,
    get_plugin_name,
    get_trampoline_cli_names,
    merge_lists,
)


class TestMergeLists:
    def test_overrides_win_over_base(self):
        class Item:
            def __init__(self, name):
                self.name = name

        base = [Item("a"), Item("b"), Item("c")]
        overrides = [Item("b_new"), Item("d")]
        merge_lists(base, overrides, "name")

        names = [item.name for item in base]
        assert "b_new" in names
        assert "d" in names
        assert "a" in names
        assert "c" in names

    def test_empty_overrides_keeps_base(self):
        class Item:
            def __init__(self, name):
                self.name = name

        base = [Item("a"), Item("b")]
        overrides = []
        merge_lists(base, overrides, "name")

        names = [item.name for item in base]
        assert names == ["a", "b"]

    def test_empty_base_uses_overrides(self):
        class Item:
            def __init__(self, name):
                self.name = name

        base = []
        overrides = [Item("x"), Item("y")]
        merge_lists(base, overrides, "name")

        names = [item.name for item in base]
        assert names == ["x", "y"]

    def test_both_empty(self):
        base = []
        overrides = []
        merge_lists(base, overrides, "name")
        assert base == []

    def test_no_duplicates_in_result(self):
        class Item:
            def __init__(self, name):
                self.name = name

        base = [Item("a"), Item("b")]
        overrides = [Item("a"), Item("b")]
        merge_lists(base, overrides, "name")

        names = set(item.name for item in base)
        assert names == {"a", "b"}


class TestGetPluginName:
    def test_step_decorator_name_extraction(self):
        class MockDecorator:
            name = "my_decorator"

        assert get_plugin_name("step_decorator", MockDecorator()) == "my_decorator"

    def test_environment_type_extraction(self):
        class MockEnv:
            TYPE = "conda"

        assert get_plugin_name("environment", MockEnv()) == "conda"

    def test_sidecar_returns_none(self):
        class MockSidecar:
            pass

        assert get_plugin_name("sidecar", MockSidecar()) is None

    def test_cli_single_command(self):
        class MockCLI:
            commands = {"run"}

        assert get_plugin_name("cli", MockCLI()) == "run"

    def test_cli_too_many_commands(self):
        class MockCLI:
            commands = {"run", "step", "start"}

        result = get_plugin_name("cli", MockCLI())
        assert "too many commands" in result

    def test_unknown_category_raises_key_error(self):
        class MockPlugin:
            name = "test"

        with pytest.raises(KeyError):
            get_plugin_name("nonexistent_category", MockPlugin())


class TestGetPlugin:
    def test_get_plugin_raises_value_error_for_invalid_path(self):
        with pytest.raises(ValueError, match="Cannot locate"):
            get_plugin("step_decorator", "nonexistent.module.path.FakeClass", "fake")

    def test_get_plugin_successfully_loads_real_class(self):
        cls = get_plugin(
            "step_decorator",
            "metaflow.plugins.retry_decorator.RetryDecorator",
            "retry",
        )
        assert cls.name == "retry"


class TestGetTrampolineCliNames:
    def test_returns_frozenset(self):
        result = get_trampoline_cli_names()
        assert isinstance(result, frozenset)

    def test_contains_expected_entries(self):
        result = get_trampoline_cli_names()
        assert "batch" in result or "kubernetes" in result
