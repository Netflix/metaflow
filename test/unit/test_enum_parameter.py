import pytest

from metaflow import EnumTypeClass, Parameter
from metaflow.exception import MetaflowException
from metaflow.parameters import flow_context
from metaflow._vendor import click


def _make_flow(*params):
    """Return a minimal fake flow class containing the given parameters."""

    attrs = {"_get_parameters": classmethod(lambda cls: [(p.name, p) for p in params])}
    for p in params:
        attrs[p.name] = p
    return type("FakeFlow", (), attrs)


def _init(param):
    flow_cls = _make_flow(param)
    with flow_context(flow_cls):
        param.init()
    return param


class TestEnumTypeClass:
    def test_repr_and_str(self):
        t = EnumTypeClass(["traffic", "labeling"])
        assert repr(t) == "Enum[traffic|labeling]"
        assert str(t) == "Enum[traffic|labeling]"

    def test_choices_stored(self):
        choices = ["a", "b", "c"]
        t = EnumTypeClass(choices)
        assert list(t.choices) == choices

    def test_convert_valid_value(self):
        t = EnumTypeClass(["traffic", "labeling"])
        assert t.convert("traffic", None, None) == "traffic"
        assert t.convert("labeling", None, None) == "labeling"

    def test_convert_invalid_value_raises(self):
        t = EnumTypeClass(["traffic", "labeling"])
        with pytest.raises(Exception):
            t.convert("invalid", None, None)

    def test_convert_case_sensitive_by_default(self):
        t = EnumTypeClass(["Traffic"])
        with pytest.raises(Exception):
            t.convert("traffic", None, None)

    def test_convert_case_insensitive_matches_original_casing(self):
        t = EnumTypeClass(["Traffic", "Labeling"], case_sensitive=False)
        assert t.convert("traffic", None, None) == "Traffic"
        assert t.convert("LABELING", None, None) == "Labeling"

    def test_metavar_format(self):
        t = EnumTypeClass(["a", "b"])
        assert t.get_metavar(None) == "[a|b]"

    def test_name_attribute(self):
        assert EnumTypeClass(["x"]).name == "Enum"


class TestEnumParameter:
    def test_basic_enum_parameter(self):
        p = _init(
            Parameter(
                "action",
                type="enum",
                values=["traffic", "labeling"],
                default="traffic",
            )
        )
        assert isinstance(p.kwargs["type"], EnumTypeClass)
        assert list(p.kwargs["type"].choices) == ["traffic", "labeling"]

    def test_default_value_preserved(self):
        p = _init(
            Parameter("mode", type="enum", values=["fast", "slow"], default="fast")
        )
        assert p.kwargs["default"] == "fast"

    def test_values_not_leaked_to_click_kwargs(self):
        p = _init(Parameter("x", type="enum", values=["a", "b"]))
        assert "values" not in p.kwargs

    def test_case_sensitive_not_leaked_to_click_kwargs(self):
        p = _init(Parameter("x", type="enum", values=["a", "b"], case_sensitive=False))
        assert "case_sensitive" not in p.kwargs

    def test_case_insensitive_parameter(self):
        p = _init(
            Parameter(
                "env",
                type="enum",
                values=["Prod", "Dev"],
                case_sensitive=False,
            )
        )
        enum_type = p.kwargs["type"]
        assert enum_type.convert("prod", None, None) == "Prod"

    def test_values_are_stringified(self):
        p = _init(Parameter("level", type="enum", values=[1, 2, 3]))
        assert list(p.kwargs["type"].choices) == ["1", "2", "3"]

    def test_enum_type_is_not_string_type(self):
        p = _init(Parameter("x", type="enum", values=["a", "b"]))
        assert not p.is_string_type

    def test_direct_enum_type_class_instantiation(self):
        p = _init(Parameter("x", type=EnumTypeClass(["yes", "no"]), default="yes"))
        assert isinstance(p.kwargs["type"], EnumTypeClass)
        assert list(p.kwargs["type"].choices) == ["yes", "no"]

    def test_option_kwargs_contains_enum_type(self):
        p = _init(Parameter("action", type="enum", values=["a", "b"]))
        kwargs = p.option_kwargs(deploy_mode=False)
        assert isinstance(kwargs["type"], EnumTypeClass)


class TestEnumParameterErrors:
    def test_missing_values_raises(self):
        with pytest.raises(MetaflowException, match="values"):
            _init(Parameter("x", type="enum"))

    def test_empty_values_list_raises(self):
        with pytest.raises(MetaflowException, match="values"):
            _init(Parameter("x", type="enum", values=[]))

    def test_values_without_enum_type_raises(self):
        with pytest.raises(MetaflowException, match="values"):
            _init(Parameter("x", values=["a", "b"]))

    def test_separator_with_enum_raises(self):
        with pytest.raises(MetaflowException, match="[Ss]eparator"):
            _init(Parameter("x", type="enum", values=["a", "b"], separator=","))
