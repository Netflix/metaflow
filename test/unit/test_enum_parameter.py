import pytest
import sys
import os

# Ensure metaflow is importable from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from metaflow.parameters import Parameter, EnumTypeClass
from metaflow.exception import MetaflowException


def _init_parameter(name, **kwargs):
    """Helper to create and initialize a Parameter."""
    p = Parameter(name, **kwargs)
    p.init()
    return p


class TestEnumTypeClass:
    """Tests for the EnumTypeClass Click ParamType."""

    def test_convert_valid_value(self):
        enum_type = EnumTypeClass(["a", "b", "c"])
        assert enum_type.convert("a", None, None) == "a"
        assert enum_type.convert("b", None, None) == "b"
        assert enum_type.convert("c", None, None) == "c"

    def test_convert_invalid_value(self):
        from metaflow._vendor.click.exceptions import BadParameter

        enum_type = EnumTypeClass(["a", "b", "c"])
        with pytest.raises(BadParameter):
            enum_type.convert("d", None, None)

    def test_name_attribute(self):
        enum_type = EnumTypeClass(["x", "y"])
        assert enum_type.name == "enum"

    def test_repr(self):
        enum_type = EnumTypeClass(["traffic", "labeling"])
        assert repr(enum_type) == "Enum(['traffic', 'labeling'])"

    def test_values_stored(self):
        enum_type = EnumTypeClass(["traffic", "labeling"])
        assert enum_type.values == ["traffic", "labeling"]

    def test_values_from_tuple(self):
        enum_type = EnumTypeClass(("traffic", "labeling"))
        assert enum_type.values == ["traffic", "labeling"]

    def test_get_metavar(self):
        enum_type = EnumTypeClass(["a", "b", "c"])
        metavar = enum_type.get_metavar(None)
        assert "a" in metavar
        assert "b" in metavar
        assert "c" in metavar


class TestEnumParameter:
    """Tests for Parameter with type='enum'."""

    def test_basic_enum_parameter(self):
        p = _init_parameter(
            "action",
            type="enum",
            values=["traffic", "labeling"],
            default="traffic",
        )
        assert isinstance(p.kwargs["type"], EnumTypeClass)
        assert p.kwargs["type"].values == ["traffic", "labeling"]
        assert p.kwargs["default"] == "traffic"

    def test_enum_without_values_raises(self):
        with pytest.raises(MetaflowException, match="'values' is required"):
            _init_parameter("action", type="enum", default="traffic")

    def test_enum_with_non_list_values_raises(self):
        with pytest.raises(MetaflowException, match="must be a list or tuple"):
            _init_parameter(
                "action", type="enum", values="traffic", default="traffic"
            )

    def test_enum_with_empty_values_raises(self):
        with pytest.raises(MetaflowException, match="must not be empty"):
            _init_parameter("action", type="enum", values=[], default="traffic")

    def test_enum_with_tuple_values(self):
        p = _init_parameter(
            "mode",
            type="enum",
            values=("fast", "slow"),
            default="fast",
        )
        assert isinstance(p.kwargs["type"], EnumTypeClass)
        assert p.kwargs["type"].values == ["fast", "slow"]

    def test_enum_without_default(self):
        """Enum parameter without default should work (required mode)."""
        p = _init_parameter(
            "action",
            type="enum",
            values=["traffic", "labeling"],
            required=True,
        )
        assert isinstance(p.kwargs["type"], EnumTypeClass)
        assert p.kwargs.get("default") is None

    def test_enum_values_not_in_kwargs_after_init(self):
        """The 'values' kwarg should be consumed and removed during init."""
        p = _init_parameter(
            "action",
            type="enum",
            values=["traffic", "labeling"],
            default="traffic",
        )
        assert "values" not in p.kwargs

    def test_enum_is_string_type(self):
        """Enum parameters should be treated as string type since values are strings."""
        p = _init_parameter(
            "action",
            type="enum",
            values=["traffic", "labeling"],
            default="traffic",
        )
        # EnumTypeClass is not str, so is_string_type should be False
        assert not p.is_string_type

    def test_enum_type_validates_on_convert(self):
        """The underlying Click Choice should reject invalid values."""
        from metaflow._vendor.click.exceptions import BadParameter

        p = _init_parameter(
            "action",
            type="enum",
            values=["traffic", "labeling"],
            default="traffic",
        )
        enum_type = p.kwargs["type"]
        assert enum_type.convert("traffic", None, None) == "traffic"
        assert enum_type.convert("labeling", None, None) == "labeling"
        with pytest.raises(BadParameter):
            enum_type.convert("invalid", None, None)

    def test_enum_with_help_text(self):
        p = _init_parameter(
            "action",
            type="enum",
            values=["traffic", "labeling"],
            default="traffic",
            help="The action to perform.",
        )
        assert p.kwargs["help"] == "The action to perform."
