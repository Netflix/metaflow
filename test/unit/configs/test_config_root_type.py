import pytest

from metaflow import Config, FlowSpec, step
from metaflow._vendor import click
from metaflow.parameters import flow_context
from metaflow.user_configs.config_options import ConfigInput, ConvertDictOrStr
from metaflow.user_configs.config_parameters import ConfigValue


class ConfigRootTypeFlow(FlowSpec):
    cfg = Config("cfg")
    barecfg = Config("barecfg", default_value="", plain=True)

    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def process(name, value):
    config_input = ConfigInput(
        req_configs=[],
        defaults={"cfg": (None, False), "barecfg": ("", False)},
        parsers={"cfg": (None, False), "barecfg": (None, True)},
    )
    flow_name = ConfigRootTypeFlow.__name__
    with flow_context(ConfigRootTypeFlow):
        # Click calls process_configs once for --config and once for
        # --config-value; only the second call does the work.
        config_input.process_configs(flow_name, "config", {}, True, "local")
        return config_input.process_configs(
            flow_name,
            "config_value",
            {name: ConvertDictOrStr.convert_value(value, False)},
            True,
            "local",
        )


@pytest.mark.parametrize(
    ("value", "value_type"),
    [("[]", list), ("123", int), ('"text"', str), ("true", bool)],
    ids=["array", "number", "string", "boolean"],
)
def test_non_mapping_config_value_raises_usage_error(value, value_type):
    with pytest.raises(
        click.UsageError,
        match=r"must be a mapping \(got type %s\)" % value_type.__name__,
    ):
        process("cfg", value)


def test_null_config_value_is_accepted():
    assert process("cfg", "null")["cfg"] is None


def test_object_config_value_is_accepted():
    result = process("cfg", "{}")

    assert isinstance(result["cfg"], ConfigValue)
    assert result["cfg"] == {}


def test_plain_config_value_accepts_bare_string():
    assert process("barecfg", "a bare testvalue")["barecfg"] == "a bare testvalue"
