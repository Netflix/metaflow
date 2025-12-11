"""
Flow testing Config with plain=True option.

Tests that plain Config parameters:
- Without parser: return raw string
- With parser returning list: return list (non-dict type)
- With parser returning tuple: return tuple (non-dict type)
"""

import json
from metaflow import FlowSpec, Config, step


def list_parser(content: str):
    """Parser that returns a list instead of a dict."""
    return content.strip().split(",")


def tuple_parser(content: str):
    """Parser that returns a tuple instead of a dict."""
    data = json.loads(content)
    return (data["name"], data["count"], data["enabled"])


class ConfigPlainFlow(FlowSpec):
    """Test flow for Config with plain option."""

    # Plain config without parser (returns raw string)
    plain_string_config = Config(
        "plain-string-config",
        default_value='{"raw": "string", "number": 123}',
        plain=True,
    )

    # Plain config with parser returning a list (non-dict)
    plain_list_config = Config(
        "plain-list-config",
        default_value="apple,banana,cherry,date",
        parser=list_parser,
        plain=True,
    )

    # Plain config with parser returning a tuple (non-dict)
    plain_tuple_config = Config(
        "plain-tuple-config",
        default_value='{"name": "test_tuple", "count": 42, "enabled": true}',
        parser=tuple_parser,
        plain=True,
    )

    # None config and plain flag work properlty
    plain_none_config = Config(
        "plain-none-config",
        default_value=None,
        plain=True,
    )
    # None config works well
    none_config = Config("none-config", default_value=None)

    @step
    def start(self):
        """Access plain configs with different types and validate values."""
        # Plain string config (no parser)
        self.plain_str_value = self.plain_string_config
        self.plain_str_type = type(self.plain_string_config).__name__

        # Validate plain string config
        assert isinstance(
            self.plain_string_config, str
        ), f"Expected str, got {type(self.plain_string_config)}"
        assert (
            self.plain_str_value == '{"raw": "string", "number": 123}'
        ), f"Unexpected value: {self.plain_str_value}"
        assert (
            self.plain_str_type == "str"
        ), f"Expected 'str', got {self.plain_str_type}"
        print(
            f"✓ Plain string validated: {self.plain_str_value} (type: {self.plain_str_type})"
        )

        # Plain list config
        self.plain_list_value = self.plain_list_config
        self.plain_list_type = type(self.plain_list_config).__name__
        self.plain_list_length = len(self.plain_list_config)
        self.plain_list_first = self.plain_list_config[0]

        # Validate plain list config
        assert isinstance(
            self.plain_list_config, list
        ), f"Expected list, got {type(self.plain_list_config)}"
        assert self.plain_list_value == [
            "apple",
            "banana",
            "cherry",
            "date",
        ], f"Unexpected list: {self.plain_list_value}"
        assert (
            self.plain_list_type == "list"
        ), f"Expected 'list', got {self.plain_list_type}"
        assert (
            self.plain_list_length == 4
        ), f"Expected length 4, got {self.plain_list_length}"
        assert (
            self.plain_list_first == "apple"
        ), f"Expected 'apple', got {self.plain_list_first}"
        print(
            f"✓ Plain list validated: {self.plain_list_value} (type: {self.plain_list_type})"
        )

        # Plain tuple config
        self.plain_tuple_type = type(self.plain_tuple_config).__name__
        self.plain_tuple_value = self.plain_tuple_config
        self.tuple_name = self.plain_tuple_config[0]
        self.tuple_count = self.plain_tuple_config[1]
        self.tuple_enabled = self.plain_tuple_config[2]

        # Validate plain tuple config
        assert isinstance(
            self.plain_tuple_config, tuple
        ), f"Expected tuple, got {type(self.plain_tuple_config)}"
        assert (
            self.plain_tuple_type == "tuple"
        ), f"Expected 'tuple', got {self.plain_tuple_type}"
        assert (
            self.tuple_name == "test_tuple"
        ), f"Expected 'test_tuple', got {self.tuple_name}"
        assert self.tuple_count == 42, f"Expected 42, got {self.tuple_count}"
        assert self.tuple_enabled == True, f"Expected True, got {self.tuple_enabled}"
        assert (
            len(self.plain_tuple_config) == 3
        ), f"Expected length 3, got {len(self.plain_tuple_config)}"
        print(
            f"✓ Plain tuple validated: {self.plain_tuple_value} (type: {self.plain_tuple_type})"
        )

        assert (
            self.plain_none_config is None
        ), f"Expected None, got {self.plain_none_config}"
        print(f"✓ Plain None config validated")
        assert self.none_config is None, f"Expected None, got {self.none_config}"
        print(f"✓ Non-plain None config validated")
        self.next(self.end)

    @step
    def end(self):
        """End step."""
        print("ConfigPlainFlow completed successfully")


if __name__ == "__main__":
    ConfigPlainFlow()
