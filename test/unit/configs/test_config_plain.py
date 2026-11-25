"""
Tests for Config parameter plain setting

Tests:
- Config with plain=True returning raw strings
- Config with plain=True and parser returning lists
- Config with plain=True and parser returning custom objects
"""

import pytest


def test_flow_completes(config_plain_run):
    """Test that the configuration plain parsing flow completes successfully."""
    assert config_plain_run.successful
    assert config_plain_run.finished


@pytest.mark.parametrize(
    "key_prefix, expected_type, expected_value",
    [
        ("plain_str", "str", '{"raw": "string", "number": 123}'),
        ("plain_list", "list", ["apple", "banana", "cherry", "date"]),
        ("plain_tuple", "tuple", ("test_tuple", 42, True)),
    ],
    ids=["raw_string", "parsed_list", "parsed_tuple"],
)
def test_plain_config_types_and_values(
    config_plain_run, key_prefix, expected_type, expected_value
):
    """Test that plain Config fields yield the expected type and exact raw or parsed values."""
    end_task = config_plain_run["end"].task

    assert end_task[f"{key_prefix}_type"].data == expected_type
    assert end_task[f"{key_prefix}_value"].data == expected_value


def test_plain_list_properties(config_plain_run):
    """Test detailed extraction and length properties of a plain parsed list."""
    end_task = config_plain_run["end"].task

    assert end_task["plain_list_length"].data == 4
    assert end_task["plain_list_first"].data == "apple"


def test_plain_tuple_properties(config_plain_run):
    """Test detailed extraction and inner structures of a plain parsed tuple."""
    end_task = config_plain_run["end"].task

    assert end_task["tuple_name"].data == "test_tuple"
    assert end_task["tuple_count"].data == 42
    assert end_task["tuple_enabled"].data is True
