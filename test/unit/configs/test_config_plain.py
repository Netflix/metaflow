"""
Tests for Config parameter plain setting

Tests:
- Config with plain=True returning raw strings
- Config with plain=True and parser returning lists
- Config with plain=True and parser returning custom objects
"""

import pytest


class TestConfigPlain:
    """Test Config with plain=True option."""

    def test_flow_completes(self, config_plain_run):
        """Test that the flow completes successfully."""
        assert config_plain_run.successful
        assert config_plain_run.finished

    def test_plain_string_without_parser(self, config_plain_run):
        """Test plain Config without parser returns raw string."""
        end_task = config_plain_run["end"].task

        # Verify it's a string
        assert end_task["plain_str_type"].data == "str"

        # Verify the value is the raw string (not parsed JSON)
        assert end_task["plain_str_value"].data == '{"raw": "string", "number": 123}'

    def test_plain_list_with_parser(self, config_plain_run):
        """Test plain Config with parser returning list (non-dict)."""
        end_task = config_plain_run["end"].task

        # Verify it's a list
        assert end_task["plain_list_type"].data == "list"

        # Verify the list contents
        assert end_task["plain_list_value"].data == [
            "apple",
            "banana",
            "cherry",
            "date",
        ]
        assert end_task["plain_list_length"].data == 4
        assert end_task["plain_list_first"].data == "apple"

    def test_plain_tuple_with_parser(self, config_plain_run):
        """Test plain Config with parser returning tuple (non-dict)."""
        end_task = config_plain_run["end"].task

        # Verify it's a tuple type
        assert end_task["plain_tuple_type"].data == "tuple"

        # Verify tuple contents
        assert end_task["plain_tuple_value"].data == ("test_tuple", 42, True)
        assert end_task["tuple_name"].data == "test_tuple"
        assert end_task["tuple_count"].data == 42
        assert end_task["tuple_enabled"].data == True
