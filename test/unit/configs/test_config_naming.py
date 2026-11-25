"""
Tests for Config parameter naming

Tests:
- Config names with underscores, dashes, and mixed naming
- Config with plain=True returning raw strings
- Config with plain=True and parser returning lists
- Config with plain=True and parser returning custom objects
"""

import pytest


def test_flow_completes(config_naming_run):
    """Test that the configuration parsing flow completes successfully."""
    assert config_naming_run.successful
    assert config_naming_run.finished


@pytest.mark.parametrize(
    "prefix, expected_text, expected_value",
    [
        ("underscore", "underscore", 42),
        ("dash", "dash", 99),
        ("mixed", "mixed", 123),
    ],
    ids=["underscore_naming", "dash_naming", "mixed_naming"],
)
def test_config_parameter_naming_formats(
    config_naming_run, prefix, expected_text, expected_value
):
    """Test that configuration parameters parse correctly across different naming conventions."""
    end_task = config_naming_run["end"].task

    assert end_task[f"{prefix}_test"].data == expected_text
    assert end_task[f"{prefix}_value"].data == expected_value
    assert end_task[f"{prefix}_dict"].data == {
        "test": expected_text,
        "value": expected_value,
    }
