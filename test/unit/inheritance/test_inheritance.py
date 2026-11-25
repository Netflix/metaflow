"""
Tests for FlowSpec inheritance patterns.

Tests various inheritance patterns including:
- Linear inheritance with parameters, configs, and decorators
- Diamond inheritance with MRO resolution
- Multiple inheritance from independent hierarchies
- FlowMutators with base class configs
- FlowMutators with derived class configs
- Step overrides across hierarchies
"""

import pytest


# ---------------------------------------------------------------------------
# Linear Inheritance Tests (FlowSpec -> BaseA -> BaseB -> BaseC -> Flow)
# ---------------------------------------------------------------------------


def test_linear_all_parameters_accessible(comprehensive_linear_run):
    """Test that parameters from all levels are accessible in linear inheritance."""
    end_task = comprehensive_linear_run["end"].task

    # From BaseA
    assert end_task["result_alpha"].data == 10
    assert end_task["result_beta"].data == 5

    # From BaseC
    assert end_task["result_gamma"].data == 2.5

    # From final class
    assert end_task["result_delta"].data == "final"


def test_linear_all_configs_accessible(comprehensive_linear_run):
    """Test that configs from all levels are accessible in linear inheritance."""
    end_task = comprehensive_linear_run["end"].task

    # From BaseB
    config_b = end_task["result_config_b"].data
    assert config_b["multiplier"] == 3
    assert config_b["offset"] == 100

    # From BaseC
    config_c = end_task["result_config_c"].data
    assert config_c["mode"] == "production"
    assert config_c["debug"] is False


def test_linear_computation_with_configs(comprehensive_linear_run):
    """Test computation using inherited parameters and configs."""
    end_task = comprehensive_linear_run["end"].task

    # start_value = alpha + beta = 10 + 5 = 15
    # processed_value = start_value * multiplier + offset = 15 * 3 + 100 = 145
    assert end_task["result_final"].data == 145


# ---------------------------------------------------------------------------
# FlowMutator with Base Class Config Tests
# ---------------------------------------------------------------------------


def test_mutator_base_config_parameters_accessible(mutator_with_base_config_run):
    """Test that base parameters are accessible when using a base config mutator."""
    start_task = mutator_with_base_config_run["start"].task

    assert start_task["result_base_param"].data == "base"
    assert start_task["result_middle_param"].data == 100
    assert start_task["result_final_param"].data == 50


def test_mutator_base_config_accessible(mutator_with_base_config_run):
    """Test that config from base class is accessible."""
    start_task = mutator_with_base_config_run["start"].task

    config = start_task["result_mutator_config"].data
    assert config["param_to_inject"] == "dynamic_param"
    assert config["default_value"] == 777
    assert config["inject_count"] == 42


def test_mutator_injects_from_base_config(mutator_with_base_config_run):
    """Test that mutator injects parameters based on base config."""
    start_task = mutator_with_base_config_run["start"].task

    # These parameters should be injected by the mutator based on mutator_config
    assert start_task["result_dynamic_param"].data == 777
    assert start_task["result_injected_count"].data == 42


def test_mutator_base_config_computation_with_injected_params(
    mutator_with_base_config_run,
):
    """Test computation using parameters injected from base config."""
    start_task = mutator_with_base_config_run["start"].task

    # result_computation = middle_param + dynamic_param + injected_count
    # = 100 + 777 + 42 = 919
    assert start_task["result_computation"].data == 919


# ---------------------------------------------------------------------------
# FlowMutator with Derived Class Config Tests
# ---------------------------------------------------------------------------


def test_mutator_derived_config_all_parameters_accessible(
    mutator_with_derived_config_run,
):
    """Test that all parameters from hierarchy are accessible when using derived config."""
    start_task = mutator_with_derived_config_run["start"].task

    assert start_task["result_base_param"].data == "base_value"
    assert start_task["result_middle_param"].data == 200
    assert start_task["result_final_param"].data == 999


def test_mutator_derived_config_all_configs_accessible(mutator_with_derived_config_run):
    """Test that all configs from hierarchy are accessible."""
    start_task = mutator_with_derived_config_run["start"].task

    middle_config = start_task["result_middle_config"].data
    assert middle_config["env"] == "staging"

    runtime_config = start_task["result_runtime_config"].data
    assert runtime_config["features"] == ["logging", "metrics"]
    assert runtime_config["worker_count"] == 16


def test_base_mutator_uses_derived_config(mutator_with_derived_config_run):
    """Test that base class mutator injects parameters from derived config."""
    start_task = mutator_with_derived_config_run["start"].task

    # These parameters should be injected by base mutator using derived runtime_config
    assert start_task["result_feature_logging"].data is True
    assert start_task["result_feature_metrics"].data is True
    assert start_task["result_worker_count"].data == 16


def test_computation_with_forward_injected_params(mutator_with_derived_config_run):
    """Test computation using parameters injected from derived config."""
    start_task = mutator_with_derived_config_run["start"].task

    # result_computation = worker_count * enabled_features + final_param
    # enabled_features = feature_logging (True=1) + feature_metrics (True=1) = 2
    # = 16 * 2 + 999 = 1031
    assert start_task["result_computation"].data == 1031


# ---------------------------------------------------------------------------
# Comprehensive Diamond Inheritance Tests
# ---------------------------------------------------------------------------


def test_diamond_parameters_from_all_branches(comprehensive_diamond_run):
    """Test parameters from all branches of diamond inheritance are accessible."""
    end_task = comprehensive_diamond_run["end"].task

    # From BaseA branch
    assert end_task["result_param_a"].data == 100

    # From BaseB branch
    assert end_task["result_param_b"].data == 50

    # From BaseC (merge point)
    assert end_task["result_param_c"].data == 25

    # From final class
    assert end_task["result_final_param"].data == "complete"


def test_diamond_configs_from_all_branches(comprehensive_diamond_run):
    """Test configs from all branches of diamond inheritance are accessible."""
    end_task = comprehensive_diamond_run["end"].task

    # From BaseA branch
    config_a = end_task["result_config_a"].data
    assert config_a["branch"] == "A"
    assert config_a["priority"] == 1

    # From BaseB branch
    config_b = end_task["result_config_b"].data
    assert config_b["branch"] == "B"
    assert config_b["weight"] == 2.5

    # From BaseC (merge point)
    config_c = end_task["result_config_c"].data
    assert config_c["mode"] == "diamond"
    assert config_c["enabled"] is True


def test_diamond_mro_resolution(comprehensive_diamond_run):
    """Test that MRO correctly resolves diamond pattern."""
    # If flow completes and uses correct step from BaseA, MRO is working
    assert comprehensive_diamond_run.successful
    step_ids = [step.id for step in comprehensive_diamond_run.steps()]
    assert "start" in step_ids
    assert "process" in step_ids


def test_diamond_computation_across_branches(comprehensive_diamond_run):
    """Test computation using values from all branches of a diamond structure."""
    end_task = comprehensive_diamond_run["end"].task

    # value_a = param_a * priority = 100 * 1 = 100
    # processed = value_a + (param_b * weight) + param_c
    #           = 100 + (50 * 2.5) + 25 = 100 + 125 + 25 = 250
    assert end_task["result_final"].data == 250


# ---------------------------------------------------------------------------
# Comprehensive Multiple Inheritance Tests (Independent Hierarchies)
# ---------------------------------------------------------------------------


def test_multi_hierarchy_parameters_from_first_hierarchy(
    comprehensive_multi_hierarchy_run,
):
    """Test parameters from first independent hierarchy are accessible."""
    end_task = comprehensive_multi_hierarchy_run["end"].task

    assert end_task["result_param_a"].data == 10
    assert end_task["result_param_b"].data == 20


def test_multi_hierarchy_parameters_from_second_hierarchy(
    comprehensive_multi_hierarchy_run,
):
    """Test parameters from second independent hierarchy are accessible."""
    end_task = comprehensive_multi_hierarchy_run["end"].task

    assert end_task["result_param_x"].data == 30
    assert end_task["result_param_y"].data == 40


def test_multi_hierarchy_merge_point_parameters(comprehensive_multi_hierarchy_run):
    """Test parameters from hierarchy merge point are accessible."""
    end_task = comprehensive_multi_hierarchy_run["end"].task

    assert end_task["result_param_c"].data == 5
    assert end_task["result_final_param"].data == "merged"


def test_multi_hierarchy_configs_from_both_hierarchies(
    comprehensive_multi_hierarchy_run,
):
    """Test configs from both separate hierarchies are accessible."""
    end_task = comprehensive_multi_hierarchy_run["end"].task

    # First hierarchy
    config_a = end_task["result_config_a"].data
    assert config_a["source"] == "hierarchy_a"
    assert config_a["value"] == 100

    # Second hierarchy
    config_x = end_task["result_config_x"].data
    assert config_x["source"] == "hierarchy_x"
    assert config_x["multiplier"] == 2

    config_y = end_task["result_config_y"].data
    assert config_y["enabled"] is True
    assert config_y["threshold"] == 50

    # Merge point
    config_c = end_task["result_config_c"].data
    assert config_c["merge"] is True
    assert config_c["offset"] == 200


def test_multi_hierarchy_step_override_from_merge_point(
    comprehensive_multi_hierarchy_run,
):
    """Test that BaseC's process step overrides BaseY's process step."""
    # If the computation matches BaseC's logic (not BaseY's), override worked
    end_task = comprehensive_multi_hierarchy_run["end"].task

    # hierarchy_a_result = param_a + param_b + config_a.value = 10 + 20 + 100 = 130
    # base_value = hierarchy_a_result * multiplier = 130 * 2 = 260
    # Since base_value (260) > threshold (50):
    # processed_value = base_value + offset + param_c = 260 + 200 + 5 = 465
    assert end_task["result_final"].data == 465


def test_multi_hierarchy_cross_hierarchy_computation(comprehensive_multi_hierarchy_run):
    """Test computation using values combined from both separate hierarchies."""
    end_task = comprehensive_multi_hierarchy_run["end"].task

    # Cross-hierarchy sum = param_a + param_b + param_x + param_y + param_c
    # = 10 + 20 + 30 + 40 + 5 = 105
    assert end_task["result_cross_hierarchy"].data == 105


def test_multi_hierarchy_mutator_from_first_hierarchy_executes(
    comprehensive_multi_hierarchy_run,
):
    """Verify mutator defined in first hierarchy applies context changes successfully."""
    end_task = comprehensive_multi_hierarchy_run["end"].task
    assert end_task["logging_param_count"].data == 6
    assert end_task["logging_config_count"].data == 4


def test_multi_hierarchy_decorated_step_from_first_hierarchy(
    comprehensive_multi_hierarchy_run,
):
    """Test that decorated step from first hierarchy functions correctly."""
    end_task = comprehensive_multi_hierarchy_run["end"].task
    assert end_task["source_from_var"].data == "hierarchy_x"


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture_name",
    [
        "comprehensive_linear_run",
        "mutator_with_base_config_run",
        "mutator_with_derived_config_run",
        "comprehensive_diamond_run",
        "comprehensive_multi_hierarchy_run",
    ],
    ids=[
        "linear_inheritance",
        "mutator_base_config",
        "mutator_derived_config",
        "diamond_inheritance",
        "multi_hierarchy",
    ],
)
def test_all_flows_complete_successfully(fixture_name, request):
    """Test that all structural inheritance pattern flows run completely and successfully."""
    run = request.getfixturevalue(fixture_name)
    assert run.successful, f"{fixture_name} did not complete successfully"
    assert run.finished, f"{fixture_name} did not finish"


@pytest.mark.parametrize(
    "fixture_name, expected_steps",
    [
        ("comprehensive_linear_run", ["start", "process", "end"]),
        ("mutator_with_base_config_run", ["start", "end"]),
        ("mutator_with_derived_config_run", ["start", "end"]),
        ("comprehensive_diamond_run", ["start", "process", "end"]),
        ("comprehensive_multi_hierarchy_run", ["start", "process", "end"]),
    ],
    ids=[
        "linear_steps",
        "mutator_base_steps",
        "mutator_derived_steps",
        "diamond_steps",
        "multi_hierarchy_steps",
    ],
)
def test_expected_steps_present(fixture_name, expected_steps, request):
    """Test that all expected DAG step structural markers are safely compiled into each run topology."""
    run = request.getfixturevalue(fixture_name)
    step_names = [step.id for step in run.steps()]

    for expected_step in expected_steps:
        assert (
            expected_step in step_names
        ), f"Step {expected_step} not found in {fixture_name}"
