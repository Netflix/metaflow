import os
import uuid
import pytest

pytestmark = pytest.mark.config

_FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")

from .test_utils import (
    execute_test_flow,
    deploy_flow_to_scheduler,
    wait_for_deployed_run,
    disp_test,
)


def _run_config_flow(
    flow_name,
    exec_mode,
    decospecs,
    compute_env,
    tag,
    scheduler_config,
    test_name,
    tl_args_extra=None,
    run_params=None,
):
    """Shared helper: build tl_args with config env and run via execute_test_flow."""
    disp_test(exec_mode, decospecs, tag, scheduler_config)

    extra = {
        "env": {
            "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
            **(compute_env or {}),
        },
    }
    if tl_args_extra:
        extra.update(tl_args_extra)

    return execute_test_flow(
        flow_name=flow_name,
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=test_name,
        run_params=run_params,
        tl_args_extra=extra,
    )


def test_config_simple_default(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Config test with default values."""
    trigger_param = str(uuid.uuid4())[:8]
    run = _run_config_flow(
        flow_name="config/config_simple.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=f"config_simple_default_{backend_name}",
        tl_args_extra={"package_suffixes": ".py,.json"},
        run_params={"trigger_param": trigger_param},
    )

    default_config = {"a": {"b": "41", "project_name": "config_project"}}

    assert run.successful, "Run was not successful"
    expected_project_tag = f"project:{default_config['a']['project_name']}"
    assert expected_project_tag in run.tags, "Project name is incorrect"

    end_task = run["end"].task
    assert end_task.data.trigger_param == trigger_param
    assert end_task.data.config_val == 5, "config_val incorrect"
    assert (
        end_task.data.config_val_2 == default_config["a"]["b"]
    ), "config_val_2 incorrect"
    assert end_task.data.config_from_env == "5", "config_from_env incorrect"
    assert (
        end_task.data.config_from_env_2 == default_config["a"]["b"]
    ), "config_from_env_2 incorrect"


def test_config_simple_config_value(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Config test using config_value override."""
    trigger_param = str(uuid.uuid4())[:8]
    config_value = [
        ("cfg_default_value", {"a": {"project_name": "config_project_2", "b": "56"}})
    ]
    run = _run_config_flow(
        flow_name="config/config_simple.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=f"config_simple_config_value_{backend_name}",
        tl_args_extra={"package_suffixes": ".py,.json", "config_value": config_value},
        run_params={"trigger_param": trigger_param},
    )

    config = config_value[0][1]

    assert run.successful, "Run was not successful"
    expected_project_tag = f"project:{config['a']['project_name']}"
    assert expected_project_tag in run.tags, "Project name is incorrect"

    end_task = run["end"].task
    assert end_task.data.trigger_param == trigger_param
    assert end_task.data.config_val == 5, "config_val incorrect"
    assert end_task.data.config_val_2 == config["a"]["b"], "config_val_2 incorrect"
    assert end_task.data.config_from_env == "5", "config_from_env incorrect"
    assert (
        end_task.data.config_from_env_2 == config["a"]["b"]
    ), "config_from_env_2 incorrect"


def test_config_simple_config(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Config test using an explicit config file."""
    trigger_param = str(uuid.uuid4())[:8]
    config_files = [
        ("cfg", os.path.join(_FLOWS_DIR, "config", "config_simple_cmd.json"))
    ]
    run = _run_config_flow(
        flow_name="config/config_simple.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=f"config_simple_config_{backend_name}",
        tl_args_extra={
            "env": compute_env,  # no PROCESS_CONFIG needed for --config
            "package_suffixes": ".py,.json",
            "config": config_files,
        },
        run_params={"trigger_param": trigger_param},
    )

    assert run.successful, "Run was not successful"
    assert run["end"].task.data.trigger_param == trigger_param


def test_mutable_flow_default(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Mutable config test with default values."""
    trigger_param = str(uuid.uuid4())[:8]
    run = _run_config_flow(
        flow_name="config/mutable_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=f"mutable_flow_default_{backend_name}",
        run_params={"trigger_param": trigger_param, "param2": "48"},
    )

    default_config = {
        "parameters": [
            {"name": "param1", "default": "41"},
            {"name": "param2", "default": "42"},
        ],
        "step_add_environment": {"vars": {"STEP_LEVEL": "2"}},
        "step_add_environment_2": {"vars": {"STEP_LEVEL_2": "3"}},
        "flow_add_environment": {"vars": {"FLOW_LEVEL": "4"}},
        "project_name": "config_project",
    }

    assert run.successful, "Run was not successful"

    expected_project_tag = f"project:{default_config['project_name']}"
    assert expected_project_tag in run.tags, "Project name is incorrect"

    start_task_data = run["start"].task.data
    assert start_task_data.trigger_param == trigger_param

    test_parameters = {"trigger_param": trigger_param, "param2": "48"}
    for param in default_config["parameters"]:
        value = test_parameters.get(param["name"], None) or param["default"]
        assert hasattr(
            start_task_data, param["name"]
        ), f"Missing parameter {param['name']}"
        assert (
            getattr(start_task_data, param["name"]) == value
        ), f"Parameter {param['name']} incorrect: got {getattr(start_task_data, param['name'])}, expected {value}"

    assert (
        start_task_data.flow_level
        == default_config["flow_add_environment"]["vars"]["FLOW_LEVEL"]
    ), "flow_level incorrect"
    assert (
        start_task_data.step_level
        == default_config["step_add_environment"]["vars"]["STEP_LEVEL"]
    ), "step_level incorrect"
    assert (
        start_task_data.step_level_2
        == default_config["step_add_environment_2"]["vars"]["STEP_LEVEL_2"]
    ), "step_level_2 incorrect"


def test_mutable_flow_config_value(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Mutable flow with config_value override."""
    trigger_param = str(uuid.uuid4())[:8]
    config_value = [
        (
            "config",
            {
                "parameters": [
                    {"name": "param3", "default": "43"},
                    {"name": "param4", "default": "44"},
                ],
                "step_add_environment": {"vars": {"STEP_LEVEL": "5"}},
                "step_add_environment_2": {"vars": {"STEP_LEVEL_2": "6"}},
                "flow_add_environment": {"vars": {"FLOW_LEVEL": "7"}},
                "project_name": "config_project_2",
            },
        )
    ]
    run = _run_config_flow(
        flow_name="config/mutable_flow.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=f"mutable_flow_config_value_{backend_name}",
        tl_args_extra={"config_value": config_value},
        run_params={"trigger_param": trigger_param, "param3": "45"},
    )

    config = config_value[0][1]

    assert run.successful, "Run was not successful"

    expected_project_tag = f"project:{config['project_name']}"
    assert expected_project_tag in run.tags, "Project name is incorrect"

    start_task_data = run["start"].task.data
    assert start_task_data.trigger_param == trigger_param

    test_parameters = {"trigger_param": trigger_param, "param3": "45"}
    for param in config["parameters"]:
        value = test_parameters.get(param["name"], None) or param["default"]
        assert hasattr(
            start_task_data, param["name"]
        ), f"Missing parameter {param['name']}"
        assert (
            getattr(start_task_data, param["name"]) == value
        ), f"Parameter {param['name']} incorrect: got {getattr(start_task_data, param['name'])}, expected {value}"

    assert (
        start_task_data.flow_level
        == config["flow_add_environment"]["vars"]["FLOW_LEVEL"]
    ), "flow_level incorrect"
    assert (
        start_task_data.step_level
        == config["step_add_environment"]["vars"]["STEP_LEVEL"]
    ), "step_level incorrect"
    assert (
        start_task_data.step_level_2
        == config["step_add_environment_2"]["vars"]["STEP_LEVEL_2"]
    ), "step_level_2 incorrect"


def test_config_corner_cases(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Config corner cases: env_cfg, config_expr with a function, and extra env vars."""
    trigger_param = str(uuid.uuid4())[:8]
    run = _run_config_flow(
        flow_name="config/config_corner_cases.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        compute_env=compute_env,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name=f"config_corner_cases_{backend_name}",
        tl_args_extra={"package_suffixes": ".json"},
        run_params={"trigger_param": trigger_param},
    )

    default_config = {"a": {"b": "41", "project_name": "config_project"}}

    assert run.successful, "Run was not successful"

    expected_project_tag = f"project:{default_config['a']['project_name']}"
    assert expected_project_tag in run.tags, "Project name is incorrect"

    end_task = run["end"].task
    assert end_task.data.trigger_param == trigger_param
    assert end_task.data.config_val == 5, "config_val incorrect"
    assert (
        end_task.data.config_val_2 == default_config["a"]["b"]
    ), "config_val_2 incorrect"
    assert end_task.data.config_from_env == "5", "config_from_env incorrect"
    assert (
        end_task.data.config_from_env_2 == default_config["a"]["b"]
    ), "config_from_env_2 incorrect"
    assert end_task.data.var1 == "1", "var1 incorrect"
    assert end_task.data.var2 == "2", "var2 incorrect"


@pytest.mark.scheduler_only
def test_config_from_deployment(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Verify DeployedFlow.from_deployment() works with Config-based flows."""
    from metaflow.runner.deployer import DeployedFlow

    test_unique_tag = f"test_config_from_deployment_{backend_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    scheduler_type = scheduler_config.scheduler_type
    if scheduler_type is None:
        pytest.skip("No scheduler configured")
    impl = scheduler_type.replace("-", "_")

    deployed_flow = deploy_flow_to_scheduler(
        flow_name="config/hello_from_deployment_with_config.py",
        tl_args={
            "decospecs": decospecs,
            "env": {
                "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
                **(compute_env or {}),
            },
        },
        scheduler_args={"cluster": scheduler_config.cluster},
        deploy_args={
            "tags": combined_tags,
            **(getattr(scheduler_config, "deploy_args", None) or {}),
        },
        scheduler_type=scheduler_type,
    )

    # First run
    run1 = wait_for_deployed_run(deployed_flow)
    assert run1.successful, "First run was not successful"
    assert run1["start"].task.data.batch_size == 32, "batch_size incorrect"

    # Recover via from_deployment and trigger a second run
    deployment_id = deployed_flow.deployer.name
    recovered = DeployedFlow.from_deployment(deployment_id, impl=impl)
    run2 = wait_for_deployed_run(recovered)
    assert run2.successful, "Run from recovered deployment was not successful"
    assert run2["start"].task.data.batch_size == 32, "batch_size incorrect on recovery"
