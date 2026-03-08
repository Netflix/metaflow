import os
import uuid
import pytest

pytestmark = pytest.mark.config

_FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")

from .test_utils import (
    run_flow_with_env,
    deploy_flow_to_scheduler,
    verify_single_run,
    wait_for_deployed_run,
    disp_test,
)


def test_config_simple_default(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Config test with default values."""
    disp_test(exec_mode, decospecs, tag, scheduler_config)
    trigger_param = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_config_simple_default_{backend_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {
        "env": {
            "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
            **compute_env,
        },
        "package_suffixes": ".py,.json",
        "decospecs": decospecs,
    }

    if exec_mode == "deployer":
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="config/config_simple.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_config.scheduler_type,
        )
        run = wait_for_deployed_run(
            deployed_flow, run_kwargs={"trigger_param": trigger_param}
        )
        if not run.successful:
            raise RuntimeError(f"Run {run.id} failed")
    else:
        run_flow_with_env(
            flow_name="config/config_simple.py",
            runner_args={"tags": combined_tags, "trigger_param": trigger_param},
            **tl_args,
        )
        run = verify_single_run("ConfigSimple", tags=combined_tags)

    default_config = {"a": {"b": "41", "project_name": "config_project"}}

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
    disp_test(exec_mode, decospecs, tag, scheduler_config)
    trigger_param = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_config_simple_config_value_{backend_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    config_value = [
        ("cfg_default_value", {"a": {"project_name": "config_project_2", "b": "56"}})
    ]

    tl_args = {
        "env": {
            "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
            **compute_env,
        },
        "package_suffixes": ".py,.json",
        "config_value": config_value,
        "decospecs": decospecs,
    }

    if exec_mode == "deployer":
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="config/config_simple.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_config.scheduler_type,
        )
        run = wait_for_deployed_run(
            deployed_flow, run_kwargs={"trigger_param": trigger_param}
        )
        if not run.successful:
            raise RuntimeError(f"Run {run.id} failed")
    else:
        run_flow_with_env(
            flow_name="config/config_simple.py",
            runner_args={"tags": combined_tags, "trigger_param": trigger_param},
            **tl_args,
        )
        run = verify_single_run("ConfigSimple", tags=combined_tags)

    config = {"a": {"project_name": "config_project_2", "b": "56"}}

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
    disp_test(exec_mode, decospecs, tag, scheduler_config)
    trigger_param = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_config_simple_config_{backend_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    config_files = [
        ("cfg", os.path.join(_FLOWS_DIR, "config", "config_simple_cmd.json"))
    ]

    tl_args = {
        "env": compute_env,
        "package_suffixes": ".py,.json",
        "config": config_files,
        "decospecs": decospecs,
    }

    if exec_mode == "deployer":
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="config/config_simple.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_config.scheduler_type,
        )
        run = wait_for_deployed_run(
            deployed_flow, run_kwargs={"trigger_param": trigger_param}
        )
        if not run.successful:
            raise RuntimeError(f"Run {run.id} failed")
    else:
        run_flow_with_env(
            flow_name="config/config_simple.py",
            runner_args={"tags": combined_tags, "trigger_param": trigger_param},
            **tl_args,
        )
        run = verify_single_run("ConfigSimple", tags=combined_tags)

    assert run.successful, "Run was not successful"
    assert run["end"].task.data.trigger_param == trigger_param


def test_mutable_flow_default(
    exec_mode, decospecs, compute_env, tag, scheduler_config, backend_name
):
    """Mutable config test with default values."""
    disp_test(exec_mode, decospecs, tag, scheduler_config)
    trigger_param = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_mutable_flow_default_{backend_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

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

    tl_args = {
        "env": {
            "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
            **compute_env,
        },
        "decospecs": decospecs,
    }

    if exec_mode == "deployer":
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="config/mutable_flow.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_config.scheduler_type,
        )
        run = wait_for_deployed_run(
            deployed_flow,
            run_kwargs={"trigger_param": trigger_param, "param2": "48"},
        )
        if not run.successful:
            raise RuntimeError(f"Run {run.id} failed")
    else:
        run_flow_with_env(
            flow_name="config/mutable_flow.py",
            runner_args={
                "tags": combined_tags,
                "trigger_param": trigger_param,
                "param2": "48",
            },
            **tl_args,
        )
        run = verify_single_run("ConfigMutableFlow", tags=combined_tags)

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
    disp_test(exec_mode, decospecs, tag, scheduler_config)
    trigger_param = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_mutable_flow_config_value_{backend_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

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

    tl_args = {
        "env": {
            "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
            **compute_env,
        },
        "config_value": config_value,
        "decospecs": decospecs,
    }

    if exec_mode == "deployer":
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="config/mutable_flow.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_config.scheduler_type,
        )
        run = wait_for_deployed_run(
            deployed_flow,
            run_kwargs={"trigger_param": trigger_param, "param3": "45"},
        )
        if not run.successful:
            raise RuntimeError(f"Run {run.id} failed")
    else:
        run_flow_with_env(
            flow_name="config/mutable_flow.py",
            runner_args={
                "tags": combined_tags,
                "trigger_param": trigger_param,
                "param3": "45",
            },
            **tl_args,
        )
        run = verify_single_run("ConfigMutableFlow", tags=combined_tags)

    assert run.successful, "Run was not successful"

    config = config_value[0][1]
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
    disp_test(exec_mode, decospecs, tag, scheduler_config)
    trigger_param = str(uuid.uuid4())[:8]
    test_unique_tag = f"test_config_corner_cases_{backend_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args = {
        "env": {
            "METAFLOW_CLICK_API_PROCESS_CONFIG": "1",
            **compute_env,
        },
        "package_suffixes": ".json",
        "decospecs": decospecs,
    }

    if exec_mode == "deployer":
        deployed_flow = deploy_flow_to_scheduler(
            flow_name="config/config_corner_cases.py",
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **(scheduler_config.deploy_args or {})},
            scheduler_type=scheduler_config.scheduler_type,
        )
        run = wait_for_deployed_run(
            deployed_flow, run_kwargs={"trigger_param": trigger_param}
        )
        if not run.successful:
            raise RuntimeError(f"Run {run.id} failed")
    else:
        run_flow_with_env(
            flow_name="config/config_corner_cases.py",
            runner_args={"tags": combined_tags, "trigger_param": trigger_param},
            **tl_args,
        )
        run = verify_single_run("ConfigSimple", tags=combined_tags)

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
