import json
import os
import uuid

from typing import Any, Dict, List, Optional

maestro_rand = str(uuid.uuid4())[:8]
scheduler_cluster = os.environ.get("NETFLIX_ENVIRONMENT", "sandbox")
# Use sandbox for tests
if scheduler_cluster == "prod":
    scheduler_cluster = "sandbox"


# Generates tests for regular, titus and maestro invocations
def all_three_options(
    id_base: str,
    flow: str,
    config_values: Optional[List[Dict[str, Any]]] = None,
    configs: Optional[List[Dict[str, str]]] = None,
    addl_params: Optional[List[str]] = None,
):
    result = []
    if config_values is None:
        config_values = [{}]
    if configs is None:
        configs = [{}]
    if addl_params is None:
        addl_params = []

    if len(config_values) < len(configs):
        config_values.extend([{}] * (len(configs) - len(config_values)))
    if len(configs) < len(config_values):
        configs.extend([{}] * (len(config_values) - len(configs)))
    if len(addl_params) < len(config_values):
        addl_params.extend([""] * (len(config_values) - len(addl_params)))

    for idx, (config_value, config) in enumerate(zip(config_values, configs)):
        # Regular run
        result.append(
            {
                "id": f"{id_base}_{idx}",
                "flow": flow,
                "config_values": config_value,
                "configs": config,
                "params": "run " + addl_params[idx],
            }
        )

        # Titus run
        result.append(
            {
                "id": f"{id_base}_titus_{idx}",
                "flow": flow,
                "config_values": config_value,
                "configs": config,
                "params": "run --with titus " + addl_params[idx],
            }
        )

        # Maestro run
        result.append(
            {
                "id": f"{id_base}_maestro_{idx}",
                "flow": flow,
                "config_values": config_value,
                "configs": config,
                "params": [
                    # Create the flow
                    f"--branch {maestro_rand}_{id_base}_maestro_{idx} maestro "
                    f"--cluster {scheduler_cluster} create",
                    # Trigger the run
                    f"--branch {maestro_rand}_{id_base}_maestro_{idx} maestro "
                    f"--cluster {scheduler_cluster} trigger --trigger_param "
                    f"{maestro_rand} --force " + addl_params[idx],
                ],
                "user_environment": {"METAFLOW_SETUP_GANDALF_POLICY": "0"},
            }
        )
    return result


TESTS = [
    *all_three_options(
        "config_simple",
        "config_simple.py",
        [
            {},
            {
                "cfg_default_value": json.dumps(
                    {"a": {"project_name": "config_project_2", "b": "56"}}
                )
            },
        ],
    ),
    *all_three_options(
        "mutable_flow",
        "mutable_flow.py",
        [
            {},
            {
                "config": json.dumps(
                    {
                        "parameters": [
                            {"name": "param3", "default": "43"},
                            {"name": "param4", "default": "44"},
                        ],
                        "step_add_environment": {"vars": {"STEP_LEVEL": "5"}},
                        "step_add_environment_2": {"vars": {"STEP_LEVEL_2": "6"}},
                        "flow_add_environment": {"vars": {"FLOW_LEVEL": "7"}},
                        "project_name": "config_project_2",
                    }
                )
            },
        ],
        addl_params=["", "--param3 45"],
    ),
    *all_three_options(
        "config_parser_flow",
        "config_parser.py",
        [{}],
    ),
]
