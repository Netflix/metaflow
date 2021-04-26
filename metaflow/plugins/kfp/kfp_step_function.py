from typing import List, Dict


def kfp_step_function(
    cmd_template: str,
    kfp_run_id: str,
    metaflow_configs: Dict[str, str],
    passed_in_split_indexes: str = '""',  # only if is_inside_foreach
    preceding_component_inputs: List[
        str
    ] = None,  # fields to return from Flow state to KFP
    preceding_component_outputs: List[
        str
    ] = None,  # fields to be pushed into Flow state from KFP
    flow_parameters_json: str = None,  # json formatted string
    **kwargs,
) -> object:
    """
    Renders and runs the cmd_template containing Metaflow step/init commands to
    run within the container.

    Returns: namedtuple(["foreach_splits"] + preceding_component_inputs)
    """
    import os
    import json
    import logging
    from subprocess import Popen
    from collections import namedtuple
    from typing import Dict

    if preceding_component_inputs is None:
        preceding_component_inputs = []
    if preceding_component_outputs is None:
        preceding_component_outputs = []

    # expose passed KFP passed in arguments as environment variables to
    # the bash command
    preceding_component_outputs_env: Dict[str, str] = {
        field: kwargs[field] for field in preceding_component_outputs
    }

    cmd = cmd_template.format(
        run_id=kfp_run_id,
        passed_in_split_indexes=passed_in_split_indexes,
    )

    metaflow_configs_new = {
        name: value for name, value in metaflow_configs.items() if value
    }

    if not "METAFLOW_USER" in metaflow_configs_new:
        metaflow_configs_new["METAFLOW_USER"] = "kfp-user"

    env = {
        **os.environ,
        **metaflow_configs_new,
        "PRECEDING_COMPONENT_INPUTS": json.dumps(preceding_component_inputs),
        "PRECEDING_COMPONENT_OUTPUTS": json.dumps(preceding_component_outputs),
        **preceding_component_outputs_env,
    }
    if flow_parameters_json is not None:
        env["METAFLOW_PARAMETERS"] = flow_parameters_json

    # TODO: Map username to KFP specific user/profile/namespace
    # Running Metaflow
    # KFP orchestrator -> running MF runtime (runs user code, handles state)
    with Popen(
        cmd, shell=True, universal_newlines=True, executable="/bin/bash", env=env
    ) as process:
        pass

    if process.returncode != 0:
        logging.info(f"---- Following command returned: {process.returncode}")
        logging.info(cmd.replace(" && ", "\n"))
        logging.info("----")
        raise Exception("Returned: %s" % process.returncode)

    task_context_dict = {}
    # File written by kfp_decorator.py:task_finished
    KFP_METAFLOW_FOREACH_SPLITS_PATH = "/tmp/kfp_metaflow_foreach_splits_dict.json"
    if os.path.exists(KFP_METAFLOW_FOREACH_SPLITS_PATH):  # is a foreach step
        with open(KFP_METAFLOW_FOREACH_SPLITS_PATH, "r") as file:
            task_context_dict = json.load(file)

    # json serialize foreach_splits else, the NamedTuple gets serialized
    # as string and we get the following error:
    #   withParam value could not be parsed as a JSON list: ['0', '1']
    values = [json.dumps(task_context_dict.get("foreach_splits", []))]

    # read fields to return from Flow state to KFP
    preceding_component_inputs_dict = {}
    if len(preceding_component_inputs) > 0:
        preceding_component_inputs_PATH = "/tmp/preceding_component_inputs.json"
        with open(preceding_component_inputs_PATH, "r") as file:
            preceding_component_inputs_dict = json.load(file)
            values += list(preceding_component_inputs_dict.values())

    ret = namedtuple(
        "StepOpRet", ["foreach_splits"] + list(preceding_component_inputs_dict.keys())
    )(*values)
    return ret
