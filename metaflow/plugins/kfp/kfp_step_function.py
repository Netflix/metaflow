def kfp_step_function(
    datastore_root: str,
    cmd_template: str,
    kfp_run_id: str,
    passed_in_split_indexes: str = '""',  # only if is_inside_foreach
    metaflow_service_url: str = "",
    flow_parameters_json: str = None,  # json formatted string
) -> list:
    """
    Renders and runs the cmd_template containing Metaflow step/init commands to
    run within the container.
    """
    import os
    import json
    from subprocess import Popen
    from metaflow.cli import logger

    cmd = cmd_template.format(
        run_id=kfp_run_id,
        datastore_root=datastore_root,
        passed_in_split_indexes=passed_in_split_indexes,
    )

    env = dict(
        os.environ,
        METAFLOW_USER="kfp-user",  # TODO: what should this be for a non-scheduled run?
        METAFLOW_SERVICE_URL=metaflow_service_url,
    )
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
        logger(f"---- Following command returned: {process.returncode}")
        logger(cmd.replace(" && ", "\n"))
        logger("----")
        raise Exception("Returned: %s" % process.returncode)

    # File written by kfp_decorator.py:task_finished
    KFP_METAFLOW_FOREACH_SPLITS_PATH = "/tmp/kfp_metaflow_foreach_splits_dict.json"
    if os.path.exists(KFP_METAFLOW_FOREACH_SPLITS_PATH):  # is a foreach step
        with open(KFP_METAFLOW_FOREACH_SPLITS_PATH, "r") as file:
            task_context_dict = json.load(file)
            return task_context_dict.get("foreach_splits")
