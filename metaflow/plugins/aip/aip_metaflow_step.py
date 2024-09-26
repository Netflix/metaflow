import json
import logging
import os
import pathlib
from subprocess import Popen
from typing import Dict, List

from metaflow._vendor import click
from metaflow.mflog import (
    BASH_SAVE_LOGS,
    bash_capture_logs,
    export_mflog_env_vars,
)
from metaflow.plugins.aip.aip_constants import (
    INPUT_PATHS_ENV_NAME,
    AIP_METAFLOW_FOREACH_SPLITS_PATH,
    LOGS_DIR,
    PRECEDING_COMPONENT_INPUTS_PATH,
    RETRY_COUNT,
    SPLIT_INDEX_ENV_NAME,
    STDERR_PATH,
    STDOUT_PATH,
    STEP_ENVIRONMENT_VARIABLES,
    TASK_ID_ENV_NAME,
    AIP_JOIN_METAFLOW_S3OP_NUM_WORKERS,
)
from metaflow.plugins.cards.card_client import get_cards, Card
from ... import R, metaflow_version


def _write_card_artifacts(
    flow_name: str,
    step_name: str,
    task_id: str,
    passed_in_split_indexes: str,
    run_id: str,
):
    task_id_template: str = f"{task_id}.{passed_in_split_indexes}".strip(".")
    pathspec = f"{flow_name}/{run_id}/{step_name}/{task_id_template}"

    cards: List[Card] = list(get_cards(pathspec))
    # sort such that the default card is first
    sorted_cards = sorted(
        cards, key=lambda card: (card.type != "default", cards.index(card))
    )

    pathlib.Path("/tmp/outputs/cards/").mkdir(parents=True, exist_ok=True)
    for index, card in enumerate(sorted_cards):
        file_name = f"/tmp/outputs/cards/card-{index}.html"
        try:
            with open(file_name, "w") as card_file:
                card_file.write(card.get())
        except Exception:
            logging.exception(f"Failed to write card {index} of type {card.type}")
            raise


def _step_cli(
    step_name: str,
    task_id: str,
    run_id: str,
    namespace: str,
    tags: List[str],
    sys_tags: List[str],
    is_split_index: bool,
    environment: str,
    event_logger: str,
    monitor: str,
    max_user_code_retries: int,
    workflow_name: str,
    script_name: str,
    add_default_card: bool,
) -> str:
    """
    Analogous to step_functions.py
    This returns the command line to run the internal Metaflow step click entrypiont.
    """
    cmds: List[str] = []

    executable: str = "python3" if R.use_r() else "python"

    if R.use_r():
        entrypoint = [R.entrypoint()]
    else:
        entrypoint = [executable, script_name]

    input_paths = None

    tags_extended: List[str] = []

    if tags:
        tags_extended.extend("--tag %s" % tag for tag in tags)
    if sys_tags:
        tags_extended.extend("--sys-tag %s" % tag for tag in sys_tags)

    if step_name == "start":
        # We need a separate unique ID for the special _parameters task
        task_id_params = "1-params"

        # Export user-defined parameters into runtime environment
        param_file = "parameters.sh"
        # TODO: move to AIP plugin
        export_params = (
            "python -m "
            "metaflow.plugins.aip.set_batch_environment "
            "parameters --output_file %s && . `pwd`/%s" % (param_file, param_file)
        )
        params: List[str] = entrypoint + [
            "--quiet",
            "--environment=%s" % environment,
            "--datastore=s3",
            "--datastore-root=$METAFLOW_DATASTORE_SYSROOT_S3",
            "--event-logger=%s" % event_logger,
            "--monitor=%s" % monitor,
            "--no-pylint",
            "init",
            "--run-id %s" % run_id,
            "--task-id %s" % task_id_params,
        ]

        params.extend(tags_extended)

        # If the start step gets retried, we must be careful not to
        # regenerate multiple parameters tasks. Hence we check first if
        # _parameters exists already.
        input_paths: str = "{run_id}/_parameters/{task_id_params}".format(
            run_id=run_id, task_id_params=task_id_params
        )
        exists: List[str] = entrypoint + [
            "dump",
            "--max-value-size=0",
            input_paths,
        ]
        param_cmd: str = "if ! %s >/dev/null 2>/dev/null; then %s && %s; fi" % (
            " ".join(exists),
            export_params,
            " ".join(params),
        )
        cmds.append(param_cmd)

    top_level: List[str] = [
        "--quiet",
        "--environment=%s" % environment,
        "--datastore=s3",
        "--datastore-root=$METAFLOW_DATASTORE_SYSROOT_S3",
        "--event-logger=%s" % event_logger,
        "--monitor=%s" % monitor,
        "--no-pylint",
    ]

    cmds.append(
        " ".join(
            entrypoint
            + top_level
            + [
                "aip step-init",
                "--run-id %s" % run_id,
                "--step_name %s" % step_name,
                '--passed_in_split_indexes "{passed_in_split_indexes}"',
                "--task_id %s" % task_id,  # the assigned task_id from Flow graph
            ]
        )
    )

    # load environment variables set in STEP_ENVIRONMENT_VARIABLES
    cmds.append(f". {STEP_ENVIRONMENT_VARIABLES}")

    step: List[str] = [
        "--with=aip",
        "--with 'card:id=default'" if add_default_card else "",
        "step",
        step_name,
        "--run-id %s" % run_id,
        f"--task-id ${TASK_ID_ENV_NAME}",
        f"--retry-count ${RETRY_COUNT}",
        "--max-user-code-retries %d" % max_user_code_retries,
        (
            "--input-paths %s" % input_paths
            if step_name == "start"
            else f"--input-paths ${INPUT_PATHS_ENV_NAME}"
        ),
    ]

    if is_split_index:
        step.append(f"--split-index ${SPLIT_INDEX_ENV_NAME}")

    step.extend(tags_extended)

    if namespace:
        step.append("--namespace %s" % namespace)

    cmds.append(" ".join(entrypoint + top_level + step))
    step_cli_string = " && ".join(cmds)
    return step_cli_string


def _command(
    volume_dir: str,
    step_cli: str,
    task_id: str,
    passed_in_split_indexes: str,
    step_name: str,
    flow_name: str,
    is_interruptible: bool,
) -> str:
    """
    Analogous to batch.py
    """
    retry_count_python: str = (
        "import os;"
        'name = os.environ.get("MF_ARGO_NODE_NAME");'
        'index = name.rfind("(");'
        'retry_count = (0 if index == -1 else name[index + 1: -1]) if name.endswith(")") else 0;'
        "print(str(retry_count))"
    )

    task_id_template: str = f"{task_id}.{passed_in_split_indexes}".strip(".")
    mflog_expr: str = export_mflog_env_vars(
        flow_name=flow_name,
        run_id="{run_id}",
        step_name=step_name,
        task_id=task_id_template,
        retry_count=f"`python -c '{retry_count_python}'`",
        datastore_type="s3",
        datastore_root="$METAFLOW_DATASTORE_SYSROOT_S3",
        stdout_path=STDOUT_PATH,
        stderr_path=STDERR_PATH,
    )

    if volume_dir and not is_interruptible:
        clean_volume_cmd: str = f"rm -rf {os.path.join(volume_dir, '*')}"
    else:
        clean_volume_cmd: str = "true"

    # construct an entry point that
    # 1) Clean attached volume if any
    # 2) Initializes the mflog environment (mflog_expr)
    # 3) Executes a task (step_expr)
    cmd_str: str = (
        f"{clean_volume_cmd} "
        f"&& mkdir -p {LOGS_DIR} && {mflog_expr} "
        f"&& {bash_capture_logs(step_cli)};"
    )

    # after the task has finished, we save its exit code (fail/success)
    # and persist the final logs. The whole entrypoint should exit
    # with the exit code (c) of the task.
    #
    # Note that if step_expr OOMs, this tail expression is never executed.
    # We lose the last logs in this scenario.
    cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
    return cmd_str


@click.command()
@click.option("--volume_dir")
@click.option("--environment")
@click.option("--is_foreach_step/--not_foreach_step", default=False)
@click.option("--flow_name")
@click.option("--flow_parameters_json", required=False, default="")
@click.option("--event_logger")
@click.option("--metaflow_configs_json")
@click.option("--metaflow_run_id")
@click.option("--monitor")
@click.option("--namespace", required=False, default="")
@click.option("--is_split_index/--no-need_split_index", default=False)
@click.option("--passed_in_split_indexes")
@click.option("--preceding_component_inputs_json")
@click.option("--preceding_component_outputs_json")
@click.option("--preceding_component_outputs_dict")
@click.option("--script_name")
@click.option("--step_name")
@click.option("--tags_json")
@click.option("--sys_tags_json")
@click.option("--task_id")
@click.option("--user_code_retries", type=int)
@click.option("--workflow_name")
@click.option("--is-interruptible/--not-interruptible", default=False)
@click.option("--is-join-step", is_flag=True, default=False)
@click.option("--add-default-cards", is_flag=True, default=False)
def aip_metaflow_step(
    volume_dir: str,
    environment: str,
    flow_name: str,
    flow_parameters_json: str,  # json formatted string
    is_foreach_step: bool,
    event_logger: str,
    metaflow_configs_json: str,
    metaflow_run_id: str,
    monitor: str,
    namespace: str,
    is_split_index: bool,
    passed_in_split_indexes: str,  # only if is_inside_foreach
    preceding_component_inputs_json: str,  # fields to return from Flow state to KFP
    preceding_component_outputs_json: str,  # fields to be pushed into Flow state from KFP
    preceding_component_outputs_dict: str,  # json string of type Dict[str, str]
    script_name: str,
    step_name: str,
    tags_json: str,
    sys_tags_json: str,
    task_id: str,
    user_code_retries: int,
    workflow_name: str,
    is_interruptible: bool,
    is_join_step: bool,
    add_default_cards: bool,
) -> None:
    """
    (1) Renders and runs the Metaflow package_commands and Metaflow step
    (2) Writes output of the Metaflow step function to ensure subsequent KFP steps
        have access to these outputs.
    """
    metaflow_configs: Dict[str, str] = json.loads(metaflow_configs_json)
    tags: List[str] = json.loads(tags_json)
    sys_tags: List[str] = json.loads(sys_tags_json)
    preceding_component_inputs: List[str] = json.loads(preceding_component_inputs_json)
    preceding_component_outputs: List[str] = json.loads(
        preceding_component_outputs_json
    )

    if volume_dir == "":
        volume_dir = None

    step_cli: str = _step_cli(
        step_name,
        task_id,
        metaflow_run_id,
        namespace,
        tags,
        sys_tags,
        is_split_index,
        environment,
        event_logger,
        monitor,
        user_code_retries,
        workflow_name,
        script_name,
        add_default_cards,
    )

    # expose passed KFP passed in arguments as environment variables to
    # the bash command
    kwargs: Dict[str, str] = {}
    for arg in preceding_component_outputs_dict.split(","):
        if arg:  # ensure arg is not an empty string
            key, value = arg.split("=")
            kwargs[key] = value
    preceding_component_outputs_env: Dict[str, str] = {
        field: kwargs[field] for field in preceding_component_outputs
    }
    cmd_template: str = _command(
        volume_dir,
        step_cli,
        task_id,
        passed_in_split_indexes,
        step_name,
        flow_name,
        is_interruptible,
    )
    step_cmd: str = cmd_template.format(
        run_id=metaflow_run_id,
        passed_in_split_indexes=passed_in_split_indexes,
    )

    metaflow_configs_new: Dict[str, str] = {
        name: value for name, value in metaflow_configs.items() if value
    }

    if (
        not "METAFLOW_USER" in metaflow_configs_new
        or metaflow_configs_new["METAFLOW_USER"] is None
    ):
        metaflow_configs_new["METAFLOW_USER"] = "aip-user"

    if is_join_step and "METAFLOW_S3OP_NUM_WORKERS" not in os.environ:
        # AIP-7487: Metaflow joins steps require lots of memory
        os.environ["METAFLOW_S3OP_NUM_WORKERS"] = str(
            AIP_JOIN_METAFLOW_S3OP_NUM_WORKERS
        )

    env: Dict[str, str] = {
        **os.environ,
        **metaflow_configs_new,
        "PRECEDING_COMPONENT_INPUTS": json.dumps(preceding_component_inputs),
        "PRECEDING_COMPONENT_OUTPUTS": json.dumps(preceding_component_outputs),
        **preceding_component_outputs_env,
        "METAFLOW_VERSION": metaflow_version.get_version(),
    }
    if flow_parameters_json is not None:
        env["METAFLOW_PARAMETERS"] = flow_parameters_json

    # TODO: Map username to KFP specific user/profile/namespace
    # Running Metaflow runtime (runs user code, handles state)
    with Popen(
        step_cmd, shell=True, universal_newlines=True, executable="/bin/bash", env=env
    ) as process:
        pass

    if process.returncode != 0:
        logging.info(f"---- Following command returned: {process.returncode}")
        logging.info(step_cmd.replace(" && ", "\n"))
        logging.info("----")
        raise Exception("Returned: %s" % process.returncode)

    values: List[str] = []
    output_paths: List[str] = []
    if is_foreach_step:
        task_context_dict = {}
        # File written by aip_decorator.py:task_finished
        if os.path.exists(AIP_METAFLOW_FOREACH_SPLITS_PATH):  # is a foreach step
            with open(AIP_METAFLOW_FOREACH_SPLITS_PATH, "r") as file:
                task_context_dict = json.load(file)

        # json serialize foreach_splits else, the NamedTuple gets serialized
        # as string and we get the following error:
        #   withParam value could not be parsed as a JSON list: ['0', '1']
        values.append(json.dumps(task_context_dict.get("foreach_splits", [])))
        output_paths.append("/tmp/outputs/foreach_splits")

    # read fields to return from Flow state to KFP
    if len(preceding_component_inputs) > 0:
        # File written by aip_decorator.py:task_finished
        with open(PRECEDING_COMPONENT_INPUTS_PATH, "r") as file:
            preceding_component_inputs_dict: Dict = json.load(file)
            values.extend(list(preceding_component_inputs_dict.values()))

    # We replicate what is done in the KFP SDK _container_op.py,
    # see: https://github.com/kubeflow/pipelines/blob/master/sdk/python/kfp/dsl/_container_op.py
    # We write outputs to a tmp file, which KFP internally uses to produces the output
    # of the container op.
    for preceding_component_input in preceding_component_inputs:
        output_paths.append(f"/tmp/outputs/{preceding_component_input}")

    # Write all the outputs of the aip_step_function into the appropriate
    # output files which KFP uses to produce outputs for the container op.
    for idx, output_path in enumerate(output_paths):
        output_file = os.path.join(output_path, "data")
        pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            f.write(str(values[idx]))

    # Write card manifest (html) to Argo output artifact path.
    try:
        _write_card_artifacts(
            flow_name,
            step_name,
            task_id,
            passed_in_split_indexes,
            metaflow_run_id,
        )
    except Exception as e:
        # Workflow should still succeed even if cards fail to render
        logging.exception(f"Failed to write card artifacts: {e}")


if __name__ == "__main__":
    aip_metaflow_step()
