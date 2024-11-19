import pickle

from metaflow._vendor import click

from ..cli import echo_always, echo_dev_null
from ..datastore import TaskDataStoreSet
from ..exception import CommandException


@click.command(
    help="Get data artifacts of a task or all tasks in a step. "
    "The format for input-path is either <run_id>/<step_name> or "
    "<run_id>/<step_name>/<task_id>."
)
@click.argument("input-path")
@click.option(
    "--private/--no-private",
    default=False,
    show_default=True,
    help="Show also private attributes.",
)
@click.option(
    "--max-value-size",
    default=1000,
    show_default=True,
    type=int,
    help="Show only values that are smaller than this number. "
    "Set to 0 to see only keys.",
)
@click.option(
    "--include",
    type=str,
    default="",
    help="Include only artifacts in the given comma-separated list.",
)
@click.option(
    "--file", type=str, default=None, help="Serialize artifacts in the given file."
)
@click.pass_obj
def dump(obj, input_path, private=None, max_value_size=None, include=None, file=None):

    if obj.is_quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    output = {}
    kwargs = {
        "show_private": private,
        "max_value_size": max_value_size,
        "include": {t for t in include.split(",") if t},
    }

    # Pathspec can either be run_id/step_name or run_id/step_name/task_id.
    parts = input_path.split("/")
    if len(parts) == 2:
        run_id, step_name = parts
        task_id = None
    elif len(parts) == 3:
        run_id, step_name, task_id = parts
    else:
        raise CommandException(
            "input_path should either be run_id/step_name or run_id/step_name/task_id"
        )

    datastore_set = TaskDataStoreSet(
        obj.flow_datastore,
        run_id,
        steps=[step_name],
        prefetch_data_artifacts=kwargs.get("include"),
    )
    if task_id:
        ds_list = [datastore_set.get_with_pathspec(input_path)]
    else:
        ds_list = list(datastore_set)  # get all tasks

    for ds in ds_list:
        echo(
            "Dumping output of run_id=*{run_id}* "
            "step=*{step}* task_id=*{task_id}*".format(
                run_id=ds.run_id, step=ds.step_name, task_id=ds.task_id
            ),
            fg="magenta",
        )

        if file is None:
            echo_always(
                ds.format(**kwargs), highlight="green", highlight_bold=False, err=False
            )
        else:
            output[ds.pathspec] = ds.to_dict(**kwargs)

    if file is not None:
        with open(file, "wb") as f:
            pickle.dump(output, f, protocol=pickle.HIGHEST_PROTOCOL)
        echo("Artifacts written to *%s*" % file)
