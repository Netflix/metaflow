from metaflow._vendor import click
from metaflow.tagging_util import validate_tags
from metaflow.metadata_provider import MetaDatum


@click.group()
def cli():
    pass


@cli.group(help="Commands related to spot metadata.")
def spot_metadata():
    pass


@spot_metadata.command(help="Record spot metadata for a task.")
@click.option(
    "--run-id",
    required=True,
    help="Run ID for which metadata is to be recorded.",
)
@click.option(
    "--step-name",
    required=True,
    help="Step Name for which metadata is to be recorded.",
)
@click.option(
    "--task-id",
    required=True,
    help="Task ID for which metadata is to be recorded.",
)
@click.option(
    "--field",
    multiple=True,
    required=True,
    type=(str, str),
    help="Metadata key value pairs.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    required=False,
    default=None,
    help="List of tags.",
)
@click.pass_obj
def record(obj, run_id, step_name, task_id, field, tags=None):
    validate_tags(tags)

    tag_list = list(tags) if tags else []

    entries = []
    for k, v in dict(field).items():
        entries.append(
            MetaDatum(
                field=k,
                value=v,
                type=k,
                tags=tag_list,
            )
        )

    obj.metadata.register_metadata(
        run_id=run_id, step_name=step_name, task_id=task_id, metadata=entries
    )
