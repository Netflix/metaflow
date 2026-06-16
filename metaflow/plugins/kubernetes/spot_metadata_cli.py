from metaflow._vendor import click
from datetime import datetime, timezone
from metaflow.tagging_util import validate_tags
from metaflow.metadata_provider import MetaDatum


@click.group()
def cli():
    pass


@cli.group(help="Commands related to spot metadata.")
def spot_metadata():
    pass


@spot_metadata.command(help="Record spot termination metadata for a task.")
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
    "--termination-notice-time",
    required=True,
    help="Spot termination notice time.",
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
def record(obj, run_id, step_name, task_id, termination_notice_time, tags=None):
    validate_tags(tags)

    tag_list = list(tags) if tags else []

    entries = [
        MetaDatum(
            field="spot-termination-received-at",
            value=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            type="spot-termination-received-at",
            tags=tag_list,
        ),
        MetaDatum(
            field="spot-termination-time",
            value=termination_notice_time,
            type="spot-termination-time",
            tags=tag_list,
        ),
    ]

    obj.metadata.register_metadata(
        run_id=run_id, step_name=step_name, task_id=task_id, metadata=entries
    )
