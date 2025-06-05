from metaflow._vendor import click
from metaflow.exception import MetaflowException


@click.group()
def cli():
    pass


@cli.group(help="Commands related to managing the conda/pypi environments")
@click.pass_context
def environment(ctx):
    # the logger is configured in cli.py
    global echo
    echo = ctx.obj.echo


@environment.command(help="Resolve the environment(s)")
@click.option(
    "--step",
    "steps",
    multiple=True,
    default=[],
    help="Steps to resolve the environment for",
)
@click.option(
    "--force/--no-force",
    default=False,
    is_flag=True,
    help="Force re-resolving the environment(s)",
)
@click.pass_obj
def resolve(obj, steps, force=False):
    # possibly limiting steps to resolve. make sure its a list and not a tuple
    step_names = list(steps)

    steps = [step for step in obj.flow if (step.name in step_names) or not step_names]

    # Delete existing environments if we are rebuilding.
    if force:
        for step in steps:
            obj.environment.delete_environment(step)

    if not hasattr(obj.environment, "disable_cache"):
        raise MetaflowException("The environment does not support disabling the cache.")

    # Disable the cache before initializing if we are rebuilding.
    if force:
        obj.environment.disable_cache()
    obj.environment.init_environment(echo, only_steps=step_names)
