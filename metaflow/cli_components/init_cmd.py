from metaflow._vendor import click

from .. import parameters
from ..runtime import NativeRuntime


@parameters.add_custom_parameters(deploy_mode=False)
@click.command(help="Internal command to initialize a run.", hidden=True)
@click.option(
    "--run-id",
    default=None,
    required=True,
    help="ID for one execution of all steps in the flow.",
)
@click.option(
    "--task-id", default=None, required=True, help="ID for this instance of the step."
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Tags for this instance of the step.",
)
@click.pass_obj
def init(obj, run_id=None, task_id=None, tags=None, **kwargs):
    # init is a separate command instead of an option in 'step'
    # since we need to capture user-specified parameters with
    # @add_custom_parameters. Adding custom parameters to 'step'
    # is not desirable due to the possibility of name clashes between
    # user-specified parameters and our internal options. Note that
    # user-specified parameters are often defined as environment
    # variables.

    obj.metadata.add_sticky_tags(tags=tags)

    runtime = NativeRuntime(
        obj.flow,
        obj.graph,
        obj.flow_datastore,
        obj.metadata,
        obj.environment,
        obj.package,
        obj.logger,
        obj.entrypoint,
        obj.event_logger,
        obj.monitor,
        run_id=run_id,
        skip_decorator_hooks=True,
    )
    obj.flow._set_constants(obj.graph, kwargs, obj.config_options)
    runtime.persist_constants(task_id=task_id)
