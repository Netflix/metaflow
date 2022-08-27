from metaflow.decorators import FlowDecorator
from metaflow.metaflow_environment import InvalidEnvironmentException


class CondaFlowDecorator(FlowDecorator):
    """
    Specifies the Conda environment for all steps of the flow.

    Use `@conda_base` to set common libraries required by all
    steps and use `@conda` to specify step-specific additions.

    Parameters
    ----------
    libraries : Dict
        Libraries to use for this flow. The key is the name of the package
        and the value is the version to use (default: `{}`).
    channels : List
        Additional channels to use for this flow. You can typically specify a
        channel using <channel>:<package> as well but this does not work for
        non aliased channels (default: `[]`).
    python : string
        Version of Python to use, e.g. '3.7.4'
        (default: None, i.e. the current Python version).
    disabled : bool
        If set to True, disables Conda (default: False).
    """

    name = "conda_base"
    defaults = {"libraries": {}, "channels": [], "python": None, "disabled": None}

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@conda* decorator requires " "--environment=conda"
            )
