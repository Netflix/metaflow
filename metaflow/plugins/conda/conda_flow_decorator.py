from metaflow.decorators import FlowDecorator
from metaflow.metaflow_environment import InvalidEnvironmentException

from typing import Dict


class CondaFlowDecorator(FlowDecorator):
    """
    Specifies the Conda environment for all steps of the flow.

    Use `@conda_base` to set common libraries required by all
    steps and use `@conda` to specify step-specific additions.

    Parameters
    ----------
    libraries : Dict[str, str], default: {}
        Libraries to use for this flow. The key is the name of the package
        and the value is the version to use.
    python : str, optional
        Version of Python to use, e.g. '3.7.4'. A default value of None means
        to use the current Python version.
    disabled : bool, default: False
        If set to True, disables Conda.
    """

    name = "conda_base"
    defaults = {"libraries": {}, "python": None, "disabled": None}

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@conda* decorator requires " "--environment=conda"
            )
