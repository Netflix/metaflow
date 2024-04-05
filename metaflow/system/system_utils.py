from typing import Union


class DummyFlow(object):
    def __init__(self, name="not_a_real_flow"):
        self.name = name


# This function is used to initialize the environment outside a flow.
def init_environment_outside_flow(
    flow: Union["metaflow.flowspec.FlowSpec", "metaflow.sidecar.DummyFlow"]
) -> "metaflow.metaflow_environment.MetaflowEnvironment":
    from metaflow.plugins import ENVIRONMENTS
    from metaflow.metaflow_config import DEFAULT_ENVIRONMENT
    from metaflow.metaflow_environment import MetaflowEnvironment

    return [
        e for e in ENVIRONMENTS + [MetaflowEnvironment] if e.TYPE == DEFAULT_ENVIRONMENT
    ][0](flow)
