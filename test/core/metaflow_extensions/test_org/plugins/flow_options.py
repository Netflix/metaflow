from metaflow.decorators import FlowDecorator
from metaflow import current


class FlowDecoratorWithOptions(FlowDecorator):
    name = "test_flow_decorator"

    options = {"foobar": dict(default=None, show_default=False, help="Test flag")}

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        current._update_env({"foobar_value": options["foobar"]})
