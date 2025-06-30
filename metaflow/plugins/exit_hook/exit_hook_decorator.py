from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException


class ExitHookDecorator(FlowDecorator):
    name = "exit_hook"
    allow_multiple = True

    defaults = {
        "image": None,
        "on_success": [],
        "on_error": [],
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        on_success = self.attributes["on_success"]
        on_error = self.attributes["on_error"]

        if not on_success and not on_error:
            raise MetaflowException(
                "Choose at least one of the options on_success/on_error"
            )

        self.success_hooks = []
        self.error_hooks = []
        for success_fn in on_success:
            self.success_hooks.append(success_fn.__name__)

        for error_fn in on_error:
            self.error_hooks.append(error_fn.__name__)
