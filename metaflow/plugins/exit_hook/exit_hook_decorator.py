from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException


class ExitHookDecorator(FlowDecorator):
    name = "exit_hook"
    allow_multiple = True

    defaults = {
        "on_success": [],
        "on_error": [],
        "options": {},
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
            if isinstance(success_fn, str):
                self.success_hooks.append(success_fn)
            elif callable(success_fn):
                self.success_hooks.append(success_fn.__name__)
            else:
                raise ValueError(
                    "Exit hooks inside 'on_success' must be a function or a string referring to the function"
                )

        for error_fn in on_error:
            if isinstance(error_fn, str):
                self.error_hooks.append(error_fn)
            elif callable(error_fn):
                self.error_hooks.append(error_fn.__name__)
            else:
                raise ValueError(
                    "Exit hooks inside 'on_error' must be a function or a string referring to the function"
                )
