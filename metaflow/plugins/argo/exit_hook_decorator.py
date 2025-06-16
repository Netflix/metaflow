from metaflow.decorators import FlowDecorator
import inspect

from metaflow.exception import MetaflowException
from metaflow.plugins.argo.exit_hooks import ScriptHook


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

        self.hooks = []
        for success_fn in self.attributes["on_success"]:
            self.hooks.append(
                ScriptHook(
                    name=f"success-{success_fn.__name__}",
                    source=inspect.getsource(success_fn),
                    image=self.attributes["image"],
                    on_success=True,
                )
            )

        for error_fn in self.attributes["on_error"]:
            self.hooks.append(
                ScriptHook(
                    name=f"error-{error_fn.__name__}",
                    source=inspect.getsource(error_fn),
                    image=self.attributes["image"],
                    on_error=True,
                )
            )
