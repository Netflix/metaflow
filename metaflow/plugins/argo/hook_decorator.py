from metaflow.decorators import FlowDecorator
import inspect

from metaflow.exception import MetaflowException
from metaflow.plugins.argo.exit_hooks import ScriptHook


class RunOnFinishDecorator(FlowDecorator):
    name = "run_on_finish"
    allow_multiple = True

    defaults = {
        "functions": [],
        "image": None,
        "language": "python",
        "on_success": None,
        "on_error": None,
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        on_success = self.attributes["on_success"]
        on_error = self.attributes["on_error"]

        if not on_success and not on_error:
            raise MetaflowException("Choose one of the options on_success/on_error")

        prefix = ""
        if on_success:
            prefix = "success"
        elif on_error:
            prefix = "error"

        self.hooks = []
        for fn in self.attributes["functions"]:
            self.hooks.append(
                ScriptHook(
                    name=f"{prefix}-{fn.__name__}",
                    language=self.attributes["language"],
                    source=inspect.getsource(fn),
                    image=self.attributes["image"],
                    on_success=self.attributes["on_success"],
                    on_error=self.attributes["on_error"],
                )
            )
