from metaflow.decorators import FlowDecorator
import inspect

from metaflow.plugins.argo.exit_hooks import ScriptHook


class RunOnFinishDecorator(FlowDecorator):
    name = "run_on_finish"
    multiple = True

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
        self.hooks = []
        for fn in self.attributes["functions"]:
            self.hooks.append(
                ScriptHook(
                    name=f"success-{fn.__name__}",
                    language=self.attributes["language"],
                    source=inspect.getsource(fn),
                    image=self.attributes["image"],
                    on_success=self.attributes["on_success"],
                    on_error=self.attributes["on_error"],
                )
            )
