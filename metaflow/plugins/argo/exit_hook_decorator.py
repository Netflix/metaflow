import platform
from metaflow.decorators import FlowDecorator
import inspect

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import KUBERNETES_CONTAINER_IMAGE
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

        # If no docker image is explicitly specified, impute a default image.
        if not self.attributes["image"]:
            # If metaflow-config specifies a docker image, just use that.
            if KUBERNETES_CONTAINER_IMAGE:
                self.attributes["image"] = KUBERNETES_CONTAINER_IMAGE
            # If metaflow-config doesn't specify a docker image, assign a
            # default docker image.
            else:
                # Default to vanilla Python image corresponding to major.minor
                # version of the Python interpreter launching the flow.
                self.attributes["image"] = "python:%s.%s" % (
                    platform.python_version_tuple()[0],
                    platform.python_version_tuple()[1],
                )

        self.hooks = []
        for success_fn in self.attributes["on_success"]:
            self.hooks.append(
                ScriptHook(
                    name=f"success-{_sanitize_for_argo(success_fn.__name__)}",
                    source=_wrap_source_in_python_script(success_fn),
                    image=self.attributes["image"],
                    on_success=True,
                )
            )

        for error_fn in self.attributes["on_error"]:
            self.hooks.append(
                ScriptHook(
                    name=f"error-{_sanitize_for_argo(error_fn.__name__)}",
                    source=_wrap_source_in_python_script(error_fn),
                    image=self.attributes["image"],
                    on_error=True,
                )
            )


def _sanitize_for_argo(name: str):
    return name.replace("_", "-")


script_template = """
{src}

if __name__=="__main__":
    {name}()
"""


def _wrap_source_in_python_script(fn):
    src = inspect.getsource(fn)
    name = fn.__name__

    return script_template.format(src=src, name=name)
