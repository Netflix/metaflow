from collections import defaultdict
import json
from typing import Callable, List


class JsonSerializable(object):
    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class _LifecycleHook(JsonSerializable):
    # https://argoproj.github.io/argo-workflows/fields/#lifecyclehook

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.name = name
        self.payload = tree()

    def expression(self, expression):
        self.payload["expression"] = str(expression)
        return self

    def template(self, template):
        self.payload["template"] = template
        return self


class _Template(JsonSerializable):
    # https://argoproj.github.io/argo-workflows/fields/#template

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.name = name
        self.payload = tree()
        self.payload["name"] = name

    def http(self, http):
        self.payload["http"] = http.to_json()
        return self

    def script(self, script):
        self.payload["script"] = script.to_json()
        return self

    def service_account_name(self, service_account_name):
        self.payload["serviceAccountName"] = service_account_name
        return self


class Hook(object):
    """
    Abstraction for Argo Workflows exit hooks.
    A hook consists of a Template, and one or more LifecycleHooks that trigger the template
    """

    template: "_Template"
    lifecycle_hooks: List["_LifecycleHook"]


class _ScriptSpec(JsonSerializable):
    # https://argo-workflows.readthedocs.io/en/latest/walk-through/scripts-and-results/

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def image(self, image):
        self.payload["image"] = image
        return self

    def command(self, command: list[str]):
        self.payload["command"] = command
        return self

    def source(self, source: str):
        # encode the source as a oneliner due to json limitations
        self.payload["source"] = json.dumps(source)
        return self


class _HttpSpec(JsonSerializable):
    # https://argoproj.github.io/argo-workflows/fields/#http

    def __init__(self, method):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["method"] = method
        self.payload["headers"] = []

    def header(self, header, value):
        self.payload["headers"].append({"name": header, "value": value})
        return self

    def body(self, body):
        self.payload["body"] = str(body)
        return self

    def url(self, url):
        self.payload["url"] = url
        return self

    def success_condition(self, success_condition):
        self.payload["successCondition"] = success_condition
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


# HTTP hook
class HttpExitHook(Hook):
    def __init__(
        self,
        name,
        url,
        method="GET",
        headers=None,
        body=None,
        on_success=False,
        on_error=False,
    ):
        self.template = _Template(name)
        http = _HttpSpec(method).url(url)
        if headers is not None:
            for header, value in headers.items():
                http.header(header, value)

        if body is not None:
            http.body(json.dumps(body))

        self.template.http(http)

        self.lifecycle_hooks = []

        if on_success and on_error:
            raise Exception("Set only one of the on_success/on_error at a time.")

        if on_success:
            self.lifecycle_hooks.append(
                _LifecycleHook(name)
                .expression("workflow.status == 'Succeeded'")
                .template(self.template.name)
            )

        if on_error:
            self.lifecycle_hooks.append(
                _LifecycleHook(name)
                .expression("workflow.status == 'Error'")
                .template(self.template.name)
            )
            self.lifecycle_hooks.append(
                _LifecycleHook(f"{name}-failure")
                .expression("workflow.status == 'Failure'")
                .template(self.template.name)
            )

        if not on_success and not on_error:
            # add an expressionless lifecycle hook
            self.lifecycle_hooks.append(_LifecycleHook(name).template(name))


class ExitHookHack(Hook):
    # Warning: terrible hack to workaround a bug in Argo Workflow where the
    #          templates listed above do not execute unless there is an
    #          explicit exit hook. as and when this bug is patched, we should
    #          remove this effectively no-op template.
    # Note: We use the Http template because changing this to an actual no-op container had the side-effect of
    # leaving LifecycleHooks in a pending state even when they have finished execution.
    def __init__(
        self,
        url,
        headers=None,
        body=None,
    ):
        self.template = _Template("exit-hook-hack")
        http = _HttpSpec("GET").url(url)
        if headers is not None:
            for header, value in headers.items():
                http.header(header, value)

        if body is not None:
            http.body(json.dumps(body))

        http.success_condition("true == true")

        self.template.http(http)

        self.lifecycle_hooks = []

        # add an expressionless lifecycle hook
        self.lifecycle_hooks.append(_LifecycleHook("exit").template("exit-hook-hack"))


class ScriptHook(Hook):
    def __init__(
        self,
        name: str,
        source: str,
        image: str = None,
        language: str = "python",
        on_success=False,
        on_error=False,
    ):
        self.template = _Template(name)
        script = _ScriptSpec().command([language]).source(source)
        if image is not None:
            script.image(image)

        self.template.script(script)

        self.lifecycle_hooks = []

        if on_success and on_error:
            raise Exception("Set only one of the on_success/on_error at a time.")

        if on_success:
            self.lifecycle_hooks.append(
                _LifecycleHook(name)
                .expression("workflow.status == 'Succeeded'")
                .template(self.template.name)
            )

        if on_error:
            self.lifecycle_hooks.append(
                _LifecycleHook(name)
                .expression("workflow.status == 'Error'")
                .template(self.template.name)
            )
            self.lifecycle_hooks.append(
                _LifecycleHook(f"{name}-failure")
                .expression("workflow.status == 'Failure'")
                .template(self.template.name)
            )

        if not on_success and not on_error:
            # add an expressionless lifecycle hook
            self.lifecycle_hooks.append(_LifecycleHook(name).template(name))
