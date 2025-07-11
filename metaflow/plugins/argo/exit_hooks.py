from collections import defaultdict
import json
from typing import Dict, List


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

    def container(self, container):
        self.payload["container"] = container
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
                .expression("workflow.status == 'Error' || workflow.status == 'Failed'")
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


class ContainerHook(Hook):
    def __init__(
        self,
        name: str,
        container: Dict,
        service_account_name: str = None,
        on_success: bool = False,
        on_error: bool = False,
    ):
        self.template = _Template(name)

        if service_account_name is not None:
            self.template.service_account_name(service_account_name)

        self.template.container(container)

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
                .expression("workflow.status == 'Error' || workflow.status == 'Failed'")
                .template(self.template.name)
            )

        if not on_success and not on_error:
            # add an expressionless lifecycle hook
            self.lifecycle_hooks.append(_LifecycleHook(name).template(name))
