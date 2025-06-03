from collections import defaultdict
import json
from typing import List


class _LifecycleHook(object):
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

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class _Template(object):
    # https://argoproj.github.io/argo-workflows/fields/#template

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.name = name
        self.payload = tree()
        self.payload["name"] = name

    def container(self, container):
        # Luckily this can simply be V1Container and we are spared from writing more
        # boilerplate - https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Container.md.
        self.payload["container"] = container
        return self

    def http(self, http):
        self.payload["http"] = http.to_json()
        return self

    def service_account_name(self, service_account_name):
        self.payload["serviceAccountName"] = service_account_name
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Hook(object):
    """
    Abstraction for Argo Workflows exit hooks.
    A hook consists of a Template, and one or more LifecycleHooks that trigger the template
    """

    template: "_Template"
    lifecycle_hooks: List["_LifecycleHook"]


class _Http(object):
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
        success_condition=None,
        on_success=False,
        on_error=False,
    ):
        self.template = _Template(name)
        http = _Http(method).url(url)
        if headers is not None:
            for header, value in headers.items():
                http.header(header, value)

        if body is not None:
            http.body(json.dumps(body))

        if success_condition is not None:
            http.success_condition(success_condition)

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
