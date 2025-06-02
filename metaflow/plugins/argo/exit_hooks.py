from collections import defaultdict
import json


class Template(object):
    # https://argoproj.github.io/argo-workflows/fields/#template

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
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


class Http(object):
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
class HttpExitHook(Template):
    def __init__(
        self, name, url, method="GET", headers=None, body=None, success_condition=None
    ):
        super().__init__(name)

        http = Http(method).url(url)
        if headers is not None:
            for header, value in headers.items():
                http.header(header, value)

        if body is not None:
            http.body(json.dumps(body))

        if success_condition is not None:
            http.success_condition(success_condition)

        self.payload["http"] = http.to_json()
