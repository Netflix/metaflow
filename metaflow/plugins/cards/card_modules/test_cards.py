import json
from .card import MetaflowCard, MetaflowCardComponent
from .renderer_tools import render_safely


class TestStringComponent(MetaflowCardComponent):
    REALTIME_UPDATABLE = True

    def __init__(self, text):
        self._text = text

    def render(self):
        return str(self._text)

    def update(self, text):
        self._text = text


class TestPathSpecCard(MetaflowCard):
    type = "test_pathspec_card"

    def render(self, task):
        import random
        import string

        return "%s %s" % (
            task.pathspec,
            "".join(
                random.choice(string.ascii_uppercase + string.digits) for _ in range(6)
            ),
        )


class TestEditableCard(MetaflowCard):
    type = "test_editable_card"

    seperator = "$&#!!@*"

    ALLOW_USER_COMPONENTS = True

    def __init__(self, options={}, components=[], graph=None):
        self._components = components

    def render(self, task):
        return self.seperator.join([str(comp) for comp in self._components])


class TestEditableCard2(MetaflowCard):
    type = "test_editable_card_2"

    seperator = "$&#!!@*"

    ALLOW_USER_COMPONENTS = True

    def __init__(self, options={}, components=[], graph=None):
        self._components = components

    def render(self, task):
        return self.seperator.join([str(comp) for comp in self._components])


class TestNonEditableCard(MetaflowCard):
    type = "test_non_editable_card"

    seperator = "$&#!!@*"

    def __init__(self, options={}, components=[], graph=None):
        self._components = components

    def render(self, task):
        return self.seperator.join([str(comp) for comp in self._components])


class TestMockCard(MetaflowCard):
    type = "test_mock_card"

    def __init__(self, options={"key": "dummy_key"}, **kwargs):
        self._key = options["key"]

    def render(self, task):
        task_data = task[self._key].data
        return "%s" % task_data


class TestErrorCard(MetaflowCard):
    type = "test_error_card"

    # the render function will raise Exception
    def render(self, task):
        raise Exception("Unknown Things Happened")


class TestTimeoutCard(MetaflowCard):
    type = "test_timeout_card"

    def __init__(self, options={"timeout": 50}, **kwargs):
        super().__init__()
        self._timeout = 10
        if "timeout" in options:
            self._timeout = options["timeout"]

    # the render function will raise Exception
    def render(self, task):
        import time

        time.sleep(self._timeout)
        return "%s" % task.pathspec


REFRESHABLE_HTML_TEMPLATE = """
<html>
<script> 
var METAFLOW_RELOAD_TOKEN = "[METAFLOW_RELOAD_TOKEN]"

window.metaflow_card_update = function(data) {
    document.querySelector("h1").innerHTML = JSON.stringify(data);
}
</script>
<h1>[PATHSPEC]</h1>
<h1>[REPLACE_CONTENT_HERE]</h1>
</html>
"""


class TestJSONComponent(MetaflowCardComponent):

    REALTIME_UPDATABLE = True

    def __init__(self, data):
        self._data = data

    @render_safely
    def render(self):
        return self._data

    def update(self, data):
        self._data = data


class TestRefreshCard(MetaflowCard):
    """
    This card takes no components and helps test the `current.card.refresh(data)` interface.
    """

    HTML_TEMPLATE = REFRESHABLE_HTML_TEMPLATE

    RUNTIME_UPDATABLE = True

    ALLOW_USER_COMPONENTS = True

    # Not implementing Reload Policy here since the reload Policy is set to always
    RELOAD_POLICY = MetaflowCard.RELOAD_POLICY_ALWAYS

    type = "test_refresh_card"

    def render(self, task) -> str:
        return self._render_func(task, self.runtime_data)

    def _render_func(self, task, data):
        return self.HTML_TEMPLATE.replace(
            "[REPLACE_CONTENT_HERE]", json.dumps(data["user"])
        ).replace("[PATHSPEC]", task.pathspec)

    def render_runtime(self, task, data):
        return self._render_func(task, data)

    def refresh(self, task, data):
        return data


import hashlib


def _component_values_to_hash(components):
    comma_str = ",".join(["".join(x) for v in components.values() for x in v])
    return hashlib.sha256(comma_str.encode("utf-8")).hexdigest()


class TestRefreshComponentCard(MetaflowCard):
    """
    This card takes components and helps test the `current.card.components["A"].update()`
    interface
    """

    HTML_TEMPLATE = REFRESHABLE_HTML_TEMPLATE

    RUNTIME_UPDATABLE = True

    ALLOW_USER_COMPONENTS = True

    # Not implementing Reload Policy here since the reload Policy is set to always
    RELOAD_POLICY = MetaflowCard.RELOAD_POLICY_ONCHANGE

    type = "test_component_refresh_card"

    def __init__(self, options={}, components=[], graph=None):
        self._components = components

    def render(self, task) -> str:
        # Calling `render`/`render_runtime` wont require the `data` object
        return self.HTML_TEMPLATE.replace(
            "[REPLACE_CONTENT_HERE]", json.dumps(self._components)
        ).replace("[PATHSPEC]", task.pathspec)

    def render_runtime(self, task, data):
        return self.render(task)

    def refresh(self, task, data):
        # Govers the information passed in the data update
        return data["components"]

    def reload_content_token(self, task, data):
        if task.finished:
            return "final"
        return "runtime-%s" % _component_values_to_hash(data["components"])
