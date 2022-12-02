from .card import MetaflowCard, MetaflowCardComponent


class TestStringComponent(MetaflowCardComponent):
    def __init__(self, text):
        self._text = text

    def render(self):
        return str(self._text)


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
