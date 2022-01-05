from .card import MetaflowCard, MetaflowCardComponent


class TestPathSpecCard(MetaflowCard):
    type = "test_pathspec_card"

    def render(self, task):
        import random

        return "%s %d" % (task.pathspec, random.randint(0, 100))


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
