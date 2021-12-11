from metaflow.cards import MetaflowCard


class MockCard(MetaflowCard):
    type = "mock_card"

    def __init__(self, options={"key": "dummy_key"}, **kwargs):
        self._key = options["key"]

    def render(self, task):
        task_data = task[self._key].data
        return "%s" % task_data


class ErrorCard(MetaflowCard):
    type = "error_card"

    # the render function will raise Exception
    def render(self, task):
        raise Exception("Unknown Things Happened")


class TimeoutCard(MetaflowCard):
    type = "timeout_card"

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


CARDS = [ErrorCard, TimeoutCard, MockCard]
