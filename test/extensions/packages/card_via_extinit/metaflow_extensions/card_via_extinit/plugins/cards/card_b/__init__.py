from metaflow.cards import MetaflowCard


class TestMockCard(MetaflowCard):
    type = "card_ext_init_b"

    def __init__(self, options=None, components=None, graph=None):
        if options is None:
            options = {"key": "task"}
        self._key = options["key"] if "key" in options else "task"

    def render(self, task):
        task_data = task[self._key].data
        return "%s" % task_data


CARDS = [TestMockCard]
