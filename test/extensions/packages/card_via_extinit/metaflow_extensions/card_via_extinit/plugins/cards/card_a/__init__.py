from metaflow.cards import MetaflowCard


class TestMockCard(MetaflowCard):
    type = "card_ext_init_a"

    def __init__(self, options={"key": "dummy_key"}, **kwargs):
        self._key = options["key"]

    def render(self, task):
        task_data = task[self._key].data
        return "%s" % task_data


CARDS = [TestMockCard]
