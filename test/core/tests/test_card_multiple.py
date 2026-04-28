"""Multiple @card decorators on a step write multiple cards."""

from metaflow import FlowSpec, card, step


class CardMultipleFlow(FlowSpec):
    @card(type="default", id="first")
    @card(type="default", id="second")
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_card_multiple(metaflow_runner, executor):
    result = metaflow_runner(CardMultipleFlow, executor=executor)
    assert result.successful, result.stderr

    from metaflow.cards import get_cards

    container = get_cards(result.run()["start"].task)
    ids = {c.id for c in container}
    assert {"first", "second"}.issubset(ids)
