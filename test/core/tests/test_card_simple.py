"""@card writes a default-type card that the cards API can retrieve."""

from metaflow import FlowSpec, card, step


class CardSimpleFlow(FlowSpec):
    @card
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_card_simple(metaflow_runner, executor):
    result = metaflow_runner(CardSimpleFlow, executor=executor)
    assert result.successful, result.stderr

    from metaflow.cards import get_cards

    task = result.run()["start"].task
    container = get_cards(task)
    assert len(container) >= 1
