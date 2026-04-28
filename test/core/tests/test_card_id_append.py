"""Cards on later steps don't overwrite cards from earlier steps."""

from metaflow import FlowSpec, card, step


class CardIdAppendFlow(FlowSpec):
    @card(type="default", id="my_id")
    @step
    def start(self):
        self.next(self.middle)

    @card(type="default", id="my_id")
    @step
    def middle(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_card_id_append(metaflow_runner, executor):
    result = metaflow_runner(CardIdAppendFlow, executor=executor)
    assert result.successful, result.stderr

    from metaflow.cards import get_cards

    run = result.run()
    start_card = get_cards(run["start"].task, id="my_id")
    middle_card = get_cards(run["middle"].task, id="my_id")
    assert len(start_card) >= 1
    assert len(middle_card) >= 1
    # The cards have the same id but live on different tasks.
    assert start_card[0].path != middle_card[0].path
