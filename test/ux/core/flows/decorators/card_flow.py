"""Flow for testing @card decorator creates a card artifact."""

from metaflow import FlowSpec, card, project, step


@project(name="card_flow")
class CardFlow(FlowSpec):
    @card
    @step
    def start(self):
        self.message = "hello from card flow"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    CardFlow()
