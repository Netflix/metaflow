from metaflow import FlowSpec, card, current, step
from metaflow.cards import Markdown


class CustomNamedCardFlow(FlowSpec):
    @card
    @step(start=True)
    def begin(self):
        current.card.append(Markdown("# Custom graph"))
        self.value = 1
        self.next(self.middle)

    @step
    def middle(self):
        self.value += 1
        self.next(self.finish)

    @step(end=True)
    def finish(self):
        self.value += 1


if __name__ == "__main__":
    CustomNamedCardFlow()
