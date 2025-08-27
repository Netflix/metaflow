from metaflow import FlowSpec, step, card, Parameter, current
from metaflow.cards import Markdown


class SimpleCardFlow(FlowSpec):

    number = Parameter("number", default=3)

    @card(type="blank")
    @step
    def start(self):
        current.card.append(Markdown("# Guess my number"))
        if self.number > 5:
            current.card.append(Markdown("My number is **smaller** ⬇️"))
        elif self.number < 5:
            current.card.append(Markdown("My number is **larger** ⬆️"))
        else:
            current.card.append(Markdown("## Correct! 🎉"))
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    SimpleCardFlow()
