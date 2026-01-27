from metaflow import FlowSpec, step, card, Parameter, current
from metaflow.cards import Markdown

import requests, pandas, string

URL = "https://upload.wikimedia.org/wikipedia/commons/4/45/Blue_Marble_rotating.gif"


class SimpleCardFlow(FlowSpec):
    number = Parameter("number", default=3)
    image_url = Parameter("image_url", default=URL)

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

        self.next(self.a)

    @step
    def a(self):
        print(f"image: {self.image_url}")
        self.image = requests.get(
            self.image_url, headers={"user-agent": "metaflow-example"}
        ).content
        self.dataframe = pandas.DataFrame(
            {
                "lowercase": list(string.ascii_lowercase),
                "uppercase": list(string.ascii_uppercase),
            }
        )
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    SimpleCardFlow()
