import time
from metaflow import FlowSpec, step, Config, card


class CardConfigFlow(FlowSpec):

    config = Config("config", default_value="")

    @card(type=config.type)
    @step
    def start(self):
        print("card type", self.config.type)
        self.next(self.end)

    @step
    def end(self):
        print("full config", self.config)


if __name__ == "__main__":
    CardConfigFlow()
