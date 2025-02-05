import time
from metaflow import FlowSpec, step, card, current, Config, Parameter, config_expr
from metaflow.cards import Image

BASE = "https://picsum.photos/id"


class ConfigurablePhotoFlow(FlowSpec):
    cfg = Config("config", default="photo_config.json")
    id = Parameter("id", default=cfg.id, type=int)
    size = Parameter("size", default=cfg.size, type=int)

    @card
    @step
    def start(self):
        import requests

        params = {k: v for k, v in self.cfg.style.items() if v}
        self.url = f"{BASE}/{self.id}/{self.size}/{self.size}"
        img = requests.get(self.url, params)
        current.card.append(Image(img.content))
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConfigurablePhotoFlow()
