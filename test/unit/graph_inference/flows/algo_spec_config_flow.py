from metaflow import Parameter, Config, conda_base
from metaflow.algospec import AlgoSpec


@conda_base(python="3.10")
class ConfigAlgoSpec(AlgoSpec):
    """AlgoSpec with Config and @conda_base flow decorator."""

    config = Config("config", default="config.json")

    multiplier = Parameter("multiplier", type=float, default=2.0)

    def call(self):
        scale = self.config["scale"]
        self.result = 5**2 * self.multiplier * scale


if __name__ == "__main__":
    ConfigAlgoSpec()
