"""
Base classes for comprehensive linear inheritance pattern.

Hierarchy: FlowSpec -> BaseA -> BaseB -> BaseC
"""

from metaflow import FlowSpec, step, Parameter, Config, retry


class BaseA(FlowSpec):
    """Base class with parameters"""

    alpha = Parameter("alpha", help="Alpha parameter", default=10)
    beta = Parameter("beta", help="Beta parameter", default=5)


class BaseB(BaseA):
    """Middle class with config and decorated step"""

    config_b = Config("config_b", default_value={"multiplier": 3, "offset": 100})

    @retry(times=2)
    @step
    def start(self):
        """Start step with retry decorator"""
        print(f"Starting with alpha={self.alpha}, beta={self.beta}")
        self.start_value = self.alpha + self.beta
        print(f"Start value: {self.start_value}")
        self.next(self.process)


class BaseC(BaseB):
    """Another middle class with additional config and parameter"""

    gamma = Parameter("gamma", help="Gamma parameter", default=2.5)
    config_c = Config("config_c", default_value={"mode": "production", "debug": False})
