"""
Base classes for comprehensive diamond inheritance pattern.

Hierarchy: FlowSpec -> BaseA, FlowSpec -> BaseB, BaseA + BaseB -> BaseC
"""

from metaflow import FlowSpec, step, Parameter, Config, retry


class BaseA(FlowSpec):
    """First branch: parameters and config"""

    param_a = Parameter("param_a", help="Parameter from BaseA", default=100)
    config_a = Config("config_a", default_value={"branch": "A", "priority": 1})

    @retry(times=2)
    @step
    def start(self):
        """Start step from BaseA"""
        print(f"Start from BaseA: param_a={self.param_a}")
        self.value_a = self.param_a * self.config_a.get("priority", 1)
        self.next(self.process)


class BaseB(FlowSpec):
    """Second branch: different parameters and config"""

    param_b = Parameter("param_b", help="Parameter from BaseB", default=50)
    config_b = Config("config_b", default_value={"branch": "B", "weight": 2.5})


class BaseC(BaseA, BaseB):
    """Diamond merge point with additional features"""

    param_c = Parameter("param_c", help="Parameter from BaseC", default=25)
    config_c = Config("config_c", default_value={"mode": "diamond", "enabled": True})

    @step
    def process(self):
        """Process step using values from both branches"""
        weight = self.config_b.get("weight", 1.0)
        mode = self.config_c.get("mode", "unknown")

        print(f"Processing in {mode} mode")
        print(
            f"Using value_a={self.value_a}, param_b={self.param_b}, param_c={self.param_c}"
        )

        # Compute using values from all branches
        self.processed = self.value_a + (self.param_b * weight) + self.param_c

        self.next(self.end)
