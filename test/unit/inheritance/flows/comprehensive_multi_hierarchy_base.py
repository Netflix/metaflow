"""
Base classes for comprehensive multiple inheritance from independent hierarchies.

Structure:
- FlowSpec -> BaseA -> BaseB (hierarchy 1)
- FlowSpec -> BaseX -> BaseY (hierarchy 2)
- BaseB + BaseY -> BaseC
"""

import os

from metaflow import (
    FlowSpec,
    step,
    Parameter,
    Config,
    environment,
    FlowMutator,
    config_expr,
)


class LoggingMutator(FlowMutator):
    """Simple mutator that logs flow information"""

    def pre_mutate(self, mutable_flow):
        print("LoggingMutator: Analyzing flow structure")
        param_count = sum(1 for _ in mutable_flow.parameters)
        config_count = sum(1 for _ in mutable_flow.configs)
        print(f"  Found {param_count} parameters, {config_count} configs")
        mutable_flow.add_parameter(
            "logging_param_count",
            Parameter(
                "logging_param_count",
                help="Parameter to store the result count",
                default=param_count,
            ),
        )
        mutable_flow.add_parameter(
            "logging_config_count",
            Parameter(
                "logging_config_count",
                help="Parameter to store the result count",
                default=config_count,
            ),
        )


# First hierarchy
@LoggingMutator()
class BaseA(FlowSpec):
    """First hierarchy root with mutator"""

    param_a = Parameter("param_a", help="Parameter A", default=10)
    config_a = Config("config_a", default_value={"source": "hierarchy_a", "value": 100})


class BaseB(BaseA):
    """First hierarchy extension with step and parameter"""

    param_b = Parameter("param_b", help="Parameter B", default=20)

    @environment(vars={"SOURCE": config_expr("config_x.source")})
    @step
    def start(self):
        """Start step from first hierarchy"""
        print(f"Start from BaseB: param_a={self.param_a}, param_b={self.param_b}")
        config_val = self.config_a.get("value", 0)
        self.hierarchy_a_result = self.param_a + self.param_b + config_val
        self.source_from_var = os.environ.get("SOURCE")
        self.next(self.process)


# Second hierarchy
class BaseX(FlowSpec):
    """Second hierarchy root"""

    param_x = Parameter("param_x", help="Parameter X", default=30)
    config_x = Config(
        "config_x", default_value={"source": "hierarchy_x", "multiplier": 2}
    )


class BaseY(BaseX):
    """Second hierarchy extension with parameter and config"""

    param_y = Parameter("param_y", help="Parameter Y", default=40)
    config_y = Config("config_y", default_value={"enabled": True, "threshold": 50})

    @step
    def process(self):
        """Process step that can be overridden"""
        print("BaseY.process - default implementation")
        multiplier = self.config_x.get("multiplier", 1)
        self.processed_value = self.hierarchy_a_result * multiplier
        self.next(self.end)


# Merge point
class BaseC(BaseB, BaseY):
    """
    Combines both hierarchies with parameter, config, and step override.

    Overrides the process step from BaseY.
    """

    param_c = Parameter("param_c", help="Parameter C", default=5)
    config_c = Config("config_c", default_value={"merge": True, "offset": 200})

    @step
    def process(self):
        """Override process step with new logic"""
        print("BaseC.process - overridden implementation")
        multiplier = self.config_x.get("multiplier", 1)
        offset = self.config_c.get("offset", 0)
        threshold = self.config_y.get("threshold", 0)

        # Use values from both hierarchies
        base_value = self.hierarchy_a_result * multiplier
        if base_value > threshold:
            self.processed_value = base_value + offset + self.param_c
        else:
            self.processed_value = base_value + self.param_c

        print(f"Processed value: {self.processed_value}")
        self.next(self.end)
