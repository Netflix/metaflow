"""
Base classes for FlowMutator in base class using config from derived class.

Tests that a FlowMutator defined on a base class can access and use
config values defined in derived classes (later in the hierarchy).
"""

from metaflow import FlowSpec, Parameter, Config, FlowMutator


class DerivedConfigMutator(FlowMutator):
    """
    Mutator that looks for configs anywhere in hierarchy and uses them.

    This demonstrates forward-looking: base class mutator using derived class config.
    """

    def init(self, target_config_name):
        self.target_config_name = target_config_name

    def pre_mutate(self, mutable_flow):
        """Inject parameters based on config that may be defined in derived classes"""
        from metaflow import Parameter

        print(f"DerivedConfigMutator: Searching for config '{self.target_config_name}'")

        # Search for the target config in the entire hierarchy
        config_value = None
        for name, config in mutable_flow.configs:
            if name == self.target_config_name:
                # Config is a dictionary-like object
                config_value = dict(config)
                print(f"Found config '{name}' with value: {config_value}")
                break

        if config_value:
            # Use config value to inject parameters
            if "features" in config_value:
                features = config_value["features"]
                print(f"Injecting feature parameters: {features}")
                for feature in features:
                    param_name = f"feature_{feature}"
                    mutable_flow.add_parameter(
                        param_name, Parameter(param_name, default=True)
                    )

            if "worker_count" in config_value:
                workers = config_value["worker_count"]
                print(f"Injecting worker_count parameter: {workers}")
                mutable_flow.add_parameter(
                    "worker_count", Parameter("worker_count", default=workers)
                )
        else:
            print(f"Config '{self.target_config_name}' not found, no injection")


@DerivedConfigMutator("runtime_config")
class BaseA(FlowSpec):
    """
    Base class with mutator that will use config from derived class.

    The mutator looks for 'runtime_config' which will be defined in BaseC (derived class).
    """

    base_param = Parameter("base_param", help="Base parameter", default="base_value")


class BaseB(BaseA):
    """Middle class with its own config and parameter"""

    middle_config = Config("middle_config", default_value={"env": "staging"})
    middle_param = Parameter("middle_param", help="Middle parameter", default=200)


class BaseC(BaseB):
    """
    Another middle class that defines the config the base mutator is looking for.

    This is the 'derived config' that the base class mutator will use.
    """

    runtime_config = Config(
        "runtime_config",
        default_value={"features": ["logging", "metrics"], "worker_count": 16},
    )
