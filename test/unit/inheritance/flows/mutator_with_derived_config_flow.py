"""
FlowMutator in base class using config from derived class.

Tests that a FlowMutator defined on a base class can access and use
config values defined in derived classes (later in the hierarchy).
"""

from metaflow import step, Parameter, project, current
from mutator_with_derived_config_base import BaseC


@project(name="mutator_with_derived_config_flow")
class MutatorWithDerivedConfigFlow(BaseC):
    """
    Flow testing FlowMutator from base class using config from derived class.

    Verifies:
    - Base class mutator can access derived class config
    - Parameters are injected based on derived config values
    - All original parameters and configs remain accessible
    """

    final_param = Parameter("final_param", help="Final parameter", default=999)

    @step
    def start(self):
        """Verify all parameters including those injected by base mutator"""
        print("Starting MutatorWithDerivedConfigFlow")

        # Original parameters
        print(f"base_param: {self.base_param}")
        print(f"middle_param: {self.middle_param}")
        print(f"final_param: {self.final_param}")

        # Configs
        print(f"middle_config: {self.middle_config}")
        print(f"runtime_config: {self.runtime_config}")

        # Injected parameters (from derived config)
        print(f"feature_logging (injected): {self.feature_logging}")
        print(f"feature_metrics (injected): {self.feature_metrics}")
        print(f"worker_count (injected): {self.worker_count}")

        # Store for verification
        self.result_base_param = self.base_param
        self.result_middle_param = self.middle_param
        self.result_final_param = self.final_param
        self.result_middle_config = dict(self.middle_config)
        self.result_runtime_config = dict(self.runtime_config)

        # Injected params
        self.result_feature_logging = self.feature_logging
        self.result_feature_metrics = self.feature_metrics
        self.result_worker_count = self.worker_count

        # Compute using injected params
        enabled_features = sum(
            [self.feature_logging, self.feature_metrics]
        )  # Count of True values
        self.result_computation = (
            self.worker_count * enabled_features + self.final_param
        )

        self.next(self.end)

    @step
    def end(self):
        """End step"""
        print(f"Computation result: {self.result_computation}")
        print(f"Pathspec: {current.pathspec}")
        print("MutatorWithDerivedConfigFlow completed successfully")


if __name__ == "__main__":
    MutatorWithDerivedConfigFlow()
