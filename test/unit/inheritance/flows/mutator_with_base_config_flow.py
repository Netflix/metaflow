"""
FlowMutator using config from base class.

Tests that a FlowMutator can access and use config values defined in base classes.
"""

from metaflow import step, Parameter, project, current
from mutator_with_base_config_base import BaseB


@project(name="mutator_with_base_config_flow")
class MutatorWithBaseConfigFlow(BaseB):
    """
    Flow testing FlowMutator that uses config from base class.

    Verifies:
    - Mutator can access config from base class
    - Parameters are injected based on config values
    - Original parameters remain accessible
    """

    final_param = Parameter("final_param", help="Final parameter", default=50)

    @step
    def start(self):
        """Verify all parameters including injected ones"""
        print("Starting MutatorWithBaseConfigFlow")

        # Original parameters
        print(f"base_param: {self.base_param}")
        print(f"middle_param: {self.middle_param}")
        print(f"final_param: {self.final_param}")

        # Config used by mutator
        print(f"mutator_config: {self.mutator_config}")

        # Injected parameters
        print(f"dynamic_param (injected): {self.dynamic_param}")
        print(f"injected_count (injected): {self.injected_count}")

        # Store for verification
        self.result_base_param = self.base_param
        self.result_middle_param = self.middle_param
        self.result_final_param = self.final_param
        self.result_mutator_config = dict(self.mutator_config)
        self.result_dynamic_param = self.dynamic_param
        self.result_injected_count = self.injected_count

        # Compute using injected params
        self.result_computation = (
            self.middle_param + self.dynamic_param + self.injected_count
        )

        self.next(self.end)

    @step
    def end(self):
        """End step"""
        print(f"Computation result: {self.result_computation}")
        print(f"Pathspec: {current.pathspec}")
        print("MutatorWithBaseConfigFlow completed successfully")


if __name__ == "__main__":
    MutatorWithBaseConfigFlow()
