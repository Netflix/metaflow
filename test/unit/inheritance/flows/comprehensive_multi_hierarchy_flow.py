"""
Comprehensive multiple inheritance from independent hierarchies.

Tests: Two separate inheritance chains that merge, with parameters, configs,
decorators, mutators, and step overrides.

Structure:
- FlowSpec -> BaseA -> BaseB (hierarchy 1)
- FlowSpec -> BaseX -> BaseY (hierarchy 2)
- BaseB + BaseY -> BaseC -> ComprehensiveMultiHierarchyFlow
"""

from metaflow import step, Parameter, project, current
from comprehensive_multi_hierarchy_base import BaseC


@project(name="comprehensive_multi_hierarchy_flow")
class ComprehensiveMultiHierarchyFlow(BaseC):
    """
    Comprehensive multi-hierarchy inheritance flow.

    Verifies:
    - Parameters from both hierarchies (param_a, param_b, param_x, param_y, param_c, final_param)
    - Configs from both hierarchies (config_a, config_x, config_y, config_c)
    - Mutator from first hierarchy executes
    - Decorated step from first hierarchy works
    - Step override in BaseC is used (not BaseY's version)
    - Computations use values from both hierarchies
    """

    final_param = Parameter("final_param", help="Final parameter", default="merged")

    @step
    def end(self):
        """End step storing all verification artifacts"""
        # Store parameters from first hierarchy
        self.result_param_a = self.param_a
        self.result_param_b = self.param_b

        # Store parameters from second hierarchy
        self.result_param_x = self.param_x
        self.result_param_y = self.param_y

        # Store merge point parameter
        self.result_param_c = self.param_c
        self.result_final_param = self.final_param

        # Store configs from first hierarchy
        self.result_config_a = dict(self.config_a)

        # Store configs from second hierarchy
        self.result_config_x = dict(self.config_x)
        self.result_config_y = dict(self.config_y)

        # Store merge point config
        self.result_config_c = dict(self.config_c)

        # Store computed values
        self.result_final = self.processed_value

        # Compute cross-hierarchy value
        self.result_cross_hierarchy = (
            self.param_a + self.param_b + self.param_x + self.param_y + self.param_c
        )

        print(f"Final result: {self.result_final}")
        print(f"Cross-hierarchy sum: {self.result_cross_hierarchy}")
        print(f"Pathspec: {current.pathspec}")
        print("ComprehensiveMultiHierarchyFlow completed successfully")


if __name__ == "__main__":
    ComprehensiveMultiHierarchyFlow()
