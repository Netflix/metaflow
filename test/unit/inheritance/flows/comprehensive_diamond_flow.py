"""
Comprehensive diamond inheritance pattern with all features.

Tests: FlowSpec -> BaseA, FlowSpec -> BaseB, BaseA + BaseB -> BaseC -> ComprehensiveDiamondFlow

Features:
- Parameters in both branches
- Configs in both branches
- Step methods in base classes
- Decorators on inherited steps
- MRO resolution
"""

from metaflow import step, Parameter, project, current
from comprehensive_diamond_base import BaseC


@project(name="comprehensive_diamond_flow")
class ComprehensiveDiamondFlow(BaseC):
    """
    Comprehensive diamond inheritance flow.

    Verifies:
    - MRO correctly resolves diamond pattern
    - Parameters from all branches accessible (param_a, param_b, param_c, final_param)
    - Configs from all branches accessible (config_a, config_b, config_c)
    - Steps from BaseA execute correctly
    - Computations use values from all branches
    """

    final_param = Parameter("final_param", help="Final parameter", default="complete")

    @step
    def end(self):
        """End step storing all verification artifacts"""
        # Store all parameters
        self.result_param_a = self.param_a
        self.result_param_b = self.param_b
        self.result_param_c = self.param_c
        self.result_final_param = self.final_param

        # Store all configs
        self.result_config_a = dict(self.config_a)
        self.result_config_b = dict(self.config_b)
        self.result_config_c = dict(self.config_c)

        # Store computed value
        self.result_final = self.processed

        print(f"Final result: {self.result_final}")
        print(f"Final param: {self.result_final_param}")
        print(f"Pathspec: {current.pathspec}")
        print("ComprehensiveDiamondFlow completed successfully")


if __name__ == "__main__":
    ComprehensiveDiamondFlow()
