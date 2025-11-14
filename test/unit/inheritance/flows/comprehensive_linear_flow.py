"""
Comprehensive linear inheritance pattern testing all features.

Tests: FlowSpec -> BaseA -> BaseB -> BaseC -> ComprehensiveLinearFlow

Features:
- Parameters at multiple levels
- Configs at multiple levels
- Decorators on inherited steps
- Multi-step flow execution
"""

from metaflow import step, Parameter, project, current
from comprehensive_linear_base import BaseC


@project(name="comprehensive_linear_flow")
class ComprehensiveLinearFlow(BaseC):
    """
    Final flow testing comprehensive linear inheritance.

    Verifies:
    - All parameters from all levels accessible (alpha, beta, gamma, delta)
    - All configs from all levels accessible (config_b, config_c)
    - Decorated steps execute correctly
    - Computations using inherited values work
    """

    delta = Parameter("delta", help="Delta parameter", default="final")

    @step
    def process(self):
        """Process step using config values"""
        multiplier = self.config_b.get("multiplier", 1)
        offset = self.config_b.get("offset", 0)
        mode = self.config_c.get("mode", "test")

        print(f"Processing in {mode} mode")
        self.processed_value = self.start_value * multiplier + offset
        self.combined_params = f"{self.delta}_with_gamma_{self.gamma}"

        print(f"Processed value: {self.processed_value}")

        self.next(self.end)

    @step
    def end(self):
        """End step storing all verification artifacts"""
        # Store parameters for verification
        self.result_alpha = self.alpha
        self.result_beta = self.beta
        self.result_gamma = self.gamma
        self.result_delta = self.delta

        # Store configs
        self.result_config_b = dict(self.config_b)
        self.result_config_c = dict(self.config_c)

        # Store computed values
        self.result_final = self.processed_value

        print(f"Final result: {self.result_final}")
        print(f"Pathspec: {current.pathspec}")
        print("ComprehensiveLinearFlow completed successfully")


if __name__ == "__main__":
    ComprehensiveLinearFlow()
