"""
Flow testing Config parameter names with underscores and dashes.

Tests that Config parameters can have names containing:
- Underscores only
- Dashes only
- Both underscores and dashes
"""

from metaflow import FlowSpec, Config, step


class ConfigNamingFlow(FlowSpec):
    """Test flow for Config names with underscores and dashes."""

    # Config with underscore in name
    config_with_underscore = Config(
        "config_with_underscore", default_value={"test": "underscore", "value": 42}
    )

    # Config with dash in name
    config_with_dash = Config(
        "config-with-dash", default_value={"test": "dash", "value": 99}
    )

    # Config with both underscore and dash in name
    config_mixed = Config(
        "config-with_both-mixed", default_value={"test": "mixed", "value": 123}
    )

    @step
    def start(self):
        """Access configs with different naming patterns and validate values."""
        # Access underscore config
        self.underscore_test = self.config_with_underscore.test
        self.underscore_value = self.config_with_underscore.value
        self.underscore_dict = dict(self.config_with_underscore)

        # Validate underscore config values
        assert (
            self.underscore_test == "underscore"
        ), f"Expected 'underscore', got {self.underscore_test}"
        assert self.underscore_value == 42, f"Expected 42, got {self.underscore_value}"
        assert self.underscore_dict == {
            "test": "underscore",
            "value": 42,
        }, f"Unexpected dict: {self.underscore_dict}"

        # Access dash config
        self.dash_test = self.config_with_dash.test
        self.dash_value = self.config_with_dash.value
        self.dash_dict = dict(self.config_with_dash)

        # Validate dash config values
        assert self.dash_test == "dash", f"Expected 'dash', got {self.dash_test}"
        assert self.dash_value == 99, f"Expected 99, got {self.dash_value}"
        assert self.dash_dict == {
            "test": "dash",
            "value": 99,
        }, f"Unexpected dict: {self.dash_dict}"

        # Access mixed config
        self.mixed_test = self.config_mixed.test
        self.mixed_value = self.config_mixed.value
        self.mixed_dict = dict(self.config_mixed)

        # Validate mixed config values
        assert self.mixed_test == "mixed", f"Expected 'mixed', got {self.mixed_test}"
        assert self.mixed_value == 123, f"Expected 123, got {self.mixed_value}"
        assert self.mixed_dict == {
            "test": "mixed",
            "value": 123,
        }, f"Unexpected dict: {self.mixed_dict}"

        print(f"✓ Underscore config validated: {self.underscore_dict}")
        print(f"✓ Dash config validated: {self.dash_dict}")
        print(f"✓ Mixed config validated: {self.mixed_dict}")

        self.next(self.end)

    @step
    def end(self):
        """End step."""
        print("ConfigNamingFlow completed successfully")


if __name__ == "__main__":
    ConfigNamingFlow()
