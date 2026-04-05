"""
Phase 0 Test: Discovery
Validates that the @devcontainer decorator can find and parse
.devcontainer/devcontainer.json from the project root.

Run: python test_flows/test_phase0_discovery.py run
"""
from metaflow import FlowSpec, step, devcontainer


class DiscoveryFlow(FlowSpec):

    @devcontainer
    @step
    def start(self):
        import os
        import json

        spec_path = os.path.join(os.getcwd(), ".devcontainer/devcontainer.json")
        exists = os.path.exists(spec_path)
        print(f"devcontainer.json found: {exists}")

        if exists:
            with open(spec_path) as f:
                config = json.load(f)
            print(f"Image: {config.get('image', 'N/A')}")
            print(f"Env vars: {list(config.get('containerEnv', {}).keys())}")

        self.spec_found = exists
        self.next(self.end)

    @step
    def end(self):
        print(f"Discovery result: spec_found={self.spec_found}")


if __name__ == "__main__":
    DiscoveryFlow()
