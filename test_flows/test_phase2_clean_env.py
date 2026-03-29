"""
Phase 2 Test: Clean Environment (No Host Leakage)
Proves that the Docker SDK backend passes ONLY the vars we specify.
Host variables like VIRTUAL_ENV, PATH, SHELL should NOT exist.

Run: python test_flows/test_phase2_clean_env.py run
"""
from metaflow import FlowSpec, step, devcontainer
import os


class CleanEnvFlow(FlowSpec):

    @devcontainer
    @step
    def start(self):
        # These should NOT exist inside the container
        leaked_vars = ["VIRTUAL_ENV", "SHELL", "TERM", "DISPLAY", "EDITOR"]
        print("=== Environment Leakage Check ===")
        leaks_found = 0
        for var in leaked_vars:
            val = os.environ.get(var)
            if val:
                print(f"  LEAKED: {var}={val}")
                leaks_found += 1
            else:
                print(f"  CLEAN:  {var} not present")

        # These SHOULD exist (we injected them)
        expected_vars = ["PYTHONPATH", "USERNAME", "HOME"]
        print("\n=== Expected Vars Check ===")
        for var in expected_vars:
            val = os.environ.get(var, "MISSING")
            print(f"  {var}={val}")

        # containerEnv from devcontainer.json
        print("\n=== devcontainer.json containerEnv ===")
        print(f"  DEVELOPER={os.environ.get('DEVELOPER', 'MISSING')}")
        print(f"  PROJECT_STAGE={os.environ.get('PROJECT_STAGE', 'MISSING')}")

        self.leaks_found = leaks_found
        self.next(self.end)

    @step
    def end(self):
        if self.leaks_found == 0:
            print("SUCCESS: Zero host environment leakage detected.")
        else:
            print(f"WARNING: {self.leaks_found} host vars leaked into container.")


if __name__ == "__main__":
    CleanEnvFlow()
