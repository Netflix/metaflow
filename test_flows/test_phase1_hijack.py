"""
Phase 1 Test: CLI Hijack
Proves the runtime_step_cli hook successfully redirects execution
from the host Python into a Docker container.

The step prints platform info to prove it is running inside
a Linux container (even if the host is macOS or WSL).

Run: python test_flows/test_phase1_hijack.py run
"""
from metaflow import FlowSpec, step, devcontainer
import platform


class HijackFlow(FlowSpec):

    @devcontainer
    @step
    def start(self):
        print(f"Platform: {platform.system()} {platform.release()}")
        print(f"Python: {platform.python_version()}")
        print(f"Machine: {platform.machine()}")
        print("If 'Linux' above, the CLI hijack is working.")
        self.platform = platform.system()
        self.next(self.end)

    @step
    def end(self):
        print(f"Step ran on: {self.platform}")
        print("Flow complete.")


if __name__ == "__main__":
    HijackFlow()
