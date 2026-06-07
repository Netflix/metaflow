"""Flow for testing @environment decorator propagates vars to step execution."""

import os

from metaflow import FlowSpec, environment, project, step


@project(name="env_flow")
class EnvFlow(FlowSpec):
    @environment(vars={"TEST_ENV_FOO": "bar", "TEST_ENV_BAZ": "qux"})
    @step
    def start(self):
        self.foo = os.environ.get("TEST_ENV_FOO", "")
        self.baz = os.environ.get("TEST_ENV_BAZ", "")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    EnvFlow()
