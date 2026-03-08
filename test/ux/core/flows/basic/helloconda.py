import os
import sys

from metaflow import FlowSpec, step, conda, conda_base, project
from metaflow.client.core import Run


@project(name="hello_conda")
@conda_base(python="3.10.*")
class HelloCondaFlow(FlowSpec):
    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """
        print("HelloCondaFlow is starting.")
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.next(self.v1, self.v2, self.combo)

    @conda(libraries={"regex": "2024.11.6"})
    @step
    def v1(self):
        import regex

        self.lib_version = regex.__version__  # Should be '2.5.148'
        print("Python: %s; library: %s" % (sys.executable, regex.__file__))
        self.next(self.join)

    @conda(libraries={"regex": "2024.9.11"})
    @step
    def v2(self):
        import regex

        self.lib_version = regex.__version__  # Should be '2.5.147'
        print("Python: %s; library: %s" % (sys.executable, regex.__file__))
        self.next(self.join)

    @conda(libraries={"regex": "2024.11.6", "itsdangerous": "2.2.0"})
    @step
    def combo(self):
        import regex
        import itsdangerous
        import importlib.metadata

        self.lib_version = regex.__version__
        self.itsdangerous_version = importlib.metadata.version("itsdangerous")
        print("Python: %s; library: %s" % (sys.executable, regex.__file__))
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        pass


if __name__ == "__main__":
    HelloCondaFlow()
