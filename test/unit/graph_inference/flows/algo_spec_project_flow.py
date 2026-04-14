from metaflow import Parameter, project, pypi_base
from metaflow.algospec import AlgoSpec


@project(name="test_algo_project")
@pypi_base(packages={"requests": "2.31.0"})
class ProjectAlgoSpec(AlgoSpec):
    """AlgoSpec with @project and @pypi_base flow decorators."""

    value = Parameter("value", type=int, default=7)

    def call(self):
        import requests

        self.result = self.value**2
        self.requests_version = requests.__version__


if __name__ == "__main__":
    ProjectAlgoSpec()
