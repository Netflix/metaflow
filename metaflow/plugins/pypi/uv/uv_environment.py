import os

from metaflow.exception import MetaflowException
from metaflow.metaflow_environment import MetaflowEnvironment


class UVException(MetaflowException):
    headline = "UV Environment Error"


class UVEnvironment(MetaflowEnvironment):
    TYPE = "uv"

    def __init__(self, flow):
        self.flow = flow
        self.pyproject_path = "./pyproject.toml"
        self.uv_lock_path = "./uv.lock"

    def validate_environment(self, logger, datastore_type):
        self.datastore_type = datastore_type
        self.logger = logger

        if not os.path.isfile(self.pyproject_path):
            raise UVException("Could not find pyproject.toml")

        if not os.path.isfile(self.uv_lock_path):
            raise UVException("Could not find uv.lock")

        # TODO: Check that flow does not try to also rely on @conda or @pypi for dependencies

    def init_environment(self, echo, only_steps=None):
        self.logger("Bootstrapping UV ...")

    def executable(self, step_name, default=None):
        return "uv run python"

    def interpreter(self, step_name):
        return "uv run python"

    def pylint_config(self):
        config = super().pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    def add_to_package(self):
        files = [
            (self.uv_lock_path, self.uv_lock_path),
            (self.pyproject_path, self.pyproject_path),
        ]
        return files

    def bootstrap_commands(self, step_name, datastore_type):
        # step = next(step for step in self.flow if step.name == step_name)
        return [
            "echo 'Bootstrapping UV project...'",
            "flush_mflogs",
            # We have to prevent the tracing module from loading,
            # as the bootstrapping process uses the internal S3 client which would fail to import tracing
            # due to the required dependencies being bundled into the conda environment,
            # which is yet to be initialized at this point.
            'DISABLE_TRACING=True python -m metaflow.plugins.pypi.uv.bootstrap "%s" %s "%s"'
            % (self.flow.name, "dummy_id", self.datastore_type),
            "echo 'Environment bootstrapped.'",
            "flush_mflogs",
            "export PATH=$PATH:/uv_install",
            "export MF_ARCH=$(case $(uname)/$(uname -m) in Darwin/arm64)echo osx-arm64;;Darwin/*)echo osx-64;;Linux/aarch64)echo linux-aarch64;;*)echo linux-64;;esac)",
        ]
