import os

from metaflow.exception import MetaflowException
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.packaging_sys import ContentType


class UVException(MetaflowException):
    headline = "uv error"


class UVEnvironment(MetaflowEnvironment):
    TYPE = "uv"

    def __init__(self, flow):
        super().__init__(flow)
        self.flow = flow

    def validate_environment(self, logger, datastore_type):
        self.datastore_type = datastore_type
        self.logger = logger

    def init_environment(self, echo, only_steps=None):
        self.logger("Bootstrapping uv...")

    def executable(self, step_name, default=None):
        return "uv run --no-sync python"

    def add_to_package(self):
        # NOTE: We treat uv.lock and pyproject.toml as regular project assets and ship these along user code as part of the code package
        # These are the minimal required files to reproduce the UV environment on the remote platform.
        def _find(filename):
            current_dir = os.getcwd()
            while True:
                file_path = os.path.join(current_dir, filename)
                if os.path.isfile(file_path):
                    return file_path
                parent_dir = os.path.dirname(current_dir)
                if parent_dir == current_dir:  # Reached root
                    raise UVException(
                        f"Could not find {filename} in current directory or any parent directory"
                    )
                current_dir = parent_dir

        pyproject_path = _find("pyproject.toml")
        uv_lock_path = _find("uv.lock")
        files = [
            (uv_lock_path, "uv.lock", ContentType.OTHER_CONTENT),
            (pyproject_path, "pyproject.toml", ContentType.OTHER_CONTENT),
        ]
        return files

    def pylint_config(self):
        config = super().pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    def bootstrap_commands(self, step_name, datastore_type):
        return [
            "echo 'Bootstrapping uv project...'",
            "flush_mflogs",
            # We have to prevent the tracing module from loading, as the bootstrapping process
            # uses the internal S3 client which would fail to import tracing due to the required
            # dependencies being bundled into the conda environment, which is yet to be
            # initialized at this point.
            'DISABLE_TRACING=True python -m metaflow.plugins.uv.bootstrap "%s"'
            % datastore_type,
            "echo 'uv project bootstrapped.'",
            "flush_mflogs",
            "export PATH=$PATH:$(pwd)/uv_install",
        ]
