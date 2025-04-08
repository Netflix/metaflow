import os

from metaflow.exception import MetaflowException
from metaflow.metaflow_environment import MetaflowEnvironment


class UVException(MetaflowException):
    headline = "UV Error"


class UVEnvironment(MetaflowEnvironment):
    TYPE = "uv"

    def __init__(self, flow):
        self.flow = flow

    def validate_environment(self, logger, datastore_type):
        self.datastore_type = datastore_type
        self.logger = logger

    def init_environment(self, echo, only_steps=None):
        self.logger("Bootstrapping uv...")

    def executable(self, step_name, default=None):
        return "uv run python"

    def interpreter(self, step_name):
        return "uv run python"

    def add_to_package(self):
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
            (uv_lock_path, os.path.basename(uv_lock_path)),
            (pyproject_path, os.path.basename(pyproject_path)),
        ]
        return files

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
            "export PATH=$PATH:/uv_install",
        ]
