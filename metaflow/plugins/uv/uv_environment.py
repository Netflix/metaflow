import os
import shlex

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import UV_FORWARD_ENV_VARS, UV_FORWARD_NETRC
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
        if UV_FORWARD_NETRC and not os.path.isfile(os.path.expanduser("~/.netrc")):
            logger(
                "Warning: METAFLOW_UV_FORWARD_NETRC is set but ~/.netrc was not found. "
                "Private index authentication via netrc will not be available on the remote worker.",
                bad=True,
            )

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

        if UV_FORWARD_NETRC:
            netrc_path = os.path.expanduser("~/.netrc")
            if os.path.isfile(netrc_path):
                files.append((netrc_path, "netrc", ContentType.OTHER_CONTENT))

        return files

    def pylint_config(self):
        config = super().pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    def _get_credential_export_cmds(self):
        """
        Returns a list of shell ``export KEY=VALUE`` commands baked into the
        bootstrap script so that ``uv sync`` on the remote worker has the same
        index authentication context as the local machine.

        Auto-forwarded: all ``UV_INDEX_*`` environment variables (named-index
        authentication tokens and URLs as defined by uv).

        Opt-in: any variable names listed in ``METAFLOW_UV_FORWARD_ENV_VARS``
        (comma-separated).
        """
        cmds = []

        # Auto-forward all UV_INDEX_* vars (named-index auth tokens, URLs, etc.)
        seen = set()
        for key, value in os.environ.items():
            if key.startswith("UV_INDEX_"):
                cmds.append("export %s=%s" % (key, shlex.quote(value)))
                seen.add(key)

        # Forward any explicitly requested extra variables
        if UV_FORWARD_ENV_VARS:
            for var_name in (v.strip() for v in UV_FORWARD_ENV_VARS.split(",")):
                if var_name and var_name not in seen and var_name in os.environ:
                    cmds.append(
                        "export %s=%s" % (var_name, shlex.quote(os.environ[var_name]))
                    )

        return cmds

    def bootstrap_commands(self, step_name, datastore_type):
        export_cmds = self._get_credential_export_cmds()
        return export_cmds + [
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
