import json
import os
import sys
import tarfile

from io import BytesIO

from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.exception import MetaflowException
from metaflow.mflog import BASH_SAVE_LOGS

from .conda import Conda
from . import get_conda_manifest_path, CONDA_MAGIC_FILE


class CondaEnvironment(MetaflowEnvironment):
    TYPE = "conda"
    _filecache = None

    def __init__(self, flow):
        self.flow = flow
        self.local_root = None
        # A conda environment sits on top of whatever default environment
        # the user has, so we get that environment to be able to forward
        # any calls we don't handle specifically to that one.
        from ...plugins import ENVIRONMENTS
        from metaflow.metaflow_config import DEFAULT_ENVIRONMENT

        if DEFAULT_ENVIRONMENT == self.TYPE:
            # If the default environment is Conda itself then fallback on
            # the default 'default environment'
            self.base_env = MetaflowEnvironment(self.flow)
        else:
            self.base_env = [
                e
                for e in ENVIRONMENTS + [MetaflowEnvironment]
                if e.TYPE == DEFAULT_ENVIRONMENT
            ][0](self.flow)

    def init_environment(self, echo):
        # Print a message for now
        echo("Bootstrapping conda environment..." + "(this could take a few minutes)")
        self.base_env.init_environment(echo)

    def validate_environment(self, echo, datastore_type):
        return self.base_env.validate_environment(echo, datastore_type)

    def decospecs(self):
        # Apply conda decorator and base environment's decorators to all steps
        return ("conda",) + self.base_env.decospecs()

    def _get_conda_decorator(self, step_name):
        step = next(step for step in self.flow if step.name == step_name)
        decorator = next(deco for deco in step.decorators if deco.name == "conda")
        # Guaranteed to have a conda decorator because of self.decospecs()
        return decorator

    def _get_env_id(self, step_name):
        conda_decorator = self._get_conda_decorator(step_name)
        if conda_decorator.is_enabled():
            return conda_decorator._env_id()
        return None

    def _get_executable(self, step_name):
        env_id = self._get_env_id(step_name)
        if env_id is not None:
            return os.path.join(env_id, "bin/python -s")
        return None

    def set_local_root(self, ds_root):
        self.local_root = ds_root

    def bootstrap_commands(self, step_name, datastore_type):
        # Bootstrap conda and execution environment for step
        env_id = self._get_env_id(step_name)
        if env_id is not None:
            return [
                "echo 'Bootstrapping environment...'",
                'python -m metaflow.plugins.conda.batch_bootstrap "%s" %s "%s"'
                % (self.flow.name, env_id, datastore_type),
                "echo 'Environment bootstrapped.'",
            ]
        return []

    def add_to_package(self):
        files = self.base_env.add_to_package()
        # Add conda manifest file to job package at the top level.
        path = get_conda_manifest_path(self.local_root, self.flow.name)
        if os.path.exists(path):
            files.append((path, os.path.basename(path)))
        return files

    def pylint_config(self):
        config = self.base_env.pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    def executable(self, step_name):
        # Get relevant python interpreter for step
        executable = self._get_executable(step_name)
        if executable is not None:
            return executable
        return self.base_env.executable(step_name)

    @classmethod
    def get_client_info(cls, flow_name, metadata):
        if cls._filecache is None:
            from metaflow.client.filecache import FileCache

            cls._filecache = FileCache()
        info = metadata.get("code-package")
        env_id = metadata.get("conda_env_id")
        if info is None or env_id is None:
            return {"type": "conda"}
        info = json.loads(info)
        _, blobdata = cls._filecache.get_data(
            info["ds_type"], flow_name, info["location"], info["sha"]
        )
        with tarfile.open(fileobj=BytesIO(blobdata), mode="r:gz") as tar:
            conda_file = tar.extractfile(CONDA_MAGIC_FILE)
        if conda_file is None:
            return {"type": "conda"}
        info = json.loads(conda_file.read().decode("utf-8"))
        new_info = {
            "type": "conda",
            "explicit": info[env_id]["explicit"],
            "deps": info[env_id]["deps"],
        }
        return new_info

    def get_package_commands(self, code_package_url, datastore_type):
        return self.base_env.get_package_commands(code_package_url, datastore_type)

    def get_environment_info(self):
        return self.base_env.get_environment_info()
