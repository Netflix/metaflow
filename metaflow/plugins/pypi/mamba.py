import errno
import functools
import hashlib
import json
import os
import subprocess
import tempfile
import time
from distutils.version import LooseVersion

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import CONDA_DEPENDENCY_RESOLVER
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.util import which


class MambaException(MetaflowException):
    headline = "Mamba ran into an error while setting up environment"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(MambaException, self).__init__(msg)


class MambaStepException(MambaException):
    def __init__(self, exception, step):
        msg = "Step: {step}, Error: {error}".format(step=step, error=exception.message)
        super(MambaStepException, self).__init__(msg)


class Mamba(object):
    def __init__(self):
        self.solver = which("mamba")

        if self.solver is None:
            msg = "No installation for mamba found."
            msg += " Visit https://mamba.readthedocs.io/en/latest/installation.html for installation instructions."
            raise InvalidEnvironmentException(msg)
        # TODO (savin): Introduce a version check for mamba

    def memoize(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            kwargs = {k: json.dumps(v) for k, v in kwargs.items()}
            return wrapper2(*args, **kwargs)

        @functools.lru_cache(maxsize=None)
        def wrapper2(*args, **kwargs):
            kwargs = {k: json.loads(v) for k, v in kwargs.items()}
            return func(*args, **kwargs)

        return wrapper

    def info(self):
        return self._call(["info"])

    def interpreter(self, id):
        envs = self.info()["envs"]
        for env in envs:
            name = os.path.basename(env)
            if name == id:
                return os.path.join(env, "bin/python")

    # memoize calls to mamba solver
    @memoize
    def solve(self, **kwargs):
        with tempfile.TemporaryDirectory() as tmp_dir:
            env = {"CONDA_PKG_DIRS": "%s/envs" % tmp_dir}

            cmd = [
                "create",
                "--yes",
                "--no-default-packages",
                "--prefix",
                "%s/prefix" % tmp_dir,
                "--quiet",
                "--dry-run",
                # "--use-index-cache" # might miss out on latest packages. needs ttl.
            ]
            for package, version in kwargs.get("packages", {}).items():
                cmd.append("%s==%s" % (package, version))
            if kwargs.get("python"):
                cmd.append("python==%s" % kwargs["python"])

            return self._call(cmd, env)["actions"]["LINK"]

    def create(self, id, packages):
        # def ordered(obj):
        #     if isinstance(obj, dict):
        #         return sorted((k, ordered(v)) for k, v in obj.items())
        #     if isinstance(obj, list):
        #         return sorted(ordered(x) for x in obj)
        #     else:
        #         return obj

        # # verify if the environment already exists by checking the environment's
        # # conda-meta against resolved packages
        # envs = self.info()["envs"]
        # for env in envs:
        #     name = os.path.basename(env)
        #     if name == id:
        #         cmd = [
        #             "list",
        #             "--prefix",
        #             "%s/metaflow/%s" % (self.info()["envs_dirs"][0], id),
        #             "--no-pip",
        #         ]
        #         info = self._call(cmd)
        #         if ordered(info) == ordered(packages):
        #             return

        # # otherwise create a spanking new environment

        prefix = "{env_dirs}/metaflow/{id}".format(
            env_dirs=self.info()["envs_dirs"][0], id=id
        )

        try:
            with open(os.path.join(prefix, "canonical.packages2"), "r") as file:
                contents = file.read()
        except FileNotFoundError:
            contents = ""
        if contents == "1":
            return

        env = {
            # use hardlinks when possible, otherwise copy files
            # disabled for now since it adds to environment creation latencies
            # "CONDA_ALLOW_SOFTLINKS": "0",
        }
        cmd = [
            "create",
            "--yes",
            "--no-default-packages",
            "--offline",
            # trick to ensure environments can be created in parallel
            "--prefix",
            prefix,
            "--quiet",
            "--no-deps",  # important!
        ]
        for package in packages:
            # cmd.append("{base_url}/{platform}/{dist_name}.conda".format(**package))
            cmd.append(
                "{channel}/{platform}::{name}=={version}={build_string}".format(
                    **package
                )
            )
        self._call(cmd, env)
        with tempfile.NamedTemporaryFile("w", dir=prefix, delete=False) as temp_file:
            temp_file.write("1")
        os.rename(temp_file.name, os.path.join(prefix, "canonical.packages"))

    def download(self, **kwargs):
        # unfortunately `create --download-only` is painfully slow in short-circuiting
        # the scenario where all (or most) packages are already present in the package
        # cache.

        # get a list of all existing packages
        # existing_packages = {
        #     package["dist_name"]
        #     for package in self._call(["list", "-c"])
        #     if "dist_name" in package
        # }
        # print(existing_packages)
        with tempfile.TemporaryDirectory() as tmp_dir:
            # download packages into local package cache
            cmd = [
                "create",
                "--yes",
                "--no-default-packages",
                "--no-deps",
                "--download-only",
                "--prefix",
                "%s/prefix" % tmp_dir,
                "--quiet",
                # "-vvv",
            ]
            for package in kwargs.get("packages"):
                cmd.append("{base_url}/{platform}/{dist_name}.conda".format(**package))
                # cmd.append(
                #         "{channel}/{platform}::{name}=={version}={build_string}".format(
                #             **package
                #         )
                #     )
            return self._call(cmd)

    def _call(self, args, env=None):
        if env is None:
            env = {}
        try:
            result = (
                subprocess.check_output(
                    [self.solver] + args,
                    # stderr=subprocess.PIPE,
                    env={
                        **env,
                        **os.environ,
                        **{
                            "CONDA_JSON": "1",
                            "MAMBA_NO_BANNER": "1",
                            "MAMBA_JSON": "1",
                            # Play with fire!
                            "CONDA_SAFETY_CHECKS": "disabled",
                            "CONDA_EXTRA_SAFETY_CHECKS": "0",
                            "CONDA_UNSATISFIABLE_HINTS_CHECK_DEPTH": "0",
                            # Support packages on S3
                            "CONDA_ALLOW_NON_CHANNEL_URLS": "1",
                            # Support pip as a dependency resolver
                            "CONDA_ADD_PIP_AS_PYTHON_DEPENDENCY": "1",
                            "CONDA_PIP_INTEROP_ENABLED": "1",
                            # TODO: Remove this - only works for micromamba
                            "MAMBA_USE_LOCKFILES": "0",
                        },
                    },
                )
                .decode()
                .strip()
            )
            if result:
                return json.loads(result)
            return {}
        except subprocess.CalledProcessError as e:
            try:
                output = json.loads(e.output)
                err = [output["error"]]
                for error in output.get("errors", []):
                    err.append(error["error"])
                raise MambaException(err)
            except (TypeError, ValueError) as ve:
                pass
            raise MambaException(
                "command '{cmd}' returned error ({code}): {output}, stderr={stderr}".format(
                    cmd=e.cmd, code=e.returncode, output=e.output, stderr=e.stderr
                )
            )

    def remove(self, step_name, env_id):
        # Remove the conda environment
        try:
            with CondaLock(self._env_lock_file(env_id)):
                self._remove(env_id)
        except CondaException as e:
            raise CondaStepException(e, step_name)

    def python(self, env_id):
        # Get Python interpreter for the conda environment
        return os.path.join(self._env_path(env_id), "bin/python")
