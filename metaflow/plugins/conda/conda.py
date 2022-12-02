import errno
import os
import json
import subprocess
import time
from distutils.version import LooseVersion

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import CONDA_DEPENDENCY_RESOLVER
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.util import which


class CondaException(MetaflowException):
    headline = "Conda ran into an error while setting up environment."

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(CondaException, self).__init__(msg)


class CondaStepException(CondaException):
    def __init__(self, exception, step):
        msg = "Step: {step}, Error: {error}".format(step=step, error=exception.message)
        super(CondaStepException, self).__init__(msg)


class Conda(object):
    def __init__(self):
        dependency_solver = CONDA_DEPENDENCY_RESOLVER.lower()
        self._bin = which(dependency_solver)
        # Check if the dependency solver exists.
        if self._bin is None:
            raise InvalidEnvironmentException(
                "No %s installation found. Install %s first."
                % (dependency_solver, dependency_solver)
            )
        # Check for a minimum version for conda when conda or mamba is used
        # for dependency resolution.
        if dependency_solver == "conda" or dependency_solver == "mamba":
            if LooseVersion(self._info()["conda_version"]) < LooseVersion("4.6.0"):
                msg = "Conda version 4.6.0 or newer is required."
                if dependency_solver == "mamba":
                    msg += " Visit https://mamba.readthedocs.io/en/latest/installation.html for installation instructions."
                else:
                    msg += " Visit https://docs.conda.io/en/latest/miniconda.html for installation instructions."
                raise InvalidEnvironmentException(msg)
        # Check if conda-forge is available as a channel to pick up Metaflow's
        # dependencies. This check will go away once all of Metaflow's
        # dependencies are vendored in.
        if "conda-forge" not in "\t".join(self._info()["channels"]):
            raise InvalidEnvironmentException(
                "Conda channel 'conda-forge' is required. Specify it with CONDA_CHANNELS environment variable."
            )

    def create(
        self,
        step_name,
        env_id,
        deps,
        architecture=None,
        explicit=False,
        disable_safety_checks=False,
    ):
        # Create the conda environment
        try:
            with CondaLock(self._env_lock_file(env_id)):
                self._remove(env_id)
                self._create(
                    env_id, deps, explicit, architecture, disable_safety_checks
                )
                return self._deps(env_id)
        except CondaException as e:
            raise CondaStepException(e, step_name)

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

    def environments(self, flow):
        # List all conda environments associated with the flow
        envs = self._info()["envs"]
        ret = {}
        for env in envs:
            # Named environments are always $CONDA_PREFIX/envs/
            if "/envs/" in env:
                name = os.path.basename(env)
                if name.startswith("metaflow_%s" % flow):
                    ret[name] = env
        return ret

    def package_info(self, env_id):
        # Show conda environment package configuration
        # Not every parameter is exposed via conda cli hence this ignominy
        metadata = os.path.join(self._env_path(env_id), "conda-meta")
        for path, dirs, files in os.walk(metadata):
            for file in files:
                if file.endswith(".json"):
                    with open(os.path.join(path, file)) as f:
                        yield json.loads(f.read())

    def _info(self):
        return json.loads(self._call_conda(["info"]))

    def _create(
        self,
        env_id,
        deps,
        explicit=False,
        architecture=None,
        disable_safety_checks=False,
    ):
        cmd = ["create", "--yes", "--no-default-packages", "--name", env_id, "--quiet"]
        if explicit:
            cmd.append("--no-deps")
        cmd.extend(deps)
        self._call_conda(
            cmd, architecture=architecture, disable_safety_checks=disable_safety_checks
        )

    def _remove(self, env_id):
        self._call_conda(["env", "remove", "--name", env_id, "--yes", "--quiet"])

    def _install(self, env_id, deps, explicit=False):
        cmd = ["install", "--yes", "--name", env_id, "--quiet"]
        if explicit:
            cmd.append("--no-deps")
        cmd.extend(deps)
        self._call_conda(cmd)

    def _install_order(self, env_id):
        cmd = ["list", "--name", env_id, "--explicit"]
        response = self._call_conda(cmd).decode("utf-8")
        emit = False
        result = []
        for line in response.splitlines():
            if emit:
                result.append(line.split("/")[-1])
            if not emit and line == "@EXPLICIT":
                emit = True
        return result

    def _deps(self, env_id):
        exact_deps = []
        urls = []
        for package in self.package_info(env_id):
            exact_deps.append(
                "%s=%s=%s" % (package["name"], package["version"], package["build"])
            )
            urls.append(package["url"])
        order = self._install_order(env_id)
        return (exact_deps, urls, order)

    def _env_path(self, env_id):
        envs = self._info()["envs"]
        for env in envs:
            if "/envs/" in env:
                name = os.path.basename(env)
                if name == env_id:
                    return env
        return None

    def _env_lock_file(self, env_id):
        return os.path.join(self._info()["envs_dirs"][0], "mf_env-creation.lock")

    def _call_conda(self, args, architecture=None, disable_safety_checks=False):
        try:
            env = {
                "CONDA_JSON": "True",
                "CONDA_SUBDIR": (architecture if architecture else ""),
                "CONDA_USE_ONLY_TAR_BZ2": "True",
                "MAMBA_NO_BANNER": "1",
                "MAMBA_JSON": "True",
            }
            if disable_safety_checks:
                env["CONDA_SAFETY_CHECKS"] = "disabled"
            return subprocess.check_output(
                [self._bin] + args, stderr=subprocess.PIPE, env=dict(os.environ, **env)
            ).strip()
        except subprocess.CalledProcessError as e:
            try:
                output = json.loads(e.output)
                err = [output["error"]]
                for error in output.get("errors", []):
                    err.append(error["error"])
                raise CondaException(err)
            except (TypeError, ValueError) as ve:
                pass
            raise CondaException(
                "command '{cmd}' returned error ({code}): {output}, stderr={stderr}".format(
                    cmd=e.cmd, code=e.returncode, output=e.output, stderr=e.stderr
                )
            )


class CondaLock(object):
    def __init__(self, lock, timeout=3600, delay=10):
        self.lock = lock
        self.locked = False
        self.timeout = timeout
        self.delay = delay

    def _acquire(self):
        start = time.time()
        try:
            os.makedirs(os.path.dirname(self.lock))
        except OSError as x:
            if x.errno != errno.EEXIST:
                raise
        while True:
            try:
                self.fd = os.open(self.lock, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self.locked = True
                break
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                if self.timeout is None:
                    raise CondaException("Could not acquire lock {}".format(self.lock))
                if (time.time() - start) >= self.timeout:
                    raise CondaException(
                        "Timeout occurred while acquiring lock {}".format(self.lock)
                    )
                time.sleep(self.delay)

    def _release(self):
        if self.locked:
            os.close(self.fd)
            os.unlink(self.lock)
            self.locked = False

    def __enter__(self):
        if not self.locked:
            self._acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.__del__()

    def __del__(self):
        self._release()
