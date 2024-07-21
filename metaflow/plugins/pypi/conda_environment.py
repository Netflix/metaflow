import copy
import errno
import fcntl
import functools
import io
import json
import os
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from io import BufferedIOBase, BytesIO
from itertools import chain
from urllib.parse import unquote, urlparse

import requests

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import get_pinned_conda_libs
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.metaflow_profile import profile

from . import MAGIC_FILE, _datastore_packageroot
from .utils import conda_platform


class CondaEnvironmentException(MetaflowException):
    headline = "Ran into an error while setting up environment"

    def __init__(self, msg):
        super(CondaEnvironmentException, self).__init__(msg)


class CondaEnvironment(MetaflowEnvironment):
    TYPE = "conda"
    _filecache = None

    def __init__(self, flow):
        self.flow = flow

    def set_local_root(self, local_root):
        # TODO: Make life simple by passing echo to the constructor and getting rid of
        # this method's invocation in the decorator
        self.local_root = local_root

    def decospecs(self):
        # Apply conda decorator to manage the task execution lifecycle.
        return ("conda",) + super().decospecs()

    def validate_environment(self, logger, datastore_type):
        self.datastore_type = datastore_type
        self.logger = logger

        # Avoiding circular imports.
        from metaflow.plugins import DATASTORES

        self.datastore = [d for d in DATASTORES if d.TYPE == self.datastore_type][0]

        # Initialize necessary virtual environments for all Metaflow tasks.
        # Use Micromamba for solving conda packages and Pip for solving pypi packages.
        from .micromamba import Micromamba
        from .pip import Pip

        micromamba = Micromamba()
        self.solvers = {"conda": micromamba, "pypi": Pip(micromamba)}

    def init_environment(self, echo, only_steps=None):
        # The implementation optimizes for latency to ensure as many operations can
        # be turned into cheap no-ops as feasible. Otherwise, we focus on maintaining
        # a balance between latency and maintainability of code without re-implementing
        # the internals of Micromamba and Pip.

        # TODO: Introduce verbose logging
        #       https://github.com/Netflix/metaflow/issues/1494

        def environments(type_):
            seen = set()
            for step in self.flow:
                if only_steps and step.name not in only_steps:
                    continue
                environment = self.get_environment(step)
                if type_ in environment and environment["id_"] not in seen:
                    seen.add(environment["id_"])
                    for platform in environment[type_]["platforms"]:
                        yield environment["id_"], {
                            **{
                                k: v
                                for k, v in environment[type_].items()
                                if k != "platforms"
                            },
                            **{"platform": platform},
                        }

        def solve(id_, environment, type_):
            # Cached solve - should be quick!
            platform = environment["platform"]
            return (
                id_,
                (
                    self.read_from_environment_manifest([id_, platform, type_])
                    or self.write_to_environment_manifest(
                        [id_, platform, type_],
                        self.solvers[type_].solve(id_, **environment),
                    )
                ),
                environment["python"],
                platform,
            )

        def cache(storage, results, type_):
            def _path(url, local_path):
                # Special handling for VCS packages
                if url.startswith("git+"):
                    base, _ = os.path.split(urlparse(url).path)
                    _, file = os.path.split(local_path)
                    prefix = url.split("@")[-1]
                    return urlparse(url).netloc + os.path.join(
                        unquote(base), prefix, file
                    )
                else:
                    return urlparse(url).netloc + urlparse(url).path

            local_packages = {
                url: {
                    # Path to package in datastore.
                    "path": _path(
                        url, local_path
                    ),  # urlparse(url).netloc + urlparse(url).path,
                    # Path to package on local disk.
                    "local_path": local_path,
                }
                for result in results
                for url, local_path in self.solvers[type_].metadata(*result).items()
            }
            dirty = set()
            # Prune list of packages to cache.

            _meta = copy.deepcopy(local_packages)
            for id_, packages, _, _ in results:
                for package in packages:
                    if package.get("path"):
                        # Cache only those packages that manifest is unaware of
                        local_packages.pop(package["url"], None)
                    else:
                        package["path"] = _meta[package["url"]]["path"]
                        dirty.add(id_)

            list_of_path_and_filehandle = [
                (
                    package["path"],
                    # Lazily fetch package from the interweb if needed.
                    LazyOpen(
                        package["local_path"],
                        "rb",
                        url,
                    ),
                )
                for url, package in local_packages.items()
            ]
            storage.save_bytes(
                list_of_path_and_filehandle,
                len_hint=len(list_of_path_and_filehandle),
            )
            for id_, packages, _, platform in results:
                if id_ in dirty:
                    self.write_to_environment_manifest([id_, platform, type_], packages)

        # First resolve environments through Conda, before PyPI.
        self.logger("Bootstrapping virtual environment(s) ...")
        for solver in ["conda", "pypi"]:
            with ThreadPoolExecutor() as executor:
                results = list(
                    executor.map(lambda x: solve(*x, solver), environments(solver))
                )
            _ = list(map(lambda x: self.solvers[solver].download(*x), results))
            with ThreadPoolExecutor() as executor:
                _ = list(
                    executor.map(lambda x: self.solvers[solver].create(*x), results)
                )
            if self.datastore_type not in ["local"]:
                # Cache packages only when a remote datastore is in play.
                storage = self.datastore(_datastore_packageroot(self.datastore, echo))
                cache(storage, results, solver)
        self.logger("Virtual environment(s) bootstrapped!")

    def executable(self, step_name, default=None):
        step = next(step for step in self.flow if step.name == step_name)
        id_ = self.get_environment(step).get("id_")
        if id_:
            # bootstrap.py is responsible for ensuring the validity of this executable.
            # -s is important! Can otherwise leak packages to other environments.
            return os.path.join("linux-64", id_, "bin/python -s")
        else:
            # for @conda/@pypi(disabled=True).
            return super().executable(step_name, default)

    def interpreter(self, step_name):
        step = next(step for step in self.flow if step.name == step_name)
        id_ = self.get_environment(step)["id_"]
        # User workloads are executed through the conda environment's interpreter.
        return self.solvers["conda"].interpreter(id_)

    def is_disabled(self, step):
        for decorator in step.decorators:
            # @conda decorator is guaranteed to exist thanks to self.decospecs
            if decorator.name in ["conda", "pypi"]:
                # handle @conda/@pypi(disabled=True)
                disabled = decorator.attributes["disabled"]
                return str(disabled).lower() == "true"
        return False

    @functools.lru_cache(maxsize=None)
    def get_environment(self, step):
        environment = {}
        for decorator in step.decorators:
            # @conda decorator is guaranteed to exist thanks to self.decospecs
            if decorator.name in ["conda", "pypi"]:
                # handle @conda/@pypi(disabled=True)
                disabled = decorator.attributes["disabled"]
                if not disabled or str(disabled).lower() == "false":
                    environment[decorator.name] = {
                        k: copy.deepcopy(decorator.attributes[k])
                        for k in decorator.attributes
                        if k != "disabled"
                    }
                else:
                    return {}
        # Resolve conda environment for @pypi's Python, falling back on @conda's
        # Python
        env_python = (
            environment.get("pypi", environment["conda"]).get("python")
            or environment["conda"]["python"]
        )
        # TODO: Support dependencies for `--metadata`.
        # TODO: Introduce support for `--telemetry` as a follow up.
        # Certain packages are required for metaflow runtime to function correctly.
        # Ensure these packages are available both in Conda channels and PyPI
        # repostories.
        pinned_packages = get_pinned_conda_libs(env_python, self.datastore_type)

        # PyPI dependencies are prioritized over Conda dependencies.
        environment.get("pypi", environment["conda"])["packages"] = {
            **pinned_packages,
            **environment.get("pypi", environment["conda"])["packages"],
        }
        # Disallow specifying both @conda and @pypi together for now. Mixing Conda
        # and PyPI packages comes with a lot of operational pain that we can handle
        # as follow-up work in the future.
        if all(
            map(lambda key: environment.get(key, {}).get("packages"), ["pypi", "conda"])
        ):
            msg = "Mixing and matching PyPI packages and Conda packages within a\n"
            msg += "step is not yet supported. Use one of @pypi or @conda only."
            raise CondaEnvironmentException(msg)

        # To support cross-platform environments, these invariants are maintained
        # 1. Conda packages are resolved for target platforms
        # 2. Conda packages are resolved for local platform only for PyPI packages
        # 3. Conda environments are created only for local platform
        # 4. PyPI packages are resolved for target platform within Conda environments
        #    created for local platform
        # 5. All resolved packages (Conda or PyPI) are cached
        # 6. PyPI packages are only installed for local platform

        # Resolve `linux-64` Conda environments if @batch or @kubernetes are in play
        target_platform = conda_platform()
        for decorator in step.decorators:
            # TODO: rather than relying on decorator names, rely on attributes
            #       to make them extensible.
            if decorator.name in ["batch", "kubernetes", "nvidia"]:
                # TODO: Support arm architectures
                target_platform = "linux-64"
                break

        environment["conda"]["platforms"] = [target_platform]
        if "pypi" in environment:
            # For PyPI packages, resolve conda environment for local platform in
            # addition to target platform
            environment["conda"]["platforms"] = list(
                {target_platform, conda_platform()}
            )
            environment["pypi"]["platforms"] = [target_platform]
            # Match PyPI and Conda python versions with the resolved environment Python.
            environment["pypi"]["python"] = environment["conda"]["python"] = env_python

            # When using `Application Default Credentials` for private GCP
            # PyPI registries, the usage of environment variable `GOOGLE_APPLICATION_CREDENTIALS`
            # demands that `keyrings.google-artifactregistry-auth` has to be installed
            # and available in the underlying python environment.
            if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
                environment["conda"]["packages"][
                    "keyrings.google-artifactregistry-auth"
                ] = ">=1.1.1"

        # Z combinator for a recursive lambda
        deep_sort = (lambda f: f(f))(
            lambda f: lambda obj: (
                {k: f(f)(v) for k, v in sorted(obj.items())}
                if isinstance(obj, dict)
                else sorted([f(f)(e) for e in obj])
                if isinstance(obj, list)
                else obj
            )
        )

        return {
            **environment,
            # Create a stable unique id for the environment.
            # Add packageroot to the id so that packageroot modifications can
            # invalidate existing environments.
            "id_": sha256(
                json.dumps(
                    deep_sort(
                        {
                            **environment,
                            **{
                                "package_root": _datastore_packageroot(
                                    self.datastore, self.logger
                                )
                            },
                        }
                    )
                ).encode()
            ).hexdigest()[:15],
        }

    def pylint_config(self):
        config = super().pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    @classmethod
    def get_client_info(cls, flow_name, metadata):
        if cls._filecache is None:
            from metaflow.client.filecache import FileCache

            cls._filecache = FileCache()

        info = metadata.get("code-package")
        prefix = metadata.get("conda_env_prefix")
        if info is None or prefix is None:
            return {}
        info = json.loads(info)
        _, blobdata = cls._filecache.get_data(
            info["ds_type"], flow_name, info["location"], info["sha"]
        )
        with tarfile.open(fileobj=BytesIO(blobdata), mode="r:gz") as tar:
            manifest = tar.extractfile(MAGIC_FILE)
            info = json.loads(manifest.read().decode("utf-8"))
            return info[prefix.split("/")[2]][prefix.split("/")[1]]

    def add_to_package(self):
        # Add manifest file to job package at the top level.
        files = []
        manifest = self.get_environment_manifest_path()
        if os.path.exists(manifest):
            files.append((manifest, os.path.basename(manifest)))
        return files

    def bootstrap_commands(self, step_name, datastore_type):
        # Bootstrap conda and execution environment for step
        step = next(step for step in self.flow if step.name == step_name)
        id_ = self.get_environment(step).get("id_")
        if id_:
            return [
                "echo 'Bootstrapping virtual environment...'",
                # We have to prevent the tracing module from loading,
                # as the bootstrapping process uses the internal S3 client which would fail to import tracing
                # due to the required dependencies being bundled into the conda environment,
                # which is yet to be initialized at this point.
                'DISABLE_TRACING=True python -m metaflow.plugins.pypi.bootstrap "%s" %s "%s" linux-64'
                % (self.flow.name, id_, self.datastore_type),
                "echo 'Environment bootstrapped.'",
                "export PATH=$PATH:$(pwd)/micromamba",
            ]
        else:
            # for @conda/@pypi(disabled=True).
            return super().bootstrap_commands(step_name, datastore_type)

    # TODO: Make this an instance variable once local_root is part of the object
    #       constructor.
    def get_environment_manifest_path(self):
        return os.path.join(self.local_root, self.flow.name, MAGIC_FILE)

    def read_from_environment_manifest(self, keys):
        path = self.get_environment_manifest_path()
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path) as f:
                data = json.load(f)
                for key in keys:
                    try:
                        data = data[key]
                    except KeyError:
                        return None
                return data

    def write_to_environment_manifest(self, keys, value):
        path = self.get_environment_manifest_path()
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as x:
            if x.errno != errno.EEXIST:
                raise
        with os.fdopen(os.open(path, os.O_RDWR | os.O_CREAT), "r+") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX)
                d = {}
                if os.path.getsize(path) > 0:
                    f.seek(0)
                    d = json.load(f)
                data = d
                for key in keys[:-1]:
                    data = data.setdefault(key, {})
                data[keys[-1]] = value
                f.seek(0)
                json.dump(d, f)
                f.truncate()
                return value
            except IOError as e:
                if e.errno != errno.EAGAIN:
                    raise
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)


class LazyOpen(BufferedIOBase):
    def __init__(self, filename, mode="rb", url=None):
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.url = url
        self._file = None
        self._buffer = None
        self._position = 0

    def _ensure_file(self):
        if not self._file:
            if self.filename and os.path.exists(self.filename):
                self._file = open(self.filename, self.mode)
            elif self.url:
                if self.url.startswith("git+"):
                    raise ValueError(
                        "LazyOpen doesn't support VCS url %s yet!" % self.url
                    )
                self._buffer = self._download_to_buffer()
                self._file = io.BytesIO(self._buffer)
            else:
                raise ValueError("Both filename and url are missing")

    def _download_to_buffer(self):
        # TODO: Stream it in chunks?
        response = requests.get(self.url, stream=True)
        response.raise_for_status()
        return response.content

    def readable(self):
        return "r" in self.mode

    def seekable(self):
        return True

    def read(self, size=-1):
        self._ensure_file()
        return self._file.read(size)

    def seek(self, offset, whence=io.SEEK_SET):
        self._ensure_file()
        return self._file.seek(offset, whence)

    def tell(self):
        self._ensure_file()
        return self._file.tell()

    def close(self):
        if self._file:
            self._file.close()
