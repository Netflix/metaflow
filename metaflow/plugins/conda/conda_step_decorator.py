import importlib
import json
import os
import sys
from hashlib import sha1
from multiprocessing.dummy import Pool
import platform
import requests
import shutil
import tempfile

try:
    from urlparse import urlparse
except:
    from urllib.parse import urlparse

from metaflow.decorators import StepDecorator
from metaflow.extension_support import EXT_PKG
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import (
    get_pinned_conda_libs,
)
from metaflow.util import get_metaflow_root
from metaflow.datastore import DATASTORES, LocalStorage

from ..env_escape import generate_trampolines
from . import read_conda_manifest, write_to_conda_manifest, get_conda_package_root
from .conda import Conda

try:
    unicode
except NameError:
    unicode = str
    basestring = str


class CondaStepDecorator(StepDecorator):
    """
    Specifies the Conda environment for the step.

    Information in this decorator will augment any
    attributes set in the `@conda_base` flow-level decorator. Hence,
    you can use `@conda_base` to set common libraries required by all
    steps and use `@conda` to specify step-specific additions.

    Parameters
    ----------
    libraries : Dict
        Libraries to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`).
    python : string
        Version of Python to use, e.g. '3.7.4'
        (default: None, i.e. the current Python version).
    disabled : bool
        If set to True, disables Conda (default: False).
    """

    name = "conda"
    defaults = {"libraries": {}, "python": None, "disabled": None}

    conda = None
    environments = None

    def _get_base_attributes(self):
        if "conda_base" in self.flow._flow_decorators:
            return self.flow._flow_decorators["conda_base"].attributes
        return self.defaults

    def _python_version(self):
        return next(
            x
            for x in [
                self.attributes["python"],
                self.base_attributes["python"],
                platform.python_version(),
            ]
            if x is not None
        )

    def is_enabled(self, ubf_context=None):
        return not next(
            x
            for x in [
                self.attributes["disabled"],
                self.base_attributes["disabled"],
                False,
            ]
            if x is not None
        )

    def _lib_deps(self):
        deps = get_pinned_conda_libs(self._python_version(), self.flow_datastore.TYPE)

        base_deps = self.base_attributes["libraries"]
        deps.update(base_deps)
        step_deps = self.attributes["libraries"]

        deps.update(step_deps)
        return deps

    def _step_deps(self):
        deps = [b"python==%s" % self._python_version().encode()]
        deps.extend(
            b"%s==%s" % (name.encode("ascii"), ver.encode("ascii"))
            for name, ver in self._lib_deps().items()
        )
        return deps

    def _env_id(self):
        deps = self._step_deps()
        return "metaflow_%s_%s_%s" % (
            self.flow.name,
            self.architecture,
            sha1(b" ".join(sorted(deps))).hexdigest(),
        )

    def _resolve_step_environment(self, ds_root, force=False):
        env_id = self._env_id()
        cached_deps = read_conda_manifest(ds_root, self.flow.name)
        if CondaStepDecorator.conda is None:
            CondaStepDecorator.conda = Conda()
            CondaStepDecorator.environments = CondaStepDecorator.conda.environments(
                self.flow.name
            )
        if (
            force
            or env_id not in cached_deps
            or "cache_urls" not in cached_deps[env_id]
        ):
            if force or env_id not in cached_deps:
                deps = self._step_deps()
                (exact_deps, urls, order) = self.conda.create(
                    self.step,
                    env_id,
                    deps,
                    architecture=self.architecture,
                    disable_safety_checks=self.disable_safety_checks,
                )
                payload = {
                    "explicit": exact_deps,
                    "deps": [d.decode("ascii") for d in deps],
                    "urls": urls,
                    "order": order,
                }
            else:
                payload = cached_deps[env_id]

            if (
                self.flow_datastore.TYPE in ("s3", "azure", "gs")
                and "cache_urls" not in payload
            ):
                payload["cache_urls"] = self._cache_env()
            write_to_conda_manifest(ds_root, self.flow.name, env_id, payload)
            CondaStepDecorator.environments = CondaStepDecorator.conda.environments(
                self.flow.name
            )
        return env_id

    def _cache_env(self):
        def _download(entry):
            url, local_path = entry
            with requests.get(url, stream=True) as r:
                with open(local_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)

        env_id = self._env_id()
        files = []
        to_download = []
        for package_info in self.conda.package_info(env_id):
            url = urlparse(package_info["url"])
            path = os.path.join(
                url.netloc,
                url.path.lstrip("/"),
                package_info["md5"],
                package_info["fn"],
            )
            tarball_path = package_info["package_tarball_full_path"]
            if tarball_path.endswith(".conda"):
                # Conda doesn't set the metadata correctly for certain fields
                # when the underlying OS is spoofed.
                tarball_path = tarball_path[:-6]
            if not tarball_path.endswith(".tar.bz2"):
                tarball_path = "%s.tar.bz2" % tarball_path
            if not os.path.isfile(tarball_path):
                # The tarball maybe missing when user invokes `conda clean`!
                to_download.append((package_info["url"], tarball_path))
            files.append((path, tarball_path))
        if to_download:
            Pool(8).map(_download, to_download)

        list_of_path_and_filehandle = [
            (path, open(tarball_path, "rb")) for path, tarball_path in files
        ]

        # We need our own storage backend so that we can customize datastore_root on it
        # in a clearly safe way, without the existing backend owned by FlowDatastore
        storage_impl = DATASTORES[self.flow_datastore.TYPE]
        storage = storage_impl(get_conda_package_root(self.flow_datastore.TYPE))
        storage.save_bytes(
            list_of_path_and_filehandle, len_hint=len(list_of_path_and_filehandle)
        )

        return [files[0] for files in files]

    def _prepare_step_environment(self, step_name, ds_root):
        env_id = self._resolve_step_environment(ds_root)
        if env_id not in CondaStepDecorator.environments:
            cached_deps = read_conda_manifest(ds_root, self.flow.name)
            self.conda.create(
                self.step,
                env_id,
                cached_deps[env_id]["urls"],
                architecture=self.architecture,
                explicit=True,
                disable_safety_checks=self.disable_safety_checks,
            )
            CondaStepDecorator.environments = CondaStepDecorator.conda.environments(
                self.flow.name
            )
        return env_id

    def _disable_safety_checks(self, decos):
        # Disable conda safety checks when creating linux-64 environments on
        # a macOS. This is needed because of gotchas around inconsistently
        # case-(in)sensitive filesystems for macOS and linux.
        for deco in decos:
            if deco.name in ("batch", "kubernetes") and platform.system() == "Darwin":
                return True
        return False

    def _architecture(self, decos):
        for deco in decos:
            if deco.name in ("batch", "kubernetes"):
                # force conda resolution for linux-64 architectures
                return "linux-64"
        bit = "32"
        if platform.machine().endswith("64"):
            bit = "64"
        if platform.system() == "Linux":
            return "linux-%s" % bit
        elif platform.system() == "Darwin":
            # Support M1 Mac
            if platform.machine() == "arm64":
                return "osx-arm64"
            else:
                return "osx-%s" % bit
        else:
            raise InvalidEnvironmentException(
                "The *@conda* decorator is not supported "
                "outside of Linux and Darwin platforms"
            )

    def runtime_init(self, flow, graph, package, run_id):
        # Create a symlink to installed version of metaflow to execute user code against
        path_to_metaflow = os.path.join(get_metaflow_root(), "metaflow")
        path_to_info = os.path.join(get_metaflow_root(), "INFO")
        self.metaflow_home = tempfile.mkdtemp(dir="/tmp")
        self.addl_paths = None
        os.symlink(path_to_metaflow, os.path.join(self.metaflow_home, "metaflow"))

        # Symlink the INFO file as well to properly propagate down the Metaflow version
        # if launching on AWS Batch for example
        if os.path.isfile(path_to_info):
            os.symlink(path_to_info, os.path.join(self.metaflow_home, "INFO"))
        else:
            # If there is no "INFO" file, we will actually create one in this new
            # place because we won't be able to properly resolve the EXT_PKG extensions
            # the same way as outside conda (looking at distributions, etc.). In a
            # Conda environment, as shown below (where we set self.addl_paths), all
            # EXT_PKG extensions are PYTHONPATH extensions. Instead of re-resolving,
            # we use the resolved information that is written out to the INFO file.
            with open(
                os.path.join(self.metaflow_home, "INFO"), mode="wt", encoding="utf-8"
            ) as f:
                f.write(json.dumps(self._cur_environment.get_environment_info()))

        # Do the same for EXT_PKG
        try:
            m = importlib.import_module(EXT_PKG)
        except ImportError:
            # No additional check needed because if we are here, we already checked
            # for other issues when loading at the toplevel
            pass
        else:
            custom_paths = list(set(m.__path__))  # For some reason, at times, unique
            # paths appear multiple times. We simplify
            # to avoid un-necessary links

            if len(custom_paths) == 1:
                # Regular package; we take a quick shortcut here
                os.symlink(
                    custom_paths[0],
                    os.path.join(self.metaflow_home, EXT_PKG),
                )
            else:
                # This is a namespace package, we therefore create a bunch of directories
                # so that we can symlink in those separately, and we will add those paths
                # to the PYTHONPATH for the interpreter. Note that we don't symlink
                # to the parent of the package because that could end up including
                # more stuff we don't want
                self.addl_paths = []
                for p in custom_paths:
                    temp_dir = tempfile.mkdtemp(dir=self.metaflow_home)
                    os.symlink(p, os.path.join(temp_dir, EXT_PKG))
                    self.addl_paths.append(temp_dir)

        # Also install any environment escape overrides directly here to enable
        # the escape to work even in non metaflow-created subprocesses
        generate_trampolines(self.metaflow_home)

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@conda* decorator requires " "--environment=conda"
            )

        def _logger(line, **kwargs):
            logger(line)

        self.local_root = LocalStorage.get_datastore_root_from_config(_logger)
        environment.set_local_root(self.local_root)
        self.architecture = self._architecture(decos)
        self.disable_safety_checks = self._disable_safety_checks(decos)
        self.step = step
        self.flow = flow
        self.flow_datastore = flow_datastore
        self.base_attributes = self._get_base_attributes()
        os.environ["PYTHONNOUSERSITE"] = "1"

    def package_init(self, flow, step, environment):
        self._cur_environment = environment
        if self.is_enabled():
            self._prepare_step_environment(step, self.local_root)

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        if self.is_enabled(ubf_context):
            self.env_id = self._prepare_step_environment(self.step, self.local_root)

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        meta,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        if self.is_enabled(ubf_context):
            # Add the Python interpreter's parent to the path. This is to
            # ensure that any non-pythonic dependencies introduced by the conda
            # environment are visible to the user code.
            env_path = os.path.dirname(sys.executable)
            if os.environ.get("PATH") is not None:
                env_path = os.pathsep.join([env_path, os.environ["PATH"]])
            os.environ["PATH"] = env_path

            meta.register_metadata(
                run_id,
                step_name,
                task_id,
                [
                    MetaDatum(
                        field="conda_env_id",
                        value=self._env_id(),
                        type="conda_env_id",
                        tags=["attempt_id:{0}".format(retry_count)],
                    )
                ],
            )

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        no_batch = "batch" not in cli_args.commands
        no_kubernetes = "kubernetes" not in cli_args.commands
        if self.is_enabled(ubf_context) and no_batch and no_kubernetes:
            python_path = self.metaflow_home
            if self.addl_paths is not None:
                addl_paths = os.pathsep.join(self.addl_paths)
                python_path = os.pathsep.join([addl_paths, python_path])

            cli_args.env["PYTHONPATH"] = python_path
            cli_args.env["_METAFLOW_CONDA_ENV"] = self.env_id
            cli_args.entrypoint[0] = self.conda.python(self.env_id)

    def runtime_finished(self, exception):
        shutil.rmtree(self.metaflow_home)
