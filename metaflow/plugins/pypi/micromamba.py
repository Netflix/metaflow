import functools
import json
import os
import re
import shutil
import subprocess
import tempfile
import time

from metaflow.debug import debug
from metaflow.exception import MetaflowException
from metaflow.util import which

from .utils import MICROMAMBA_MIRROR_URL, MICROMAMBA_URL, conda_platform
from threading import Lock


class MicromambaException(MetaflowException):
    headline = "Micromamba ran into an error while setting up environment"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(MicromambaException, self).__init__(msg)


GLIBC_VERSION = os.environ.get("CONDA_OVERRIDE_GLIBC", "2.38")

_double_equal_match = re.compile("==(?=[<=>!~])")


class Micromamba(object):
    def __init__(self, logger=None, force_rebuild=False):
        # micromamba is a tiny version of the mamba package manager and comes with
        # metaflow specific performance enhancements.

        # METAFLOW_HOME might not be writable but METAFLOW_TOKEN_HOME might be.
        if os.environ.get("METAFLOW_TOKEN_HOME"):
            _home = os.environ.get("METAFLOW_TOKEN_HOME")
        else:
            _home = os.environ.get("METAFLOW_HOME", "~/.metaflowconfig")
        self._path_to_hidden_micromamba = os.path.join(
            os.path.expanduser(_home),
            "micromamba",
        )

        if logger:
            self.logger = logger
        else:
            self.logger = lambda *args, **kwargs: None  # No-op logger if not provided

        self._bin = (
            which(os.environ.get("METAFLOW_PATH_TO_MICROMAMBA") or "micromamba")
            or which("./micromamba")  # to support remote execution
            or which("./bin/micromamba")
            or which(os.path.join(self._path_to_hidden_micromamba, "bin/micromamba"))
        )

        # We keep a mutex as environments are resolved in parallel,
        # which causes a race condition in case micromamba needs to be installed first.
        self.install_mutex = Lock()

        self.force_rebuild = force_rebuild

    @property
    def bin(self) -> str:
        "Defer installing Micromamba until when the binary path is actually requested"
        if self._bin is not None:
            return self._bin
        with self.install_mutex:
            # another check as micromamba might have been installed when the mutex is released.
            if self._bin is not None:
                return self._bin

            # Install Micromamba on the fly.
            # TODO: Make this optional at some point.
            debug.conda_exec("No Micromamba binary found. Installing micromamba")
            _install_micromamba(self._path_to_hidden_micromamba)
            self._bin = which(
                os.path.join(self._path_to_hidden_micromamba, "bin/micromamba")
            )

            if self._bin is None:
                msg = "No installation for *Micromamba* found.\n"
                msg += "Visit https://mamba.readthedocs.io/en/latest/micromamba-installation.html for installation instructions."
                raise MetaflowException(msg)

        return self._bin

    def solve(self, id_, packages, python, platform):
        # Performance enhancements
        # 1. Using zstd compressed repodata index files drops the index download time
        #    by a factor of 10x - conda-forge/noarch/repodata.json has a
        #    mean download time of 3.705s ± 2.283s vs 385.1ms ± 57.9ms for
        #    conda-forge/noarch/repodata.json.zst. Thankfully, now micromamba pulls
        #    zstd compressed files by default - https://github.com/conda-forge/conda-forge.github.io/issues/1835
        # 2. Tweak repodata ttl to pull either only once a day or if a solve fails -
        #    this ensures that repodata is not pulled everytime it becomes dirty by
        #    default. This can result in an environment that has stale transitive
        #    dependencies but still correct. (--repodata-ttl 86400 --retry-clean-cache)
        # 3. Introduce pip as python dependency to resolve pip packages within conda
        #    environment
        # 4. Multiple solves can progress at the same time while relying on the same
        #    index
        debug.conda_exec("Solving packages for conda environment %s" % id_)
        with tempfile.TemporaryDirectory() as tmp_dir:
            env = {
                "MAMBA_ADD_PIP_AS_PYTHON_DEPENDENCY": "true",
                "CONDA_SUBDIR": platform,
                # "CONDA_UNSATISFIABLE_HINTS_CHECK_DEPTH": "0" # https://github.com/conda/conda/issues/9862
                # Add a default glibc version for linux-64 environments (ignored for other platforms)
                # TODO: Make the version configurable
                "CONDA_OVERRIDE_GLIBC": GLIBC_VERSION,
            }
            cmd = [
                "create",
                "--yes",
                "--quiet",
                "--dry-run",
                "--no-extra-safety-checks",
                "--repodata-ttl=86400",
                "--safety-checks=disabled",
                "--retry-clean-cache",
                "--prefix=%s/prefix" % tmp_dir,
            ]
            # Introduce conda-forge as a default channel
            for channel in self.info()["channels"] or ["conda-forge"]:
                cmd.append("--channel=%s" % channel)

            for package, version in packages.items():
                version_string = "%s==%s" % (package, version)
                cmd.append(_double_equal_match.sub("", version_string))
            if python:
                cmd.append("python==%s" % python)
            # TODO: Ensure a human readable message is returned when the environment
            #       can't be resolved for any and all reasons.
            solved_packages = [
                {k: v for k, v in item.items() if k in ["url"]}
                for item in self._call(cmd, env)["actions"]["LINK"]
            ]
            return solved_packages

    def download(self, id_, packages, python, platform):
        # Unfortunately all the packages need to be catalogued in package cache
        # because of which this function can't be parallelized

        # Micromamba is painfully slow in determining if many packages are infact
        # already cached. As a perf heuristic, we check if the environment already
        # exists to short circuit package downloads.

        prefix = "{env_dirs}/{keyword}/{platform}/{id}".format(
            env_dirs=self.info()["envs_dirs"][0],
            platform=platform,
            keyword="metaflow",  # indicates metaflow generated environment
            id=id_,
        )
        # If we are forcing a rebuild of the environment, we make sure to remove existing files beforehand.
        # This is to ensure that no irrelevant packages get bundled relative to the resolved environment.
        # NOTE: download always happens before create, so we want to do the cleanup here instead.
        if self.force_rebuild:
            shutil.rmtree(self.path_to_environment(id_, platform), ignore_errors=True)

        # cheap check
        if os.path.exists(f"{prefix}/fake.done"):
            return

        # somewhat expensive check
        if self.path_to_environment(id_, platform):
            return

        debug.conda_exec("Downloading packages for conda environment %s" % id_)
        with tempfile.TemporaryDirectory() as tmp_dir:
            env = {
                "CONDA_SUBDIR": platform,
                "CONDA_OVERRIDE_GLIBC": GLIBC_VERSION,
            }
            cmd = [
                "create",
                "--yes",
                "--no-deps",
                "--download-only",
                "--safety-checks=disabled",
                "--no-extra-safety-checks",
                "--repodata-ttl=86400",
                "--prefix=%s/prefix" % tmp_dir,
                "--quiet",
            ]
            for package in packages:
                cmd.append("{url}".format(**package))

            self._call(cmd, env)
            # Perf optimization to skip cross-platform downloads.
            if platform != self.platform():
                os.makedirs(prefix, exist_ok=True) or open(
                    f"{prefix}/fake.done", "w"
                ).close()
            return

    def create(self, id_, packages, python, platform):
        # create environment only if the platform matches system platform
        if platform != self.platform() or self.path_to_environment(id_, platform):
            return

        debug.conda_exec("Creating local Conda environment %s" % id_)
        prefix = "{env_dirs}/{keyword}/{platform}/{id}".format(
            env_dirs=self.info()["envs_dirs"][0],
            platform=platform,
            keyword="metaflow",  # indicates metaflow generated environment
            id=id_,
        )

        env = {
            # use hardlinks when possible, otherwise copy files
            # disabled for now since it adds to environment creation latencies
            "CONDA_ALLOW_SOFTLINKS": "0",
            "CONDA_OVERRIDE_GLIBC": GLIBC_VERSION,
        }
        cmd = [
            "create",
            "--yes",
            "--no-extra-safety-checks",
            # "--offline", # not needed since micromamba will first look at the cache
            "--prefix",  # trick to ensure environments can be created in parallel
            prefix,
            "--quiet",
            "--no-deps",  # important!
        ]
        for package in packages:
            cmd.append("{url}".format(**package))
        self._call(cmd, env)

    @functools.lru_cache(maxsize=None)
    def info(self):
        return self._call(["config", "list", "-a"])

    def path_to_environment(self, id_, platform=None):
        if platform is None:
            platform = self.platform()
        suffix = "{keyword}/{platform}/{id}".format(
            platform=platform,
            keyword="metaflow",  # indicates metaflow generated environment
            id=id_,
        )
        for env in self._call(["env", "list"])["envs"]:
            # TODO: Check bin/python is available as a heuristic for well formed env
            if env.endswith(suffix):
                return env

    def metadata(self, id_, packages, python, platform):
        # this method unfortunately relies on the implementation detail for
        # conda environments and has the potential to break all of a sudden.
        packages_to_filenames = {
            package["url"]: package["url"].split("/")[-1] for package in packages
        }
        directories = self.info()["pkgs_dirs"]
        # search all package caches for packages

        file_to_path = {}
        for d in directories:
            if os.path.isdir(d):
                try:
                    with os.scandir(d) as entries:
                        for entry in entries:
                            if entry.is_file():
                                # Prefer the first occurrence if the file exists in multiple directories
                                file_to_path.setdefault(entry.name, entry.path)
                except OSError:
                    continue
        ret = {
            # set package tarball local paths to None if package tarballs are missing
            url: file_to_path.get(file)
            for url, file in packages_to_filenames.items()
        }
        return ret

    def interpreter(self, id_):
        return os.path.join(self.path_to_environment(id_), "bin/python")

    def platform(self):
        return self.info()["platform"]

    def _call(self, args, env=None):
        if env is None:
            env = {}
        try:
            result = (
                subprocess.check_output(
                    [self.bin] + args,
                    stderr=subprocess.PIPE,
                    env={
                        **os.environ,
                        # prioritize metaflow-specific env vars
                        **{k: v for k, v in env.items() if v is not None},
                        **{
                            "MAMBA_NO_BANNER": "1",
                            "MAMBA_JSON": "true",
                            # play with fire! needed for resolving cross-platform
                            # environments
                            "CONDA_SAFETY_CHECKS": "disabled",
                            # "CONDA_UNSATISFIABLE_HINTS_CHECK_DEPTH": "0",
                            # Support packages on S3
                            # "CONDA_ALLOW_NON_CHANNEL_URLS": "1",
                            "MAMBA_USE_LOCKFILES": "false",
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
            msg = "command '{cmd}' returned error ({code})\n{stderr}"
            try:
                output = json.loads(e.output)
                err = []
                v_pkgs = ["__cuda", "__glibc"]
                for error in output.get("solver_problems", []):
                    # raise a specific error message for virtual package related errors
                    match = next((p for p in v_pkgs if p in error), None)
                    if match is not None:
                        vpkg_name = match[2:]
                        # try to strip version from error msg which are of the format:
                        # nothing provides <__vpkg> >=2.17,<3.0.a0 needed by <pkg_name>
                        try:
                            vpkg_version = error[
                                len("nothing provides %s " % match) : error.index(
                                    " needed by"
                                )
                            ]
                        except ValueError:
                            vpkg_version = None
                        raise MicromambaException(
                            "{msg}\n\n"
                            "*Please set the environment variable CONDA_OVERRIDE_{var} to a specific version{version} of {name}.*\n\n"
                            "Here is an example of supplying environment variables through the command line\n"
                            "CONDA_OVERRIDE_{var}=<{name}-version> python flow.py <args>".format(
                                msg=msg.format(
                                    cmd=" ".join(e.cmd),
                                    code=e.returncode,
                                    output=e.output.decode(),
                                    stderr=error,
                                ),
                                var=vpkg_name.upper(),
                                version=(
                                    "" if not vpkg_version else f" ({vpkg_version})"
                                ),
                                name=vpkg_name,
                            )
                        )
                    err.append(error)
                raise MicromambaException(
                    msg.format(
                        cmd=" ".join(e.cmd),
                        code=e.returncode,
                        output=e.output.decode(),
                        stderr="\n".join(err),
                    )
                )
            except (TypeError, ValueError):
                pass
            raise MicromambaException(
                msg.format(
                    cmd=" ".join(e.cmd),
                    code=e.returncode,
                    output=e.output.decode(),
                    stderr=e.stderr.decode(),
                )
            )


def _install_micromamba(installation_location):
    # Unfortunately no 32bit binaries are available for micromamba, which ideally
    # shouldn't be much of a problem in today's world.
    platform = conda_platform()
    url = MICROMAMBA_URL.format(platform=platform, version="1.5.7")
    mirror_url = MICROMAMBA_MIRROR_URL.format(platform=platform, version="1.5.7")
    os.makedirs(installation_location, exist_ok=True)

    def _download_and_extract(url):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # https://mamba.readthedocs.io/en/latest/micromamba-installation.html#manual-installation
                # requires bzip2
                result = subprocess.Popen(
                    f"curl -Ls {url} | tar -xvj -C {installation_location} bin/micromamba",
                    shell=True,
                    stderr=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                )
                _, err = result.communicate()
                if result.returncode != 0:
                    raise MicromambaException(
                        f"Micromamba installation '{result.args}' failed:\n{err.decode()}"
                    )
            except subprocess.CalledProcessError as e:
                if attempt == max_retries - 1:
                    raise MicromambaException(
                        "Micromamba installation failed:\n{}".format(e.stderr.decode())
                    )
                time.sleep(2**attempt)

    try:
        # prioritize downloading from mirror
        _download_and_extract(mirror_url)
    except Exception:
        # download from official source as a fallback
        _download_and_extract(url)
