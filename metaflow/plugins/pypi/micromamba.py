import json
import os
import subprocess
import tempfile

from metaflow.exception import MetaflowException
from metaflow.util import which

from .utils import conda_platform


class MicromambaException(MetaflowException):
    headline = "Micromamba ran into an error while setting up environment"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(MicromambaException, self).__init__(msg)


class Micromamba(object):
    def __init__(self):
        # micromamba is a tiny version of the mamba package manager and comes with
        # metaflow specific performance enhancements.

        # METAFLOW_HOME might not be writable but METAFLOW_TOKEN_HOME might be.
        if os.environ.get("METAFLOW_TOKEN_HOME"):
            _home = os.environ.get("METAFLOW_TOKEN_HOME")
        else:
            _home = os.environ.get("METAFLOW_HOME", "~/.metaflowconfig")
        _path_to_hidden_micromamba = os.path.join(
            os.path.expanduser(_home),
            "micromamba",
        )
        self.bin = (
            which(os.environ.get("METAFLOW_PATH_TO_MICROMAMBA") or "micromamba")
            or which("./micromamba")  # to support remote execution
            or which("./bin/micromamba")
            or which(os.path.join(_path_to_hidden_micromamba, "bin/micromamba"))
        )
        if self.bin is None:
            # Install Micromamba on the fly.
            # TODO: Make this optional at some point.
            _install_micromamba(_path_to_hidden_micromamba)
            self.bin = which(os.path.join(_path_to_hidden_micromamba, "bin/micromamba"))

        if self.bin is None:
            msg = "No installation for *Micromamba* found.\n"
            msg += "Visit https://mamba.readthedocs.io/en/latest/micromamba-installation.html for installation instructions."
            raise MetaflowException(msg)

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
        with tempfile.TemporaryDirectory() as tmp_dir:
            env = {
                "MAMBA_ADD_PIP_AS_PYTHON_DEPENDENCY": "true",
                "CONDA_SUBDIR": platform,
                # "CONDA_UNSATISFIABLE_HINTS_CHECK_DEPTH": "0" # https://github.com/conda/conda/issues/9862
            }
            cmd = [
                "create",
                "--yes",
                "--quiet",
                "--dry-run",
                "--no-extra-safety-checks",
                "--repodata-ttl=86400",
                "--retry-clean-cache",
                "--prefix=%s/prefix" % tmp_dir,
            ]
            # Introduce conda-forge as a default channel
            for channel in self.info()["channels"] or ["conda-forge"]:
                cmd.append("--channel=%s" % channel)

            for package, version in packages.items():
                cmd.append("%s==%s" % (package, version))
            if python:
                cmd.append("python==%s" % python)
            # TODO: Ensure a human readable message is returned when the environment
            #       can't be resolved for any and all reasons.
            return [
                {k: v for k, v in item.items() if k in ["url"]}
                for item in self._call(cmd, env)["actions"]["LINK"]
            ]

    def download(self, id_, packages, python, platform):
        # Unfortunately all the packages need to be catalogued in package cache
        # because of which this function can't be parallelized

        # Micromamba is painfully slow in determining if many packages are infact
        # already cached. As a perf heuristic, we check if the environment already
        # exists to short circuit package downloads.
        if self.path_to_environment(id_, platform):
            return

        prefix = "{env_dirs}/{keyword}/{platform}/{id}".format(
            env_dirs=self.info()["envs_dirs"][0],
            platform=platform,
            keyword="metaflow",  # indicates metaflow generated environment
            id=id_,
        )

        # Another forced perf heuristic to skip cross-platform downloads.
        if os.path.exists(f"{prefix}/fake.done"):
            return

        with tempfile.TemporaryDirectory() as tmp_dir:
            env = {
                "CONDA_SUBDIR": platform,
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
        metadata = {
            url: os.path.join(d, file)
            for url, file in packages_to_filenames.items()
            for d in directories
            if os.path.isdir(d)
            and file in os.listdir(d)
            and os.path.isfile(os.path.join(d, file))
        }
        # set package tarball local paths to None if package tarballs are missing
        for url in packages_to_filenames:
            metadata.setdefault(url, None)
        return metadata

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
                for error in output.get("solver_problems", []):
                    err.append(error)
                raise MicromambaException(
                    msg.format(
                        cmd=" ".join(e.cmd),
                        code=e.returncode,
                        output=e.output.decode(),
                        stderr="\n".join(err),
                    )
                )
            except (TypeError, ValueError) as ve:
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
    try:
        subprocess.Popen(f"mkdir -p {installation_location}", shell=True).wait()
        # https://mamba.readthedocs.io/en/latest/micromamba-installation.html#manual-installation
        # requires bzip2
        result = subprocess.Popen(
            f"curl -Ls https://micro.mamba.pm/api/micromamba/{platform}/1.5.7 | tar -xvj -C {installation_location} bin/micromamba",
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
        raise MicromambaException(
            "Micromamba installation failed:\n{}".format(e.stderr.decode())
        )
