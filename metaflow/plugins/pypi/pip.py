import json
import os
import re
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from itertools import chain, product
from urllib.parse import unquote

from metaflow.exception import MetaflowException

from .micromamba import Micromamba
from .utils import pip_tags, wheel_tags


class PipException(MetaflowException):
    headline = "Pip ran into an error while setting up environment"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(PipException, self).__init__(msg)


class PipPackageNotFound(Exception):
    "Wrapper for pip package resolve errors."

    def __init__(self, error):
        self.error = error
        try:
            # Parse the package spec from error message:
            # ERROR: ERROR: Could not find a version that satisfies the requirement pkg==0.0.1 (from versions: none)
            # ERROR: No matching distribution found for pkg==0.0.1
            self.package_spec = re.search(
                "ERROR: No matching distribution found for (.*)", self.error
            )[1]
            self.package_name = re.match("\w*", self.package_spec)[0]
        except Exception:
            pass


METADATA_FILE = "{prefix}/.pip/metadata"
INSTALLATION_MARKER = "{prefix}/.pip/id"

# TODO:
#     1. Support local dirs, non-wheel like packages
#     2. Support protected indices


class Pip(object):
    def __init__(self, micromamba=None):
        # pip is assumed to be installed inside a conda environment managed by
        # micromamba. pip commands are executed using `micromamba run --prefix`
        self.micromamba = micromamba or Micromamba()

    def solve(self, id_, packages, python, platform):
        prefix = self.micromamba.path_to_environment(id_)
        if prefix is None:
            msg = "Unable to locate a Micromamba managed virtual environment\n"
            msg += "for id {id}".format(id=id_)
            raise PipException(msg)

        with tempfile.TemporaryDirectory() as tmp_dir:
            report = "{tmp_dir}/report.json".format(tmp_dir=tmp_dir)
            implementations, platforms, abis = zip(
                *[
                    (tag.interpreter, tag.platform, tag.abi)
                    for tag in pip_tags(python, platform)
                ]
            )
            custom_index_url, extra_index_urls = self.indices(prefix)
            cmd = [
                "install",
                "--dry-run",
                "--only-binary=:all:",  # only wheels
                "--upgrade-strategy=only-if-needed",
                "--target=%s" % tmp_dir,
                "--report=%s" % report,
                "--progress-bar=off",
                "--quiet",
                *(["--index-url", custom_index_url] if custom_index_url else []),
                *(
                    chain.from_iterable(
                        product(["--extra-index-url"], set(extra_index_urls))
                    )
                ),
                *(chain.from_iterable(product(["--abi"], set(abis)))),
                *(chain.from_iterable(product(["--platform"], set(platforms)))),
                # *(chain.from_iterable(product(["--implementations"], set(implementations)))),
            ]
            for package, version in packages.items():
                if version.startswith(("<", ">", "!", "~", "@")):
                    cmd.append(f"{package}{version}")
                elif not version:
                    cmd.append(f"{package}{version}")
                else:
                    cmd.append(f"{package}=={version}")
            try:
                self._call(prefix, cmd)
            except PipPackageNotFound as ex:
                # pretty print package errors
                raise PipException(
                    "Could not find a binary distribution for %s \n"
                    "for the platform %s\n\n"
                    "Note that ***@pypi*** does not currently support source distributions"
                    % (ex.package_spec, platform)
                )

            def _format(dl_info):
                res = {k: v for k, v in dl_info.items() if k in ["url"]}
                # If source url is not a wheel, we need to build the target.
                res["require_build"] = not res["url"].endswith(".whl")

                # Reconstruct the VCS url and pin to current commit_id
                # so using @branch as a version acts as expected.
                vcs_info = dl_info.get("vcs_info")
                if vcs_info:
                    subdirectory = dl_info.get("subdirectory")
                    res["url"] = "{vcs}+{url}@{commit_id}{subdir_str}".format(
                        **vcs_info,
                        **res,
                        subdir_str="#subdirectory=%s" % subdirectory
                        if subdirectory
                        else ""
                    )
                    # used to deduplicate the storage location in case wheel does not
                    # build with enough unique identifiers.
                    res["hash"] = vcs_info["commit_id"]
                return res

            with open(report, mode="r", encoding="utf-8") as f:
                return [
                    _format(item["download_info"]) for item in json.load(f)["install"]
                ]

    def download(self, id_, packages, python, platform):
        prefix = self.micromamba.path_to_environment(id_)
        metadata_file = METADATA_FILE.format(prefix=prefix)
        # download packages only if they haven't ever been downloaded before
        if os.path.isfile(metadata_file):
            return

        metadata = {}
        custom_index_url, extra_index_urls = self.indices(prefix)

        # build wheels if needed
        with ThreadPoolExecutor() as executor:

            def _build(key, package):
                dest = "{prefix}/.pip/built_wheels/{key}".format(prefix=prefix, key=key)
                cmd = [
                    "wheel",
                    "--no-deps",
                    "--progress-bar=off",
                    "--wheel-dir=%s" % dest,
                    "--quiet",
                    *(["--index-url", custom_index_url] if custom_index_url else []),
                    *(
                        chain.from_iterable(
                            product(["--extra-index-url"], set(extra_index_urls))
                        )
                    ),
                    package["url"],
                ]
                self._call(prefix, cmd)
                return package, dest

            results = list(
                executor.map(
                    lambda x: _build(*x),
                    enumerate(
                        package for package in packages if package["require_build"]
                    ),
                )
            )

            for package, path in results:
                (wheel,) = [
                    f
                    for f in os.listdir(path)
                    if os.path.isfile(os.path.join(path, f)) and f.endswith(".whl")
                ]
                if (
                    len(set(pip_tags(python, platform)).intersection(wheel_tags(wheel)))
                    == 0
                ):
                    raise PipException(
                        "The built wheel %s is not supported for %s with Python %s"
                        % (wheel, platform, python)
                    )
                target = "{prefix}/.pip/wheels/{hash}/{wheel}".format(
                    prefix=prefix,
                    wheel=wheel,
                    hash=package["hash"],
                )
                os.makedirs(os.path.dirname(target), exist_ok=True)
                shutil.move(os.path.join(path, wheel), target)
                metadata["{url}".format(**package)] = target

        implementations, platforms, abis = zip(
            *[
                (tag.interpreter, tag.platform, tag.abi)
                for tag in pip_tags(python, platform)
            ]
        )

        cmd = [
            "download",
            "--no-deps",
            "--no-index",
            "--progress-bar=off",
            #  if packages are present in Pip cache, this will be a local copy
            "--dest={prefix}/.pip/wheels".format(prefix=prefix),
            "--quiet",
            *(["--index-url", custom_index_url] if custom_index_url else []),
            *(
                chain.from_iterable(
                    product(["--extra-index-url"], set(extra_index_urls))
                )
            ),
            *(chain.from_iterable(product(["--abi"], set(abis)))),
            *(chain.from_iterable(product(["--platform"], set(platforms)))),
            # *(chain.from_iterable(product(["--implementations"], set(implementations)))),
        ]
        packages = [package for package in packages if not package["require_build"]]
        for package in packages:
            cmd.append("{url}".format(**package))
            metadata["{url}".format(**package)] = "{prefix}/.pip/wheels/{wheel}".format(
                prefix=prefix, wheel=unquote(package["url"].split("/")[-1])
            )
        self._call(prefix, cmd)
        # write the url to wheel mappings in a magic location
        with open(metadata_file, "w") as file:
            file.write(json.dumps(metadata))

    def create(self, id_, packages, python, platform):
        prefix = self.micromamba.path_to_environment(id_)
        installation_marker = INSTALLATION_MARKER.format(prefix=prefix)
        metadata = self.metadata(id_, packages, python, platform)
        # install packages only if they haven't been installed before
        if os.path.isfile(installation_marker):
            return
        # Pip can't install packages if the underlying virtual environment doesn't
        # share the same platform
        if self.micromamba.platform() == platform:
            cmd = [
                "install",
                "--no-compile",
                "--no-deps",
                "--no-index",
                "--progress-bar=off",
                "--quiet",
            ]
            for package in packages:
                cmd.append(metadata[package["url"]])
            self._call(prefix, cmd)
        with open(installation_marker, "w") as file:
            file.write(json.dumps({"id": id_}))

    def metadata(self, id_, packages, python, platform):
        # read the url to wheel mappings from a magic location
        prefix = self.micromamba.path_to_environment(id_)
        metadata_file = METADATA_FILE.format(prefix=prefix)
        with open(metadata_file, "r") as file:
            return json.loads(file.read())

    def indices(self, prefix):
        indices = []
        extra_indices = []
        try:
            config = self._call(prefix, args=["config", "list"], isolated=False)
            for line in config.splitlines():
                key, value = line.split("=", 1)
                _, key = key.split(".")
                if key in ("index-url", "extra-index-url"):
                    values = map(lambda x: x.strip("'\""), re.split("\s+", value, re.M))
                    (indices if key == "index-url" else extra_indices).extend(values)
        except Exception:
            pass

        # If there is more than one main index defined, use the first one and move the rest to extra indices.
        # There is no priority between indices with pip so the order does not matter.
        index = indices[0] if indices else None
        extras = indices[1:]

        extras.extend(extra_indices)

        return index, extras

    def _call(self, prefix, args, env=None, isolated=True):
        if env is None:
            env = {}
        try:
            return (
                subprocess.check_output(
                    [
                        self.micromamba.bin,
                        "run",
                        "--prefix",
                        prefix,
                        "pip3",
                        "--disable-pip-version-check",
                        "--no-color",
                    ]
                    # credentials are being determined from the JSON file referenced by
                    # the GOOGLE_APPLICATION_CREDENTIALS environment variable and are
                    # probably injected dynamically via `keyrings.google-artifactregistry-auth`
                    # Thus, we avoid passing `--no-input` in this case.
                    + (
                        ["--no-input"]
                        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") is None
                        else []
                    )
                    + (["--isolated"] if isolated else [])
                    + args,
                    stderr=subprocess.PIPE,
                    env={
                        **os.environ,
                        # prioritize metaflow-specific env vars
                        **{"PYTHONNOUSERSITE": "1"},  # no user installation!
                        **env,
                    },
                )
                .decode()
                .strip()
            )
        except subprocess.CalledProcessError as e:
            errors = e.stderr.decode()
            if "No matching distribution" in errors:
                raise PipPackageNotFound(errors)
            raise PipException(
                "command '{cmd}' returned error ({code}) {output}\n{stderr}".format(
                    cmd=" ".join(e.cmd),
                    code=e.returncode,
                    output=e.output.decode(),
                    stderr=errors,
                )
            )
