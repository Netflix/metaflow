from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
import shutil
import subprocess
import tempfile
from itertools import chain, product

from metaflow.exception import MetaflowException

from .micromamba import Micromamba
from .utils import pip_tags


class PipException(MetaflowException):
    headline = "Pip ran into an error while setting up environment"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(PipException, self).__init__(msg)


METADATA_FILE = "{prefix}/.pip/metadata"
BUILD_METADATA_FILE = "{prefix}/.pip/build_metadata"
INSTALLATION_MARKER = "{prefix}/.pip/id"

# TODO:
#     1. Support git repositories, local dirs, non-wheel like packages
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
                if version.startswith(("<", ">", "!", "~", "@")) or version == "":
                    cmd.append(f"{package}{version}")
                else:
                    cmd.append(f"{package}=={version}")
            self._call(prefix, cmd)

            def _format_item(item):
                dl_info = item["download_info"]
                res = {k: v for k, v in dl_info.items() if k in ["url"]}
                # If source url is not a wheel, we need to build the target. Add a build flag.
                res["require_build"] = not res["url"].endswith(".whl")

                # reconstruct the VCS url and pin to current commit_id
                # so using @branch as a version acts somewhat as expected.
                vcs_info = dl_info.get("vcs_info")
                if vcs_info:
                    res["url"] = "{vcs}+{url}@{commit_id}".format(**vcs_info, **res)
                return res

            with open(report, mode="r", encoding="utf-8") as f:
                return [_format_item(item) for item in json.load(f)["install"]]

    def build_wheels(self, id_, packages, python, platform):
        prefix = self.micromamba.path_to_environment(id_)
        build_metadata_file = BUILD_METADATA_FILE.format(prefix=prefix)
        # skip build if it has been tried already.
        if os.path.isfile(build_metadata_file):
            return
        metadata = {}
        with ThreadPoolExecutor() as executor:
            results = list(
                executor.map(
                    lambda x: self._build_wheel(*x, prefix), enumerate(packages)
                )
            )

            def _grab_wheel_from_path(path):
                wheels = [
                    f
                    for f in os.listdir(path)
                    if os.path.isfile(os.path.join(path, f)) and f.endswith(".whl")
                ]
                if len(wheels) != 1:
                    raise Exception(
                        "Incorrect number of wheels found in path %s after building: %s"
                        % (path, wheels)
                    )
                return wheels[0]

            # Create wheels path if it does not exist yet, which is possible as we build before downloading.
            os.makedirs("%s/.pip/wheels" % prefix, exist_ok=True)

            for package, local_path in results:
                built_wheel = _grab_wheel_from_path(local_path)
                target_path = "%s/.pip/wheels/%s" % (prefix, built_wheel)
                shutil.move(os.path.join(local_path, built_wheel), target_path)
                metadata[package["url"]] = target_path

        # write the url to wheel mappings in a magic location
        with open(build_metadata_file, "w") as file:
            file.write(json.dumps(metadata))

    def _build_wheel(self, key, package, prefix):
        custom_index_url, extra_index_urls = self.indices(prefix)
        dest = "%s/.pip/built_wheels/%s" % (prefix, key)
        cmd = [
            "wheel",
            "--no-deps",
            "--progress-bar=off",
            "-w",
            dest,
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

    def download(self, id_, packages, python, platform):
        prefix = self.micromamba.path_to_environment(id_)
        metadata_file = METADATA_FILE.format(prefix=prefix)
        # Branch off to building wheels if any are required:
        build_packages = [package for package in packages if package["require_build"]]
        download_packages = [
            package for package in packages if not package["require_build"]
        ]
        if build_packages:
            self.build_wheels(id_, build_packages, python, platform)

        # download packages only if they haven't ever been downloaded before
        if os.path.isfile(metadata_file):
            return
        metadata = {}
        implementations, platforms, abis = zip(
            *[
                (tag.interpreter, tag.platform, tag.abi)
                for tag in pip_tags(python, platform)
            ]
        )
        custom_index_url, extra_index_urls = self.indices(prefix)
        cmd = [
            "download",
            "--no-deps",
            "--no-index",
            "--progress-bar=off",
            #  if packages are present in Pip cache, this will be a local copy
            "--dest=%s/.pip/wheels" % prefix,
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
        for package in download_packages:
            cmd.append(package["url"])
            # record the url-to-path mapping fo wheels in metadata file.
            metadata[package["url"]] = "{prefix}/.pip/wheels/{wheel}".format(
                prefix=prefix, wheel=package["url"].split("/")[-1]
            )
        self._call(prefix, cmd)
        # write the url to wheel mappings in a magic location
        with open(metadata_file, "w") as file:
            file.write(json.dumps(metadata))

    def create(self, id_, packages, python, platform):
        prefix = self.micromamba.path_to_environment(id_)
        installation_marker = INSTALLATION_MARKER.format(prefix=prefix)
        url_mappings = self.metadata(id_, packages, python, platform)
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
                cmd.append(url_mappings[package["url"]])
            self._call(prefix, cmd)
        with open(installation_marker, "w") as file:
            file.write(json.dumps({"id": id_}))

    def metadata(self, id_, packages, python, platform):
        # read the url to wheel mappings from a magic location.
        # Combine the metadata and build_metadata files (these should be disjoint sets).
        prefix = self.micromamba.path_to_environment(id_)
        metadata_file = METADATA_FILE.format(prefix=prefix)
        build_metadata_file = BUILD_METADATA_FILE.format(prefix=prefix)
        meta = {}
        build_meta = {}
        try:
            with open(metadata_file, "r") as file:
                meta = json.load(file)
            with open(build_metadata_file, "r") as file:
                build_meta = json.load(file)
        except FileNotFoundError:
            pass

        return {**meta, **build_meta}

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
                        "--no-input",
                        "--no-color",
                    ]
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
            raise PipException(
                "command '{cmd}' returned error ({code}) {output}\n{stderr}".format(
                    cmd=" ".join(e.cmd),
                    code=e.returncode,
                    output=e.output.decode(),
                    stderr=e.stderr.decode(),
                )
            )
