import json
import os
import subprocess
import tempfile
from itertools import chain, product

from metaflow.exception import MetaflowException
from metaflow.util import which

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
            extra_index_urls = self.extra_index_urls(prefix)
            cmd = [
                "install",
                "--dry-run",
                "--only-binary=:all:",  # only wheels
                "--upgrade-strategy=only-if-needed",
                "--target=%s" % tmp_dir,
                "--report=%s" % report,
                "--progress-bar=off",
                "--quiet",
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
                if version.startswith(("<", ">", "!", "~")):
                    cmd.append(f"{package}{version}")
                else:
                    cmd.append(f"{package}=={version}")
            self._call(prefix, cmd)
            with open(report, mode="r", encoding="utf-8") as f:
                return [
                    {k: v for k, v in item["download_info"].items() if k in ["url"]}
                    for item in json.load(f)["install"]
                ]

    def download(self, id_, packages, python, platform):
        prefix = self.micromamba.path_to_environment(id_)
        metadata_file = METADATA_FILE.format(prefix=prefix)
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
        extra_index_urls = self.extra_index_urls(prefix)
        cmd = [
            "download",
            "--no-deps",
            "--no-index",
            "--progress-bar=off",
            #  if packages are present in Pip cache, this will be a local copy
            "--dest=%s/.pip/wheels" % prefix,
            "--quiet",
            *(
                chain.from_iterable(
                    product(["--extra-index-url"], set(extra_index_urls))
                )
            ),
            *(chain.from_iterable(product(["--abi"], set(abis)))),
            *(chain.from_iterable(product(["--platform"], set(platforms)))),
            # *(chain.from_iterable(product(["--implementations"], set(implementations)))),
        ]
        for package in packages:
            cmd.append("{url}".format(**package))
            metadata["{url}".format(**package)] = "{prefix}/.pip/wheels/{wheel}".format(
                prefix=prefix, wheel=package["url"].split("/")[-1]
            )
        self._call(prefix, cmd)
        # write the url to wheel mappings in a magic location
        with open(metadata_file, "w") as file:
            file.write(json.dumps(metadata))

    def create(self, id_, packages, python, platform):
        prefix = self.micromamba.path_to_environment(id_)
        installation_marker = INSTALLATION_MARKER.format(prefix=prefix)
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
                cmd.append("{url}".format(**package))
            self._call(prefix, cmd)
        with open(installation_marker, "w") as file:
            file.write(json.dumps({"id": id_}))

    def metadata(self, id_, packages, python, platform):
        # read the url to wheel mappings from a magic location
        prefix = self.micromamba.path_to_environment(id_)
        metadata_file = METADATA_FILE.format(prefix=prefix)
        with open(metadata_file, "r") as file:
            return json.loads(file.read())

    def extra_index_urls(self, prefix):
        # get extra index urls from Pip conf
        extra_indices = []
        for key in [":env:.extra-index-url", "global.extra-index-url"]:
            try:
                extras = self._call(prefix, args=["config", "get", key], isolated=False)
                extra_indices.extend(extras.split(" "))
            except Exception:
                # Pip will throw an error when trying to get a config key that does
                # not exist
                pass
        return extra_indices

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
