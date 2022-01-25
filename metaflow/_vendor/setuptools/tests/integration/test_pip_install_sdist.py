"""Integration tests for setuptools that focus on building packages via pip.

The idea behind these tests is not to exhaustively check all the possible
combinations of packages, operating systems, supporting libraries, etc, but
rather check a limited number of popular packages and how they interact with
the exposed public API. This way if any change in API is introduced, we hope to
identify backward compatibility problems before publishing a release.

The number of tested packages is purposefully kept small, to minimise duration
and the associated maintenance cost (changes in the way these packages define
their build process may require changes in the tests).
"""
import json
import os
import shutil
import sys
from enum import Enum
from glob import glob
from hashlib import md5
from urllib.request import urlopen

import pytest
from packaging.requirements import Requirement

from .helpers import Archive, run


pytestmark = pytest.mark.integration

LATEST, = list(Enum("v", "LATEST"))
"""Default version to be checked"""
# There are positive and negative aspects of checking the latest version of the
# packages.
# The main positive aspect is that the latest version might have already
# removed the use of APIs deprecated in previous releases of setuptools.


# Packages to be tested:
# (Please notice the test environment cannot support EVERY library required for
# compiling binary extensions. In Ubuntu/Debian nomenclature, we only assume
# that `build-essential`, `gfortran` and `libopenblas-dev` are installed,
# due to their relevance to the numerical/scientific programming ecosystem)
EXAMPLES = [
    ("pandas", LATEST),  # cython + custom build_ext
    ("sphinx", LATEST),  # custom setup.py
    ("pip", LATEST),  # just in case...
    ("pytest", LATEST),  # uses setuptools_scm
    ("mypy", LATEST),  # custom build_py + ext_modules

    # --- Popular packages: https://hugovk.github.io/top-pypi-packages/ ---
    ("botocore", LATEST),
    ("kiwisolver", "1.3.2"),  # build_ext, version pinned due to setup_requires
    ("brotli", LATEST),  # not in the list but used by urllib3

    # When adding packages to this list, make sure they expose a `__version__`
    # attribute, or modify the tests bellow
]


# Some packages have "optional" dependencies that modify their build behaviour
# and are not listed in pyproject.toml, others still use `setup_requires`
EXTRA_BUILD_DEPS = {
    "sphinx": ("babel>=1.3",),
    "kiwisolver": ("cppy>=1.1.0",)
}


VIRTUALENV = (sys.executable, "-m", "virtualenv")


# By default, pip will try to build packages in isolation (PEP 517), which
# means it will download the previous stable version of setuptools.
# `pip` flags can avoid that (the version of setuptools under test
# should be the one to be used)
SDIST_OPTIONS = (
    "--ignore-installed",
    "--no-build-isolation",
    # We don't need "--no-binary :all:" since we specify the path to the sdist.
    # It also helps with performance, since dependencies can come from wheels.
)
# The downside of `--no-build-isolation` is that pip will not download build
# dependencies. The test script will have to also handle that.


@pytest.fixture
def venv_python(tmp_path):
    run([*VIRTUALENV, str(tmp_path / ".venv")])
    possible_path = (str(p.parent) for p in tmp_path.glob(".venv/*/python*"))
    return shutil.which("python", path=os.pathsep.join(possible_path))


@pytest.fixture(autouse=True)
def _prepare(tmp_path, venv_python, monkeypatch, request):
    download_path = os.getenv("DOWNLOAD_PATH", str(tmp_path))
    os.makedirs(download_path, exist_ok=True)

    # Environment vars used for building some of the packages
    monkeypatch.setenv("USE_MYPYC", "1")

    def _debug_info():
        # Let's provide the maximum amount of information possible in the case
        # it is necessary to debug the tests directly from the CI logs.
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("Temporary directory:")
        map(print, tmp_path.glob("*"))
        print("Virtual environment:")
        run([venv_python, "-m", "pip", "freeze"])
    request.addfinalizer(_debug_info)


ALREADY_LOADED = ("pytest", "mypy")  # loaded by pytest/pytest-enabler


@pytest.mark.parametrize('package, version', EXAMPLES)
def test_install_sdist(package, version, tmp_path, venv_python, setuptools_wheel):
    venv_pip = (venv_python, "-m", "pip")
    sdist = retrieve_sdist(package, version, tmp_path)
    deps = build_deps(package, sdist)
    if deps:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("Dependencies:", deps)
        run([*venv_pip, "install", *deps])

    # Use a virtualenv to simulate PEP 517 isolation
    # but install fresh setuptools wheel to ensure the version under development
    run([*venv_pip, "install", "-I", setuptools_wheel])
    run([*venv_pip, "install", *SDIST_OPTIONS, sdist])

    # Execute a simple script to make sure the package was installed correctly
    script = f"import {package}; print(getattr({package}, '__version__', 0))"
    run([venv_python, "-c", script])


# ---- Helper Functions ----


def retrieve_sdist(package, version, tmp_path):
    """Either use cached sdist file or download it from PyPI"""
    # `pip download` cannot be used due to
    # https://github.com/pypa/pip/issues/1884
    # https://discuss.python.org/t/pep-625-file-name-of-a-source-distribution/4686
    # We have to find the correct distribution file and download it
    download_path = os.getenv("DOWNLOAD_PATH", str(tmp_path))
    dist = retrieve_pypi_sdist_metadata(package, version)

    # Remove old files to prevent cache to grow indefinitely
    for file in glob(os.path.join(download_path, f"{package}*")):
        if dist["filename"] != file:
            os.unlink(file)

    dist_file = os.path.join(download_path, dist["filename"])
    if not os.path.exists(dist_file):
        download(dist["url"], dist_file, dist["md5_digest"])
    return dist_file


def retrieve_pypi_sdist_metadata(package, version):
    # https://warehouse.pypa.io/api-reference/json.html
    id_ = package if version is LATEST else f"{package}/{version}"
    with urlopen(f"https://pypi.org/pypi/{id_}/json") as f:
        metadata = json.load(f)

    if metadata["info"]["yanked"]:
        raise ValueError(f"Release for {package} {version} was yanked")

    version = metadata["info"]["version"]
    release = metadata["releases"][version]
    dists = [d for d in release if d["packagetype"] == "sdist"]
    if len(dists) == 0:
        raise ValueError(f"No sdist found for {package} {version}")

    for dist in dists:
        if dist["filename"].endswith(".tar.gz"):
            return dist

    # Not all packages are publishing tar.gz
    return dist


def download(url, dest, md5_digest):
    with urlopen(url) as f:
        data = f.read()

    assert md5(data).hexdigest() == md5_digest

    with open(dest, "wb") as f:
        f.write(data)

    assert os.path.exists(dest)


def build_deps(package, sdist_file):
    """Find out what are the build dependencies for a package.

    We need to "manually" install them, since pip will not install build
    deps with `--no-build-isolation`.
    """
    import tomli as toml

    # delay importing, since pytest discovery phase may hit this file from a
    # testenv without tomli

    archive = Archive(sdist_file)
    pyproject = _read_pyproject(archive)

    info = toml.loads(pyproject)
    deps = info.get("build-system", {}).get("requires", [])
    deps += EXTRA_BUILD_DEPS.get(package, [])
    # Remove setuptools from requirements (and deduplicate)
    requirements = {Requirement(d).name: d for d in deps}
    return [v for k, v in requirements.items() if k != "setuptools"]


def _read_pyproject(archive):
    for member in archive:
        if os.path.basename(archive.get_name(member)) == "pyproject.toml":
            return archive.get_content(member)
    return ""
