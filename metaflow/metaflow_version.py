#!/usr/bin/env python
"""Get version identification for the package

See the documentation of get_version for more information

"""

# This file is adapted from https://github.com/aebrahim/python-git-version

from subprocess import check_output, CalledProcessError
from os import path, name, devnull, environ, listdir
import json

from metaflow import CURRENT_DIRECTORY, INFO_FILE

__all__ = ("get_version",)

GIT_COMMAND = "git"

if name == "nt":

    def find_git_on_windows():
        """find the path to the git executable on Windows"""
        # first see if git is in the path
        try:
            check_output(["where", "/Q", "git"])
            # if this command succeeded, git is in the path
            return "git"
        # catch the exception thrown if git was not found
        except CalledProcessError:
            pass
        # There are several locations where git.exe may be hiding
        possible_locations = []
        # look in program files for msysgit
        if "PROGRAMFILES(X86)" in environ:
            possible_locations.append(
                "%s/Git/cmd/git.exe" % environ["PROGRAMFILES(X86)"]
            )
        if "PROGRAMFILES" in environ:
            possible_locations.append("%s/Git/cmd/git.exe" % environ["PROGRAMFILES"])
        # look for the GitHub version of git
        if "LOCALAPPDATA" in environ:
            github_dir = "%s/GitHub" % environ["LOCALAPPDATA"]
            if path.isdir(github_dir):
                for subdir in listdir(github_dir):
                    if not subdir.startswith("PortableGit"):
                        continue
                    possible_locations.append(
                        "%s/%s/bin/git.exe" % (github_dir, subdir)
                    )
        for possible_location in possible_locations:
            if path.isfile(possible_location):
                return possible_location
        # git was not found
        return "git"

    GIT_COMMAND = find_git_on_windows()


def call_git_describe(abbrev=7):
    """return the string output of git describe"""
    try:

        # first, make sure we are actually in a Metaflow repo,
        # not some other repo
        with open(devnull, "w") as fnull:
            arguments = [GIT_COMMAND, "rev-parse", "--show-toplevel"]
            reponame = (
                check_output(arguments, cwd=CURRENT_DIRECTORY, stderr=fnull)
                .decode("ascii")
                .strip()
            )
            if path.basename(reponame) != "metaflow":
                return None

        with open(devnull, "w") as fnull:
            arguments = [GIT_COMMAND, "describe", "--tags", "--abbrev=%d" % abbrev]
            return (
                check_output(arguments, cwd=CURRENT_DIRECTORY, stderr=fnull)
                .decode("ascii")
                .strip()
            )

    except (OSError, CalledProcessError):
        return None


def format_git_describe(git_str, pep440=False):
    """format the result of calling 'git describe' as a python version"""
    if git_str is None:
        return None
    if "-" not in git_str:  # currently at a tag
        return git_str
    else:
        # formatted as version-N-githash
        # want to convert to version.postN-githash
        git_str = git_str.replace("-", ".post", 1)
        if pep440:  # does not allow git hash afterwards
            return git_str.split("-")[0]
        else:
            return git_str.replace("-g", "+git")


def read_info_version():
    """Read version information from INFO file"""
    try:
        with open(INFO_FILE, "r") as contents:
            return json.load(contents).get("metaflow_version")
    except IOError:
        return None


def get_version(pep440=False):
    """Tracks the version number.

    pep440: bool
        When True, this function returns a version string suitable for
        a release as defined by PEP 440. When False, the githash (if
        available) will be appended to the version string.

    If the script is located within an active git repository,
    git-describe is used to get the version information.

    Otherwise, the version logged by package installer is returned.

    If even that information isn't available (likely when executing on a
    remote cloud instance), the version information is returned from INFO file
    in the current directory.

    """

    version = format_git_describe(call_git_describe(), pep440=pep440)
    version_addl = None
    if version is None:  # not a git repository
        import metaflow

        version = metaflow.__version__
        version_addl = metaflow.__version_addl__
    if version is None:  # not a proper python package
        return read_info_version()
    if version_addl:
        return "+".join([version, version_addl])
    return version
