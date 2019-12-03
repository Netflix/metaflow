#!/usr/bin/env python
"""Get version identification from git

See the documentation of get_version for more information

"""

# This file is imported from https://github.com/aebrahim/python-git-version

from __future__ import print_function

from subprocess import check_output, CalledProcessError
from os import path, name, devnull, environ, listdir

__all__ = ("get_version",)

CURRENT_DIRECTORY = path.dirname(path.abspath(__file__))
VERSION_FILE = path.join(CURRENT_DIRECTORY, "VERSION")

GIT_COMMAND = "git"

if name == "nt":
    def find_git_on_windows():
        """find the path to the git executable on windows"""
        # first see if git is in the path
        try:
            check_output(["where", "/Q", "git"])
            # if this command succeeded, git is in the path
            return "git"
        # catch the exception thrown if git was not found
        except CalledProcessError:
            pass
        # There are several locations git.exe may be hiding
        possible_locations = []
        # look in program files for msysgit
        if "PROGRAMFILES(X86)" in environ:
            possible_locations.append("%s/Git/cmd/git.exe" %
                                      environ["PROGRAMFILES(X86)"])
        if "PROGRAMFILES" in environ:
            possible_locations.append("%s/Git/cmd/git.exe" %
                                      environ["PROGRAMFILES"])
        # look for the github version of git
        if "LOCALAPPDATA" in environ:
            github_dir = "%s/GitHub" % environ["LOCALAPPDATA"]
            if path.isdir(github_dir):
                for subdir in listdir(github_dir):
                    if not subdir.startswith("PortableGit"):
                        continue
                    possible_locations.append("%s/%s/bin/git.exe" %
                                              (github_dir, subdir))
        for possible_location in possible_locations:
            if path.isfile(possible_location):
                return possible_location
        # git was not found
        return "git"

    GIT_COMMAND = find_git_on_windows()


def call_git_describe(abbrev=7):
    """return the string output of git desribe"""
    try:

        # first, make sure we are actually in a Metaflow repo,
        # not some other repo
        with open(devnull, 'w') as fnull:
            arguments = [GIT_COMMAND, "rev-parse", "--show-toplevel"]
            reponame = check_output(arguments, cwd=CURRENT_DIRECTORY,
                                    stderr=fnull).decode("ascii").strip()
            if path.basename(reponame) != 'metaflow':
                return None

        with open(devnull, "w") as fnull:
            arguments = [GIT_COMMAND, "describe", "--tags",
                         "--abbrev=%d" % abbrev]
            return check_output(arguments, cwd=CURRENT_DIRECTORY,
                                stderr=fnull).decode("ascii").strip()

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


def read_release_version():
    """Read version information from VERSION file"""
    try:
        with open(VERSION_FILE, "r") as infile:
            version = str(infile.read().strip())
        if len(version) == 0:
            version = None
        return version
    except IOError:
        return None


def update_release_version():
    """Update VERSION file"""
    version = get_version(pep440=True)
    with open(VERSION_FILE, "w") as outfile:
        outfile.write(version)
        outfile.write("\n")


def get_version(pep440=False):
    """Tracks the version number.

    pep440: bool
        When True, this function returns a version string suitable for
        a release as defined by PEP 440. When False, the githash (if
        available) will be appended to the version string.

    The file VERSION holds the version information. If this is not a git
    repository, then it is reasonable to assume that the version is not
    being incremented and the version returned will be the release version as
    read from the file.

    However, if the script is located within an active git repository,
    git-describe is used to get the version information.

    The file VERSION will need to be changed by manually. This should be done
    before running git tag (set to the same as the version in the tag).

    """

    git_version = format_git_describe(call_git_describe(), pep440=pep440)
    if git_version is None:  # not a git repository
        import metaflow
        return metaflow.__version__
    else:
        return git_version
