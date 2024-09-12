#!/usr/bin/env python
"""Get version identification for the package

See the documentation of get_version for more information

"""

# This file is adapted from https://github.com/aebrahim/python-git-version

import subprocess
from os import path, name, environ, listdir

from metaflow.info_file import CURRENT_DIRECTORY, read_info_file

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


def call_git_describe(file_to_check, abbrev=7):
    """return the string output of git describe"""
    try:
        wd = path.dirname(file_to_check)
        filename = path.basename(file_to_check)

        # First check if the file is tracked in the GIT repository we are in
        # We do this because in some setups and for some bizarre reason, python files
        # are installed directly into a git repository (I am looking at you brew). We
        # don't want to consider this a GIT install in that case.
        args = [GIT_COMMAND, "ls-files", "--error-unmatch", filename]
        git_return_code = subprocess.run(
            args,
            cwd=wd,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            check=False,
        ).returncode

        if git_return_code != 0:
            return None

        args = [
            GIT_COMMAND,
            "describe",
            "--tags",
            "--dirty",
            "--long",
            "--abbrev=%d" % abbrev,
        ]
        return (
            subprocess.check_output(args, cwd=wd, stderr=subprocess.DEVNULL)
            .decode("ascii")
            .strip()
        )

    except (OSError, subprocess.CalledProcessError):
        return None


def format_git_describe(git_str, public=False):
    """format the result of calling 'git describe' as a python version"""
    if git_str is None:
        return None
    splits = git_str.split("-")
    if len(splits) == 4:
        # Formatted as <tag>-<post>-<hash>-dirty
        tag, post, h = splits[:3]
        dirty = splits[3]
    else:
        # Formatted as <tag>-<post>-<hash>
        tag, post, h = splits
        dirty = ""
    if post == "0":
        if public:
            return tag
        return tag + dirty

    if public:
        return "%s.post%s" % (tag, post)

    return "%s.post%s-git%s%s" % (tag, post, h[1:], dirty)


def read_info_version():
    """Read version information from INFO file"""
    info_file = read_info_file()
    if info_file:
        return info_file.get("metaflow_version")
    return None


def get_version(public=False):
    """Tracks the version number.

    public: bool
        When True, this function returns a *public* version specification which
        doesn't include any local information (dirtiness or hash). See
        https://packaging.python.org/en/latest/specifications/version-specifiers/#version-scheme

    We first check the INFO file to see if we recorded a version of Metaflow. If there
    is none, we check if we are in a GIT repository and if so, form the version
    from that.

    Otherwise, we return the version of Metaflow that was installed.

    """

    # To get the version we do the following:
    #  - First check if we have an INFO file present. If so, use that as it is
    #    the most reliable way to get the version. In particular, when running remotely,
    #    metaflow is installed in a directory and if any extension is using distutils to
    #    determine its version, this would return None and querying the version directly
    #    from the extension would fail to produce the correct result
    #  - Check if we are in the GIT repository and if so, use the git describe
    #  - If we don't have an INFO file, we look at the version information that is
    #    populated by metaflow and the extensions.
    version = (
        read_info_version()
    )  # Version info is cached in INFO file; includes extension info
    if version:
        return version

    import metaflow

    version_addl = metaflow.__version_addl__
    version_override = metaflow.__version_override__

    version = format_git_describe(
        call_git_describe(file_to_check=metaflow.__file__), public=public
    )

    if version is None:
        version = version_override or metaflow.__version__

    if version_addl:
        return "+".join([version, version_addl])

    return version
