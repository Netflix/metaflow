#!/usr/bin/env python
"""Get version identification for the package

See the documentation of get_version for more information

"""

# This file is adapted from https://github.com/aebrahim/python-git-version

import subprocess
from os import path, name, environ, listdir

from metaflow.extension_support import update_package_info
from metaflow.meta_files import read_info_file


# True/False correspond to the value `public`` in get_version
_version_cache = {True: None, False: None}

__all__ = ("get_version",)

GIT_COMMAND = "git"

if name == "nt":

    def find_git_on_windows():
        """find the path to the git executable on Windows"""
        # first see if git is in the path
        try:
            subprocess.check_output(["where", "/Q", "git"])
            # if this command succeeded, git is in the path
            return "git"
        # catch the exception thrown if git was not found
        except subprocess.CalledProcessError:
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
        dirty = "-" + splits[3]
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

    global _version_cache

    # To get the version we do the following:
    #  - Check if we have a cached version. If so, return that
    #  - Then check if we have an INFO file present. If so, use that as it is
    #    the most reliable way to get the version. In particular, when running remotely,
    #    metaflow is installed in a directory and if any extension is using distutils to
    #    determine its version, this would return None and querying the version directly
    #    from the extension would fail to produce the correct result
    #  - Then if we are in the GIT repository and if so, use the git describe
    #  - If we don't have an INFO file, we look at the version information that is
    #    populated by metaflow and the extensions.

    if _version_cache[public] is not None:
        return _version_cache[public]

    version = (
        read_info_version()
    )  # Version info is cached in INFO file; includes extension info

    if version:
        _version_cache[public] = version
        return version

    # Get the version for Metaflow, favor the GIT version
    import metaflow

    version = format_git_describe(
        call_git_describe(file_to_check=metaflow.__file__), public=public
    )
    if version is None:
        version = metaflow.__version__

    # Look for extensions and compute their versions. Properly formed extensions have
    # a toplevel file which will contain a __mf_extensions__ value and a __version__
    # value. We already saved the properly formed modules when loading metaflow in
    # __ext_tl_modules__.
    ext_versions = []
    for pkg_name, extension_module in metaflow.__ext_tl_modules__:
        ext_name = getattr(extension_module, "__mf_extensions__", "<unk>")
        ext_version = format_git_describe(
            call_git_describe(file_to_check=extension_module.__file__), public=public
        )
        if ext_version is None:
            ext_version = getattr(extension_module, "__version__", "<unk>")
        # Update the package information about reported version for the extension
        # (only for the full info which is called at least once -- if we update more
        # it will error out since we can only update_package_info once)
        if not public:
            update_package_info(
                package_name=pkg_name,
                extension_name=ext_name,
                package_version=ext_version,
            )
        ext_versions.append("%s(%s)" % (ext_name, ext_version))

    # We now have all the information about extensions so we can form the final string
    if ext_versions:
        version = version + "+" + ";".join(ext_versions)
    _version_cache[public] = version
    return version
