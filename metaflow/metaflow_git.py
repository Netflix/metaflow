#!/usr/bin/env python
"""Get git repository information for the package

Functions to retrieve git repository details like URL, branch name, 
and commit SHA for Metaflow code provenance tracking.
"""

import os
import sys
import subprocess
from os import path, name, environ

# Cache for git information to avoid repeated subprocess calls
_git_info_cache = None

__all__ = ("get_git_info",)

GIT_COMMAND = "git"

if name == "nt":
    # Use the same git command finding logic as in metaflow_version.py
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
                for subdir in os.listdir(github_dir):
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


def _get_repo_url():
    """Get the repository URL from git config"""
    try:
        result = subprocess.run(
            [GIT_COMMAND, "config", "--get", "remote.origin.url"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            url = result.stdout.strip()
            # Convert SSH URLs to HTTPS for clickable links
            if url.startswith("git@"):
                parts = url.split(":")
                if len(parts) == 2:
                    domain = parts[0].replace("git@", "")
                    repo_path = parts[1]
                    url = f"https://{domain}/{repo_path}"
            return url
        return None
    except (OSError, subprocess.SubprocessError):
        return None


def _get_branch_name():
    """Get the current git branch name"""
    try:
        result = subprocess.run(
            [GIT_COMMAND, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _get_commit_sha():
    """Get the current git commit SHA"""
    try:
        result = subprocess.run(
            [GIT_COMMAND, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _is_in_git_repo():
    """Check if we're currently in a git repository"""
    try:
        result = subprocess.run(
            [GIT_COMMAND, "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0 and result.stdout.strip() == "true"
    except (OSError, subprocess.SubprocessError):
        return False


def _has_uncommitted_changes():
    """Check if the git repository has uncommitted changes"""
    try:
        result = subprocess.run(
            [GIT_COMMAND, "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=False,
        )
        # If output is not empty, there are uncommitted changes
        return result.returncode == 0 and bool(result.stdout.strip())
    except (OSError, subprocess.SubprocessError):
        return None


def get_git_info():
    """Get git repository information for the current project

    Returns:
        dict: Dictionary containing:
            repo_url: Repository URL (converted to HTTPS if from SSH)
            branch_name: Current branch name
            commit_sha: Current commit SHA
            has_uncommitted_changes: Boolean indicating if there are uncommitted changes
    """
    global _git_info_cache

    if _git_info_cache is not None:
        return _git_info_cache

    _git_info_cache = {}
    if _is_in_git_repo():
        _git_info_cache = {
            "repo_url": _get_repo_url(),
            "branch_name": _get_branch_name(),
            "commit_sha": _get_commit_sha(),
            "has_uncommitted_changes": _has_uncommitted_changes(),
        }

    return _git_info_cache
