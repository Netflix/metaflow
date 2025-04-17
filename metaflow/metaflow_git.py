#!/usr/bin/env python
"""Get git repository information for the package

Functions to retrieve git repository details like URL, branch name, 
and commit SHA for Metaflow code provenance tracking.
"""

import os
import subprocess
from typing import Dict, List, Optional, Tuple, Union

# Cache for git information to avoid repeated subprocess calls
_git_info_cache = None

__all__ = ("get_repository_info",)


def _call_git(
    args: List[str], path=Union[str, os.PathLike]
) -> Tuple[Optional[str], Optional[int], bool]:
    """
    Call git with provided args.

    Returns
    -------
        tuple : Tuple containing
            (stdout, exitcode, failure) of the call
    """
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=path,
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.strip(), result.returncode, False
    except (OSError, subprocess.SubprocessError):
        # Covers subprocess timeouts and other errors which would not lead to an exit code
        return None, None, True


def _get_repo_url(path: Union[str, os.PathLike]) -> Optional[str]:
    """Get the repository URL from git config"""
    stdout, returncode, _failed = _call_git(
        ["config", "--get", "remote.origin.url"], path
    )
    if returncode == 0:
        url = stdout
        # Convert SSH URLs to HTTPS for clickable links
        if url.startswith("git@"):
            parts = url.split(":", 1)
            if len(parts) == 2:
                domain = parts[0].replace("git@", "")
                repo_path = parts[1]
                url = f"https://{domain}/{repo_path}"
        return url
    return None


def _get_branch_name(path: Union[str, os.PathLike]) -> Optional[str]:
    """Get the current git branch name"""
    stdout, returncode, _failed = _call_git(["rev-parse", "--abbrev-ref", "HEAD"], path)
    return stdout if returncode == 0 else None


def _get_commit_sha(path: Union[str, os.PathLike]) -> Optional[str]:
    """Get the current git commit SHA"""
    stdout, returncode, _failed = _call_git(["rev-parse", "HEAD"], path)
    return stdout if returncode == 0 else None


def _is_in_git_repo(path: Union[str, os.PathLike]) -> bool:
    """Check if we're currently in a git repository"""
    stdout, returncode, _failed = _call_git(
        ["rev-parse", "--is-inside-work-tree"], path
    )
    return returncode == 0 and stdout == "true"


def _has_uncommitted_changes(path: Union[str, os.PathLike]) -> Optional[bool]:
    """Check if the git repository has uncommitted changes"""
    _stdout, returncode, failed = _call_git(
        ["diff-index", "--quiet", "HEAD", "--"], path
    )
    if failed:
        return None
    return returncode != 0


def get_repository_info(path: Union[str, os.PathLike]) -> Dict[str, Union[str, bool]]:
    """Get git repository information for a path

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
    if _is_in_git_repo(path):
        _git_info_cache = {
            "repo_url": _get_repo_url(path),
            "branch_name": _get_branch_name(path),
            "commit_sha": _get_commit_sha(path),
            "has_uncommitted_changes": _has_uncommitted_changes(path),
        }

    return _git_info_cache
