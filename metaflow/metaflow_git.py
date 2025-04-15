#!/usr/bin/env python
"""Get git repository information for the package

Functions to retrieve git repository details like URL, branch name, 
and commit SHA for Metaflow code provenance tracking.
"""

import os
import subprocess
from typing import Dict, List, Optional, Union

# Cache for git information to avoid repeated subprocess calls
_git_info_cache = None

__all__ = ("get_repository_info",)


def _call_git(args: List[str], path=Union[str, os.PathLike]):
    result = subprocess.run(
        ["git", *args],
        cwd=path,
        capture_output=True,
        text=True,
        check=False,
    )
    return result


def _get_repo_url(path: Union[str, os.PathLike]) -> Optional[str]:
    """Get the repository URL from git config"""
    try:
        result = _call_git(["config", "--get", "remote.origin.url"], path)
        if result.returncode == 0:
            url = result.stdout.strip()
            # Convert SSH URLs to HTTPS for clickable links
            if url.startswith("git@"):
                parts = url.split(":", 1)
                if len(parts) == 2:
                    domain = parts[0].replace("git@", "")
                    repo_path = parts[1]
                    url = f"https://{domain}/{repo_path}"
            return url
        return None
    except (OSError, subprocess.SubprocessError):
        return None


def _get_branch_name(path: Union[str, os.PathLike]) -> Optional[str]:
    """Get the current git branch name"""
    try:
        result = _call_git(["rev-parse", "--abbrev-ref", "HEAD"], path)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _get_commit_sha(path: Union[str, os.PathLike]) -> Optional[str]:
    """Get the current git commit SHA"""
    try:
        result = _call_git(["rev-parse", "HEAD"], path)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _is_in_git_repo(path: Union[str, os.PathLike]) -> bool:
    """Check if we're currently in a git repository"""
    try:
        result = _call_git(["rev-parse", "--is-inside-work-tree"], path)
        return result.returncode == 0 and result.stdout.strip() == "true"
    except (OSError, subprocess.SubprocessError):
        return False


def _has_uncommitted_changes(path: Union[str, os.PathLike]) -> Optional[bool]:
    """Check if the git repository has uncommitted changes"""
    try:
        result = _call_git(["status", "--porcelain"], path)
        # If output is not empty, there are uncommitted changes
        return result.returncode == 0 and bool(result.stdout.strip())
    except (OSError, subprocess.SubprocessError):
        return None


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
