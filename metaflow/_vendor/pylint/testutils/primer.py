import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

import git

PRIMER_DIRECTORY_PATH = Path(".pylint_primer_tests")


class PackageToLint:
    """Represents data about a package to be tested during primer tests"""

    url: str
    """URL of the repository to clone"""

    branch: str
    """Branch of the repository to clone"""

    directories: List[str]
    """Directories within the repository to run pylint over"""

    commit: Optional[str]
    """Commit hash to pin the repository on"""

    pylint_additional_args: List[str]
    """Arguments to give to pylint"""

    pylintrc_relpath: Optional[str]
    """Path relative to project's main directory to the pylintrc if it exists"""

    def __init__(
        self,
        url: str,
        branch: str,
        directories: List[str],
        commit: Optional[str] = None,
        pylint_additional_args: Optional[List[str]] = None,
        pylintrc_relpath: Optional[str] = None,
    ) -> None:
        self.url = url
        self.branch = branch
        self.directories = directories
        self.commit = commit
        self.pylint_additional_args = pylint_additional_args or []
        self.pylintrc_relpath = pylintrc_relpath

    @property
    def pylintrc(self) -> Optional[Path]:
        if self.pylintrc_relpath is None:
            return None
        return self.clone_directory / self.pylintrc_relpath

    @property
    def clone_directory(self) -> Path:
        """Directory to clone repository into"""
        clone_name = "/".join(self.url.split("/")[-2:]).replace(".git", "")
        return PRIMER_DIRECTORY_PATH / clone_name

    @property
    def paths_to_lint(self) -> List[str]:
        """The paths we need to lint"""
        return [str(self.clone_directory / path) for path in self.directories]

    @property
    def pylint_args(self) -> List[str]:
        options: List[str] = []
        if self.pylintrc is not None:
            # There is an error if rcfile is given but does not exists
            options += [f"--rcfile={self.pylintrc}"]
        return self.paths_to_lint + options + self.pylint_additional_args

    def lazy_clone(self) -> None:  # pragma: no cover
        """Concatenates the target directory and clones the file

        Not expected to be tested as the primer won't work if it doesn't.
        It's tested in the continuous integration primers, only the coverage
        is not calculated on everything. If lazy clone breaks for local use
        we'll probably notice because we'll have a fatal when launching the
        primer locally.
        """
        logging.info("Lazy cloning %s", self.url)
        if not self.clone_directory.exists():
            options: Dict[str, Union[str, int]] = {
                "url": self.url,
                "to_path": str(self.clone_directory),
                "branch": self.branch,
                "depth": 1,
            }
            logging.info("Directory does not exists, cloning: %s", options)
            git.Repo.clone_from(**options)
            return

        remote_sha1_commit = (
            git.cmd.Git().ls_remote(self.url, self.branch).split("\t")[0]
        )
        local_sha1_commit = git.Repo(self.clone_directory).head.object.hexsha
        if remote_sha1_commit != local_sha1_commit:
            logging.info(
                "Remote sha is '%s' while local sha is '%s': pulling new commits",
                remote_sha1_commit,
                local_sha1_commit,
            )
            repo = git.Repo(self.clone_directory)
            origin = repo.remotes.origin
            origin.pull()
        else:
            logging.info("Repository already up to date.")
