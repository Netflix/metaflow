import os
from contextlib import contextmanager
from typing import Callable, Generator, List, Optional, Tuple

from ..util import to_unicode


# this is os.walk(follow_symlinks=True) with cycle detection
def walk_without_cycles(
    top_root: str,
    exclude_dirs: Optional[List[str]] = None,
) -> Generator[Tuple[str, List[str]], None, None]:
    seen = set()

    default_skip_dirs = ["__pycache__"]

    def _recurse(root, skip_dirs):
        for parent, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for d in dirs:
                path = os.path.join(parent, d)
                if os.path.islink(path):
                    # Breaking loops: never follow the same symlink twice
                    #
                    # NOTE: this also means that links to sibling links are
                    # not followed. In this case:
                    #
                    #   x -> y
                    #   y -> oo
                    #   oo/real_file
                    #
                    # real_file is only included twice, not three times
                    reallink = os.path.realpath(path)
                    if reallink not in seen:
                        seen.add(reallink)
                        for x in _recurse(path, default_skip_dirs):
                            yield x
            yield parent, files

    skip_dirs = set(default_skip_dirs + (exclude_dirs or []))
    for x in _recurse(top_root, skip_dirs):
        skip_dirs = default_skip_dirs
        yield x


def walk(
    root: str,
    exclude_hidden: bool = True,
    file_filter: Optional[Callable[[str], bool]] = None,
    exclude_tl_dirs: Optional[List[str]] = None,
) -> Generator[Tuple[str, str], None, None]:
    root = to_unicode(root)  # handle files/folder with non ascii chars
    prefixlen = len("%s/" % os.path.dirname(root))
    for (
        path,
        files,
    ) in walk_without_cycles(root, exclude_tl_dirs):
        if exclude_hidden and "/." in path:
            continue
        # path = path[2:] # strip the ./ prefix
        # if path and (path[0] == '.' or './' in path):
        #    continue
        for fname in files:
            if file_filter is None or file_filter(fname):
                p = os.path.join(path, fname)
                yield p, p[prefixlen:]


def suffix_filter(suffixes: List[str]) -> Callable[[str], bool]:
    """
    Returns a filter function that checks if a file ends with any of the given suffixes.
    """
    suffixes = [s.lower() for s in suffixes]

    def _filter(fname: str) -> bool:
        fname = fname.lower()
        return (
            suffixes is None
            or (fname[0] == "." and fname in suffixes)
            or (fname[0] != "." and any(fname.endswith(suffix) for suffix in suffixes))
        )

    return _filter


@contextmanager
def with_dir(new_dir):
    current_dir = os.getcwd()
    os.chdir(new_dir)
    yield new_dir
    os.chdir(current_dir)
