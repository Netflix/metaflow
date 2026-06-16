# Support saving of distribution information so we can give it back to users even
# if we do not install those distributions. This is used to package distributions in
# the MetaflowCodeContent package and provide an experience as if the packages were installed
# system-wide.

import os
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import (
    Callable,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    TYPE_CHECKING,
    Union,
    cast,
)

import inspect
from collections import defaultdict

from ..extension_support import metadata
from ..util import get_metaflow_root

if TYPE_CHECKING:
    import pathlib

_cached_distributions = None

packages_distributions = None  # type: Optional[Callable[[], Mapping[str, List[str]]]]
name_normalizer = re.compile(r"[-_.]+")

if sys.version_info[:2] >= (3, 10):
    packages_distributions = metadata.packages_distributions
else:
    # This is the code present in 3.10+ -- we replicate here for other versions
    def _packages_distributions() -> Mapping[str, List[str]]:
        """
        Return a mapping of top-level packages to their
        distributions.
        """
        pkg_to_dist = defaultdict(list)
        for dist in metadata.distributions():
            for pkg in _top_level_declared(dist) or _top_level_inferred(dist):
                pkg_to_dist[pkg].append(dist.metadata["Name"])
        return dict(pkg_to_dist)

    def _top_level_declared(dist: metadata.Distribution) -> List[str]:
        return (dist.read_text("top_level.txt") or "").split()

    def _topmost(name: "pathlib.PurePosixPath") -> Optional[str]:
        """
        Return the top-most parent as long as there is a parent.
        """
        top, *rest = name.parts
        return top if rest else None

    def _get_toplevel_name(name: "pathlib.PurePosixPath") -> str:
        return _topmost(name) or (
            # python/typeshed#10328
            inspect.getmodulename(name)  # type: ignore
            or str(name)
        )

    def _top_level_inferred(dist: "metadata.Distribution"):
        opt_names = set(map(_get_toplevel_name, dist.files or []))

        def importable_name(name):
            return "." not in name

        return filter(importable_name, opt_names)

    packages_distributions = _packages_distributions


def modules_to_distributions() -> Dict[str, List[metadata.Distribution]]:
    """
    Return a mapping of top-level modules to their distributions.

    Returns
    -------
    Dict[str, List[metadata.Distribution]]
        A mapping of top-level modules to their distributions.
    """
    global _cached_distributions
    pd = cast(Callable[[], Mapping[str, List[str]]], packages_distributions)
    if _cached_distributions is None:
        _cached_distributions = {
            k: [metadata.distribution(d) for d in v] for k, v in pd().items()
        }
    return _cached_distributions


_ModuleInfo = NamedTuple(
    "_ModuleInfo",
    [
        ("name", str),
        ("root_paths", Set[str]),
        ("module", ModuleType),
        ("metaflow_module", bool),
    ],
)


class PackagedDistribution(metadata.Distribution):
    """
    A Python Package packaged within a MetaflowCodeContent. This allows users to use use importlib
    as they would regularly and the packaged Python Package would be considered as a
    distribution even if it really isn't (since it is just included in the PythonPath).
    """

    def __init__(self, root: str, content: Dict[str, str]):
        self._root = Path(root)
        self._content = content

    # Strongly inspired from PathDistribution in metadata.py
    def read_text(self, filename: Union[str, os.PathLike]) -> Optional[str]:
        if str(filename) in self._content:
            return self._content[str(filename)]
        return None

    read_text.__doc__ = metadata.Distribution.read_text.__doc__

    # Returns a metadata.SimplePath but not always present in importlib.metadata libs so
    # skipping return type.
    def locate_file(self, path: Union[str, os.PathLike]):
        return self._root / path


class PackagedDistributionFinder(metadata.DistributionFinder):
    def __init__(self, dist_info: Dict[str, Dict[str, str]]):
        self._dist_info = dist_info

    def find_distributions(self, context=metadata.DistributionFinder.Context()):
        if context.name is None:
            # Yields all known distributions
            for name, info in self._dist_info.items():
                yield PackagedDistribution(
                    os.path.join(get_metaflow_root(), name), info
                )
            return None
        name = name_normalizer.sub("-", cast(str, context.name)).lower()
        if name in self._dist_info:
            yield PackagedDistribution(
                os.path.join(get_metaflow_root(), cast(str, context.name)),
                self._dist_info[name],
            )
        return None
