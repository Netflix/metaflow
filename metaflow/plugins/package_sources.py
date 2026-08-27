import inspect
import os
from typing import Iterable, Tuple

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DEFAULT_PACKAGE_SUFFIXES
from metaflow.packaging_sys import ContentType
from metaflow.packaging_sys.utils import suffix_filter, walk
from metaflow.user_decorators.user_flow_decorator import FlowMutator


class package_sources(FlowMutator):
    """Include additional files or directories in a flow's code package.

    Relative source paths are resolved from the directory containing the flow
    file, not from the current working directory. By default, each source is
    placed in the code package under its basename.

    Parameters
    ----------
    sources : path-like, (path-like, path-like), or iterable of these
        A source file or directory, a ``(source, arcname)`` pair, or multiple
        source specifications. Directories are traversed recursively.

        A source may be absolute or relative to the flow file. The optional
        ``arcname`` in a pair specifies where that source is placed inside the
        code package.

        Use a list to specify exactly two sources without archive names;
        a two-item tuple is interpreted as ``(source, arcname)``.
    arcname : path-like, optional
        Destination for a single source inside the code package. It must be a
        safe relative path and cannot be absolute, ``.``, or contain ``..``.
        For multiple sources, specify archive paths with ``(source, arcname)``
        pairs instead.
    suffixes : iterable of str or comma-separated str, optional
        File suffixes to include. Leading dots are optional and matching is
        case-insensitive. The default is ``DEFAULT_PACKAGE_SUFFIXES``
        (``.py,.R,.RDS`` by default).

        Providing this argument replaces the default suffix set; it does not
        extend it.

    Raises
    ------
    MetaflowException
        If a source does not exist, an archive path is unsafe, or ``arcname``
        is used with multiple sources.

    Examples
    --------
    Given this project layout::

        project/
        ├── flows/
        │   └── train.py
        └── src/
            └── forecasting/
                └── __init__.py

    Package ``forecasting`` at the archive root so it remains importable as
    ``import forecasting`` during remote execution::

        from metaflow import FlowSpec, package_sources

        @package_sources("../src/forecasting")
        class TrainFlow(FlowSpec):
            ...

    Package multiple sources, assigning a custom archive location to one of
    them and including JSON files::

        @package_sources(
            [
                "../shared",
                ("../generated/client", "vendor/client"),
            ],
            suffixes=[".py", ".json"],
        )
        class TrainFlow(FlowSpec):
            ...
    """

    def init(self, sources, arcname=None, suffixes=None):
        specs = self._source_specs(sources)
        if arcname is not None:
            if len(specs) != 1:
                raise MetaflowException(
                    "arcname can only be used with a single package source"
                )
            specs = ((specs[0][0], self._validate_arcname(arcname)),)

        self._sources = specs
        self._file_filter = suffix_filter(self._suffixes(suffixes))

    @classmethod
    def _source_specs(cls, sources):
        if isinstance(sources, (str, os.PathLike)) or cls._is_path_arcname_pair(
            sources
        ):
            sources = (sources,)

        specs = []
        for source in sources:
            if cls._is_path_arcname_pair(source):
                specs.append((os.fspath(source[0]), cls._validate_arcname(source[1])))
            else:
                specs.append((os.fspath(source), None))
        return tuple(specs)

    @staticmethod
    def _is_path_arcname_pair(value):
        return (
            isinstance(value, tuple)
            and len(value) == 2
            and isinstance(value[0], (str, os.PathLike))
        )

    @staticmethod
    def _validate_arcname(arcname):
        arcname = os.fspath(arcname)
        normalized = os.path.normpath(arcname)
        if (
            normalized == "."
            or os.path.isabs(normalized)
            or ".." in arcname.replace("\\", "/").split("/")
        ):
            raise MetaflowException(
                "package_sources arcname must be a relative path inside the code package"
            )
        return normalized

    @staticmethod
    def _suffixes(suffixes):
        if suffixes is None:
            suffixes = DEFAULT_PACKAGE_SUFFIXES.split(",")
        elif isinstance(suffixes, str):
            suffixes = suffixes.split(",")
        return tuple(
            suffix if suffix.startswith(".") else "." + suffix
            for suffix in (suffix.strip() for suffix in suffixes)
            if suffix
        )

    def add_to_package(self) -> Iterable[Tuple[str, str, ContentType]]:
        flow_file = inspect.getfile(self._flow_cls)
        flow_dir = os.path.dirname(os.path.abspath(flow_file))

        for source, arcname in self._sources:
            source_path = source
            if not os.path.isabs(source_path):
                source_path = os.path.join(flow_dir, source_path)
            source_path = os.path.realpath(source_path)

            if not os.path.exists(source_path):
                raise MetaflowException(
                    "package_sources source does not exist: %s" % source
                )

            root_arcname = arcname or os.path.basename(os.path.normpath(source_path))
            if os.path.isfile(source_path):
                if self._file_filter(os.path.basename(source_path)):
                    yield (source_path, root_arcname, ContentType.USER_CONTENT)
                continue

            for file_path, rel_arcname in walk(
                source_path + os.sep, file_filter=self._file_filter
            ):
                yield (
                    file_path,
                    os.path.join(root_arcname, rel_arcname),
                    ContentType.USER_CONTENT,
                )
