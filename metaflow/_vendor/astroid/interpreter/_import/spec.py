# Copyright (c) 2016-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2016 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2017 Chris Philip <chrisp533@gmail.com>
# Copyright (c) 2017 Hugo <hugovk@users.noreply.github.com>
# Copyright (c) 2017 ioanatia <ioanatia@users.noreply.github.com>
# Copyright (c) 2017 Calen Pennington <cale@edx.org>
# Copyright (c) 2018 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2019 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Peter Kolbus <peter.kolbus@gmail.com>
# Copyright (c) 2020 Raphael Gaschignard <raphael@rtpg.co>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 DudeNr33 <3929834+DudeNr33@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

import abc
import collections
import enum
import importlib.machinery
import os
import sys
import zipimport
from functools import lru_cache

from . import util

ModuleType = enum.Enum(
    "ModuleType",
    "C_BUILTIN C_EXTENSION PKG_DIRECTORY "
    "PY_CODERESOURCE PY_COMPILED PY_FROZEN PY_RESOURCE "
    "PY_SOURCE PY_ZIPMODULE PY_NAMESPACE",
)


_ModuleSpec = collections.namedtuple(
    "_ModuleSpec", "name type location " "origin submodule_search_locations"
)


class ModuleSpec(_ModuleSpec):
    """Defines a class similar to PEP 420's ModuleSpec

    A module spec defines a name of a module, its type, location
    and where submodules can be found, if the module is a package.
    """

    def __new__(
        cls,
        name,
        module_type,
        location=None,
        origin=None,
        submodule_search_locations=None,
    ):
        return _ModuleSpec.__new__(
            cls,
            name=name,
            type=module_type,
            location=location,
            origin=origin,
            submodule_search_locations=submodule_search_locations,
        )


class Finder:
    """A finder is a class which knows how to find a particular module."""

    def __init__(self, path=None):
        self._path = path or sys.path

    @abc.abstractmethod
    def find_module(self, modname, module_parts, processed, submodule_path):
        """Find the given module

        Each finder is responsible for each protocol of finding, as long as
        they all return a ModuleSpec.

        :param str modname: The module which needs to be searched.
        :param list module_parts: It should be a list of strings,
                                  where each part contributes to the module's
                                  namespace.
        :param list processed: What parts from the module parts were processed
                               so far.
        :param list submodule_path: A list of paths where the module
                                    can be looked into.
        :returns: A ModuleSpec, describing how and where the module was found,
                  None, otherwise.
        """

    def contribute_to_path(self, spec, processed):
        """Get a list of extra paths where this finder can search."""


class ImportlibFinder(Finder):
    """A finder based on the importlib module."""

    _SUFFIXES = (
        [(s, ModuleType.C_EXTENSION) for s in importlib.machinery.EXTENSION_SUFFIXES]
        + [(s, ModuleType.PY_SOURCE) for s in importlib.machinery.SOURCE_SUFFIXES]
        + [(s, ModuleType.PY_COMPILED) for s in importlib.machinery.BYTECODE_SUFFIXES]
    )

    def find_module(self, modname, module_parts, processed, submodule_path):
        if not isinstance(modname, str):
            raise TypeError(f"'modname' must be a str, not {type(modname)}")
        if submodule_path is not None:
            submodule_path = list(submodule_path)
        else:
            try:
                spec = importlib.util.find_spec(modname)
                if spec:
                    if spec.loader is importlib.machinery.BuiltinImporter:
                        return ModuleSpec(
                            name=modname,
                            location=None,
                            module_type=ModuleType.C_BUILTIN,
                        )
                    if spec.loader is importlib.machinery.FrozenImporter:
                        return ModuleSpec(
                            name=modname,
                            location=None,
                            module_type=ModuleType.PY_FROZEN,
                        )
            except ValueError:
                pass
            submodule_path = sys.path

        for entry in submodule_path:
            package_directory = os.path.join(entry, modname)
            for suffix in (".py", importlib.machinery.BYTECODE_SUFFIXES[0]):
                package_file_name = "__init__" + suffix
                file_path = os.path.join(package_directory, package_file_name)
                if os.path.isfile(file_path):
                    return ModuleSpec(
                        name=modname,
                        location=package_directory,
                        module_type=ModuleType.PKG_DIRECTORY,
                    )
            for suffix, type_ in ImportlibFinder._SUFFIXES:
                file_name = modname + suffix
                file_path = os.path.join(entry, file_name)
                if os.path.isfile(file_path):
                    return ModuleSpec(
                        name=modname, location=file_path, module_type=type_
                    )
        return None

    def contribute_to_path(self, spec, processed):
        if spec.location is None:
            # Builtin.
            return None

        if _is_setuptools_namespace(spec.location):
            # extend_path is called, search sys.path for module/packages
            # of this name see pkgutil.extend_path documentation
            path = [
                os.path.join(p, *processed)
                for p in sys.path
                if os.path.isdir(os.path.join(p, *processed))
            ]
        else:
            path = [spec.location]
        return path


class ExplicitNamespacePackageFinder(ImportlibFinder):
    """A finder for the explicit namespace packages, generated through pkg_resources."""

    def find_module(self, modname, module_parts, processed, submodule_path):
        if processed:
            modname = ".".join(processed + [modname])
        if util.is_namespace(modname) and modname in sys.modules:
            submodule_path = sys.modules[modname].__path__
            return ModuleSpec(
                name=modname,
                location="",
                origin="namespace",
                module_type=ModuleType.PY_NAMESPACE,
                submodule_search_locations=submodule_path,
            )
        return None

    def contribute_to_path(self, spec, processed):
        return spec.submodule_search_locations


class ZipFinder(Finder):
    """Finder that knows how to find a module inside zip files."""

    def __init__(self, path):
        super().__init__(path)
        self._zipimporters = _precache_zipimporters(path)

    def find_module(self, modname, module_parts, processed, submodule_path):
        try:
            file_type, filename, path = _search_zip(module_parts, self._zipimporters)
        except ImportError:
            return None

        return ModuleSpec(
            name=modname,
            location=filename,
            origin="egg",
            module_type=file_type,
            submodule_search_locations=path,
        )


class PathSpecFinder(Finder):
    """Finder based on importlib.machinery.PathFinder."""

    def find_module(self, modname, module_parts, processed, submodule_path):
        spec = importlib.machinery.PathFinder.find_spec(modname, path=submodule_path)
        if spec:
            # origin can be either a string on older Python versions
            # or None in case it is a namespace package:
            # https://github.com/python/cpython/pull/5481
            is_namespace_pkg = spec.origin in {"namespace", None}
            location = spec.origin if not is_namespace_pkg else None
            module_type = ModuleType.PY_NAMESPACE if is_namespace_pkg else None
            spec = ModuleSpec(
                name=spec.name,
                location=location,
                origin=spec.origin,
                module_type=module_type,
                submodule_search_locations=list(spec.submodule_search_locations or []),
            )
        return spec

    def contribute_to_path(self, spec, processed):
        if spec.type == ModuleType.PY_NAMESPACE:
            return spec.submodule_search_locations
        return None


_SPEC_FINDERS = (
    ImportlibFinder,
    ZipFinder,
    PathSpecFinder,
    ExplicitNamespacePackageFinder,
)


def _is_setuptools_namespace(location):
    try:
        with open(os.path.join(location, "__init__.py"), "rb") as stream:
            data = stream.read(4096)
    except OSError:
        return None
    else:
        extend_path = b"pkgutil" in data and b"extend_path" in data
        declare_namespace = (
            b"pkg_resources" in data and b"declare_namespace(__name__)" in data
        )
        return extend_path or declare_namespace


@lru_cache()
def _cached_set_diff(left, right):
    result = set(left)
    result.difference_update(right)
    return result


def _precache_zipimporters(path=None):
    """
    For each path that has not been already cached
    in the sys.path_importer_cache, create a new zipimporter
    instance and add it into the cache.
    Return a dict associating all paths, stored in the cache, to corresponding
    zipimporter instances.

    :param path: paths that has to be added into the cache
    :return: association between paths stored in the cache and zipimporter instances
    """
    pic = sys.path_importer_cache

    # When measured, despite having the same complexity (O(n)),
    # converting to tuples and then caching the conversion to sets
    # and the set difference is faster than converting to sets
    # and then only caching the set difference.

    req_paths = tuple(path or sys.path)
    cached_paths = tuple(pic)
    new_paths = _cached_set_diff(req_paths, cached_paths)
    # pylint: disable=no-member
    for entry_path in new_paths:
        try:
            pic[entry_path] = zipimport.zipimporter(entry_path)
        except zipimport.ZipImportError:
            continue
    return {
        key: value
        for key, value in pic.items()
        if isinstance(value, zipimport.zipimporter)
    }


def _search_zip(modpath, pic):
    for filepath, importer in list(pic.items()):
        if importer is not None:
            found = importer.find_module(modpath[0])
            if found:
                if not importer.find_module(os.path.sep.join(modpath)):
                    raise ImportError(
                        "No module named %s in %s/%s"
                        % (".".join(modpath[1:]), filepath, modpath)
                    )
                # import code; code.interact(local=locals())
                return (
                    ModuleType.PY_ZIPMODULE,
                    os.path.abspath(filepath) + os.path.sep + os.path.sep.join(modpath),
                    filepath,
                )
    raise ImportError(f"No module named {'.'.join(modpath)}")


def _find_spec_with_path(search_path, modname, module_parts, processed, submodule_path):
    finders = [finder(search_path) for finder in _SPEC_FINDERS]
    for finder in finders:
        spec = finder.find_module(modname, module_parts, processed, submodule_path)
        if spec is None:
            continue
        return finder, spec

    raise ImportError(f"No module named {'.'.join(module_parts)}")


def find_spec(modpath, path=None):
    """Find a spec for the given module.

    :type modpath: list or tuple
    :param modpath:
      split module's name (i.e name of a module or package split
      on '.'), with leading empty strings for explicit relative import

    :type path: list or None
    :param path:
      optional list of path where the module or package should be
      searched (use sys.path if nothing or None is given)

    :rtype: ModuleSpec
    :return: A module spec, which describes how the module was
             found and where.
    """
    _path = path or sys.path

    # Need a copy for not mutating the argument.
    modpath = modpath[:]

    submodule_path = None
    module_parts = modpath[:]
    processed = []

    while modpath:
        modname = modpath.pop(0)
        finder, spec = _find_spec_with_path(
            _path, modname, module_parts, processed, submodule_path or path
        )
        processed.append(modname)
        if modpath:
            submodule_path = finder.contribute_to_path(spec, processed)

        if spec.type == ModuleType.PKG_DIRECTORY:
            spec = spec._replace(submodule_search_locations=submodule_path)

    return spec
