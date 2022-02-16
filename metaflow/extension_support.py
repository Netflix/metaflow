import importlib
import json
import os
import re
import sys
import types

from collections import defaultdict, namedtuple

from importlib.abc import MetaPathFinder, Loader
from itertools import chain

__all__ = (
    "load_module",
    "get_modules",
    "dump_module_info",
    "load_globals",
    "alias_submodules",
    "EXT_PKG",
    "lazy_load_aliases",
    "multiload_globals",
    "multiload_all",
)

EXT_PKG = "metaflow_extensions"
EXT_CONFIG_REGEXP = re.compile(r"^mfextinit_[a-zA-Z0-9_-]+\.py$")
EXT_META_REGEXP = re.compile(r"^mfextmeta_[a-zA-Z0-9_-]+\.py$")
EXT_EXCLUDE_SUFFIXES = [".pyc"]

METAFLOW_DEBUG_EXT_MECHANISM = os.environ.get("METAFLOW_DEBUG_EXT", False)


MFExtPackage = namedtuple("MFExtPackage", "package_name tl_package config_module")
MFExtModule = namedtuple("MFExtModule", "tl_package module")


def load_module(module_name):
    _ext_debug("Loading module '%s'..." % module_name)
    return _attempt_load_module(module_name)


def get_modules(extension_point):
    modules_to_load = []
    if not _mfext_supported:
        _ext_debug("Not supported for your Python version -- 3.4+ is needed")
        return []
    if extension_point not in _extension_points:
        raise RuntimeError(
            "Metaflow extension point '%s' not supported" % extension_point
        )
    _ext_debug("Getting modules for extension point '%s'..." % extension_point)
    for pkg in _pkgs_per_extension_point.get(extension_point, []):
        _ext_debug("\tFound TL '%s' from '%s'" % (pkg.tl_package, pkg.package_name))
        m = _get_extension_config(
            pkg.package_name, pkg.tl_package, extension_point, pkg.config_module
        )
        if m:
            modules_to_load.append(m)
    _ext_debug("\tLoaded %s" % str(modules_to_load))
    return modules_to_load


def dump_module_info():
    _filter_files_all()
    sanitized_all_packages = dict()
    # Strip out root_path (we don't need it and no need to expose user's dir structure)
    for k, v in _all_packages.items():
        sanitized_all_packages[k] = {
            "root_path": None,
            "meta_module": v["meta_module"],
            "files": v["files"],
        }
    return "ext_info", [sanitized_all_packages, _pkgs_per_extension_point]


def package_mfext_package(package_name):
    from metaflow.util import to_unicode

    _filter_files_package(package_name)
    pkg_info = _all_packages.get(package_name, None)
    if pkg_info and pkg_info.get("root_path", None):
        root_path = to_unicode(pkg_info["root_path"])
        for f in pkg_info["files"]:
            f_unicode = to_unicode(f)
            yield os.path.join(root_path, f_unicode), f_unicode


def package_mfext_all():
    for p in _all_packages:
        for path_tuple in package_mfext_package(p):
            yield path_tuple


def load_globals(module, dst_globals, extra_indent=False):
    if extra_indent:
        extra_indent = "\t"
    else:
        extra_indent = ""
    _ext_debug("%sLoading globals from '%s'" % (extra_indent, module.__name__))
    for n, o in module.__dict__.items():
        if not n.startswith("__") and not isinstance(o, types.ModuleType):
            _ext_debug("%s\tImporting '%s'" % (extra_indent, n))
            dst_globals[n] = o


def alias_submodules(module, tl_package, extension_point, extra_indent=False):
    if extra_indent:
        extra_indent = "\t"
    else:
        extra_indent = ""
    lazy_load_custom_modules = {}

    _ext_debug("%sAliasing submodules for '%s'" % (extra_indent, module.__name__))

    addl_modules = module.__dict__.get("__mf_promote_submodules__")
    if addl_modules:
        # We make an alias for these modules which the extension author wants to
        # expose but since it may not already be loaded, we don't load it either

        # TODO: This does not properly work for multiple packages that overwrite
        # their submodule for example if EXT_PKG.X.datatools.Y is provided
        # by two packages. For now, don't do this.
        if extension_point is not None:
            lazy_load_custom_modules.update(
                {
                    "metaflow.%s.%s"
                    % (extension_point, k): "%s.%s.%s.%s"
                    % (EXT_PKG, tl_package, extension_point, k)
                    for k in addl_modules
                }
            )
        else:
            # TL "metaflow" overrides
            lazy_load_custom_modules.update(
                {
                    "metaflow.%s" % k: "%s.%s.%s" % (EXT_PKG, tl_package, k)
                    for k in addl_modules
                }
            )
        if lazy_load_custom_modules:
            _ext_debug(
                "%s\tFound explicit promotions in __mf_promote_submodules__: %s"
                % (extra_indent, str(list(lazy_load_custom_modules.keys())))
            )
    for n, o in module.__dict__.items():
        if (
            isinstance(o, types.ModuleType)
            and o.__package__
            and o.__package__.startswith("%s.%s" % (EXT_PKG, tl_package))
        ):
            # NOTE: The condition above prohibits loading across tl_packages. We
            # can relax if needed but may not be a great idea.
            if extension_point is not None:
                lazy_load_custom_modules["metaflow.%s.%s" % (extension_point, n)] = o
            else:
                lazy_load_custom_modules["metaflow.%s" % n] = o
    _ext_debug(
        "%s\tWill create the following module aliases: %s"
        % (extra_indent, str(list(lazy_load_custom_modules.keys())))
    )
    return lazy_load_custom_modules


def lazy_load_aliases(aliases):
    if aliases:
        sys.meta_path = [_LazyFinder(aliases)] + sys.meta_path


def multiload_globals(modules, dst_globals):
    for m in modules:
        load_globals(m.module, dst_globals, extra_indent=True)


def multiload_all(modules, extension_point, dst_globals):
    for m in modules:
        # Note that we load aliases separately (as opposed to in one fell swoop) so
        # modules loaded later in `modules` can depend on them
        lazy_load_aliases(
            alias_submodules(m.module, m.tl_package, extension_point, extra_indent=True)
        )
        load_globals(m.module, dst_globals)


_py_ver = sys.version_info[0] * 100 + sys.version_info[1]
_mfext_supported = False

if _py_ver >= 304:
    import importlib.util
    from importlib.machinery import ModuleSpec

    if _py_ver >= 308:
        from importlib import metadata
    else:
        from metaflow._vendor import importlib_metadata as metadata
    _mfext_supported = True
else:
    # Something random so there is no syntax error
    ModuleSpec = None

# IMPORTANT: More specific paths must appear FIRST (before any less specific one)
_extension_points = [
    "plugins.env_escape",
    "plugins.cards",
    "config",
    "datatools",
    "exceptions",
    "plugins",
    "toplevel",
]


def _ext_debug(*args, **kwargs):
    if METAFLOW_DEBUG_EXT_MECHANISM:
        init_str = "%s:" % EXT_PKG
        print(init_str, *args, **kwargs)


def _get_extension_packages():
    if not _mfext_supported:
        _ext_debug("Not supported for your Python version -- 3.4+ is needed")
        return {}, {}

    # If we have an INFO file with the appropriate information (if running from a saved
    # code package for example), we use that directly
    # Pre-compute on _extension_points
    from metaflow import INFO_FILE

    try:
        with open(INFO_FILE, encoding="utf-8") as contents:
            all_pkg, ext_to_pkg = json.load(contents).get("ext_info", (None, None))
            if all_pkg is not None and ext_to_pkg is not None:
                _ext_debug("Loading pre-computed information from INFO file")
                # We need to properly convert stuff in ext_to_pkg
                for k, v in ext_to_pkg.items():
                    v = [MFExtPackage(*d) for d in v]
                    ext_to_pkg[k] = v
                return all_pkg, ext_to_pkg
    except IOError:
        pass

    # Check if we even have extensions
    try:
        extensions_module = importlib.import_module(EXT_PKG)
    except ImportError as e:
        if _py_ver >= 306:
            # e.name is set to the name of the package that fails to load
            # so don't error ONLY IF the error is importing this module (but do
            # error if there is a transitive import error)
            if not (isinstance(e, ModuleNotFoundError) and e.name == EXT_PKG):
                raise
            return {}, {}

    # At this point, we look at all the paths and create a set. As we find distributions
    # that match it, we will remove from the set and then will be left with any
    # PYTHONPATH "packages"
    all_paths = set(extensions_module.__path__)
    _ext_debug("Found packages present at %s" % str(all_paths))

    list_ext_points = [x.split(".") for x in _extension_points]
    init_ext_points = [x[0] for x in list_ext_points]

    # TODO: This relies only on requirements to determine import order; we may want
    # other ways of specifying "load me after this if it exists" without depending on
    # the package. One way would be to rely on the description and have that info there.
    # Not sure of the use though so maybe we can skip for now.

    # Key: distribution name/package path
    # Value: Dict containing:
    #   root_path: The root path for all the files in this package
    #   meta_module: The module to the meta file (if any) that contains information about
    #     how to package this extension
    #   files: The list of files to be included (or considered for inclusion) when
    #     packaging this extension
    mf_ext_packages = dict()

    # Key: extension point (one of _extension_point)
    # Value: another dictionary with
    #   Key: distribution name/full path to package
    #   Value: another dictionary with
    #    Key: TL package name (so in metaflow_extensions.X...., the X)
    #    Value: MFExtPackage
    extension_points_to_pkg = defaultdict(dict)
    config_to_pkg = defaultdict(list)
    meta_to_pkg = defaultdict(list)
    for dist in metadata.distributions():
        if any(
            [pkg == EXT_PKG for pkg in (dist.read_text("top_level.txt") or "").split()]
        ):
            if dist.metadata["Name"] in mf_ext_packages:
                _ext_debug(
                    "Ignoring duplicate package '%s' (duplicate paths in sys.path? (%s))"
                    % (dist.metadata["Name"], str(sys.path))
                )
                continue
            _ext_debug("Found extension package '%s'..." % dist.metadata["Name"])

            # Remove the path from the paths to search. This is not 100% accurate because
            # it is possible that at that same location there is a package and a non
            # package but it is exceedingly unlikely so we are going to ignore this.
            dist_root = dist.locate_file(EXT_PKG).as_posix()
            all_paths.discard(dist_root)

            files_to_include = []
            meta_module = None

            # At this point, we check to see what extension points this package
            # contributes to. This is to enable multiple namespace packages to contribute
            # to the same extension point (for example, you may have multiple packages
            # that have plugins)
            for f in dist.files:
                parts = list(f.parts)

                if len(parts) > 1 and parts[0] == EXT_PKG:
                    # Ensure that we don't have a __init__.py to force this package to
                    # be a NS package
                    if parts[1] == "__init__.py":
                        raise RuntimeError(
                            "Package '%s' providing '%s' is not an implicit namespace "
                            "package as required" % (dist.metadata["Name"], EXT_PKG)
                        )

                    # Record the file as a candidate for inclusion when packaging if
                    # needed
                    if not any(
                        parts[-1].endswith(suffix) for suffix in EXT_EXCLUDE_SUFFIXES
                    ):
                        files_to_include.append(f.as_posix())

                    if parts[1] in init_ext_points:
                        # This is most likely a problem as we need an intermediate
                        # "identifier"
                        raise RuntimeError(
                            "Package '%s' should conform to '%s.X.%s' and not '%s.%s' where "
                            "X is your organization's name for example"
                            % (
                                dist.metadata["Name"],
                                EXT_PKG,
                                parts[1],
                                EXT_PKG,
                                parts[1],
                            )
                        )

                    # Check for any metadata; we can only have one metadata per
                    # distribution at most
                    if EXT_META_REGEXP.match(parts[1]) is not None:
                        potential_meta_module = ".".join([EXT_PKG, parts[1][:-3]])
                        if meta_module:
                            raise RuntimeError(
                                "Package '%s' defines more than one meta configuration: "
                                "'%s' and '%s' (at least)"
                                % (
                                    dist.metadata["Name"],
                                    meta_module,
                                    potential_meta_module,
                                )
                            )
                        meta_module = potential_meta_module
                        _ext_debug(
                            "Found meta '%s' for '%s'" % (meta_module, dist_full_name)
                        )
                        meta_to_pkg[meta_module].append(dist_full_name)

                if len(parts) > 3 and parts[0] == EXT_PKG:
                    # We go over _extension_points *in order* to make sure we get more
                    # specific paths first

                    # To give useful errors in case multiple TL packages in one package
                    dist_full_name = "%s[%s]" % (dist.metadata["Name"], parts[1])
                    for idx, ext_list in enumerate(list_ext_points):
                        if (
                            len(parts) > len(ext_list) + 2
                            and parts[2 : 2 + len(ext_list)] == ext_list
                        ):
                            # Check if this is an "init" file
                            config_module = None

                            if len(parts) == len(ext_list) + 3 and (
                                EXT_CONFIG_REGEXP.match(parts[-1]) is not None
                                or parts[-1] == "__init__.py"
                            ):
                                parts[-1] = parts[-1][:-3]  # Remove the .py
                                config_module = ".".join(parts)

                                config_to_pkg[config_module].append(dist_full_name)
                            cur_pkg = (
                                extension_points_to_pkg[_extension_points[idx]]
                                .setdefault(dist.metadata["Name"], {})
                                .get(parts[1])
                            )
                            if cur_pkg is not None:
                                if (
                                    config_module is not None
                                    and cur_pkg.config_module is not None
                                ):
                                    raise RuntimeError(
                                        "Package '%s' defines more than one "
                                        "configuration file for '%s': '%s' and '%s'"
                                        % (
                                            dist_full_name,
                                            _extension_points[idx],
                                            config_module,
                                            cur_pkg.config_module,
                                        )
                                    )
                                if config_module is not None:
                                    _ext_debug(
                                        "\tTL '%s' found config file '%s'"
                                        % (parts[1], config_module)
                                    )
                                    extension_points_to_pkg[_extension_points[idx]][
                                        dist.metadata["Name"]
                                    ][parts[1]] = MFExtPackage(
                                        package_name=dist.metadata["Name"],
                                        tl_package=parts[1],
                                        config_module=config_module,
                                    )
                            else:
                                _ext_debug(
                                    "\tTL '%s' extends '%s' with config '%s'"
                                    % (parts[1], _extension_points[idx], config_module)
                                )
                                extension_points_to_pkg[_extension_points[idx]][
                                    dist.metadata["Name"]
                                ][parts[1]] = MFExtPackage(
                                    package_name=dist.metadata["Name"],
                                    tl_package=parts[1],
                                    config_module=config_module,
                                )
                            break
            mf_ext_packages[dist.metadata["Name"]] = {
                "root_path": None,  # We keep it as None and fill it in when used; this
                # is to ensure we only package things used at least once
                "meta_module": meta_module,
                "files": files_to_include,
            }
    # At this point, we have all the packages that contribute to EXT_PKG,
    # we now check to see if there is an order to respect based on dependencies. We will
    # return an ordered list that respects that order and is ordered alphabetically in
    # case of ties. We do not do any checks because we rely on pip to have done those.
    pkg_to_reqs_count = {}
    req_to_dep = {}
    for pkg_name in mf_ext_packages:
        req_count = 0
        req_pkgs = [x.split()[0] for x in metadata.requires(pkg_name) or []]
        for req_pkg in req_pkgs:
            if req_pkg in mf_ext_packages:
                req_count += 1
                req_to_dep.setdefault(req_pkg, []).append(pkg_name)
        pkg_to_reqs_count[pkg_name] = req_count

    # Find roots
    mf_pkg_list = []
    to_process = []
    for pkg_name, count in pkg_to_reqs_count.items():
        if count == 0:
            to_process.append(pkg_name)

    # Add them in alphabetical order
    to_process.sort()
    mf_pkg_list.extend(to_process)
    # Find rest topologically
    while to_process:
        next_round = []
        for pkg_name in to_process:
            del pkg_to_reqs_count[pkg_name]
            for dep in req_to_dep.get(pkg_name, []):
                cur_req_count = pkg_to_reqs_count[dep]
                if cur_req_count == 1:
                    next_round.append(dep)
                else:
                    pkg_to_reqs_count[dep] = cur_req_count - 1
        # Add those in alphabetical order
        next_round.sort()
        mf_pkg_list.extend(next_round)
        to_process = next_round

    # Check that we got them all
    if len(pkg_to_reqs_count) > 0:
        raise RuntimeError(
            "Unresolved dependencies in '%s': %s"
            % (EXT_PKG, ", and ".join("'%s'" % p for p in pkg_to_reqs_count))
        )

    _ext_debug("'%s' distributions order is %s" % (EXT_PKG, str(mf_pkg_list)))

    # We check if we have any additional packages that were not yet installed that
    # we need to use. We always put them *last*.
    all_paths_list = list(all_paths)
    all_paths_list.sort()

    package_name_to_path = dict()
    if len(all_paths_list) > 0:
        _ext_debug("Non installed packages present at %s" % str(all_paths))
        for package_count, package_path in enumerate(all_paths_list):
            # We give an alternate name for the visible package name. It is
            # not exposed to the end user but used to refer to the package and it
            # doesn't provide much additional information to have the full path
            # particularly when it is on a remote machine.
            # We keep a temporary mapping around for error messages while loading for
            # the first time.
            package_name = "_pythonpath_%d" % package_count
            _ext_debug(
                "Walking path %s (package name %s)" % (package_path, package_name)
            )
            package_name_to_path[package_name] = package_path
            base_depth = len(package_path.split("/"))
            files_to_include = []
            meta_module = None
            for root, dirs, files in os.walk(package_path):
                parts = root.split("/")
                cur_depth = len(parts)
                # relative_root is metaflow_extensions/...
                relative_root = "/".join(parts[base_depth - 1 :])
                relative_module = ".".join(parts[base_depth - 1 :])
                files_to_include.extend(
                    [
                        "/".join([relative_root, f])
                        for f in files
                        if not any(
                            [f.endswith(suffix) for suffix in EXT_EXCLUDE_SUFFIXES]
                        )
                    ]
                )
                if cur_depth == base_depth:
                    if "__init__.py" in files:
                        raise RuntimeError(
                            "'%s' at '%s' is not an implicit namespace package as required"
                            % (EXT_PKG, root)
                        )
                    for d in dirs:
                        if d in init_ext_points:
                            raise RuntimeError(
                                "Package at '%s' should conform to' %s.X.%s' and not "
                                "'%s.%s' where X is your organization's name for example"
                                % (root, EXT_PKG, d, EXT_PKG, d)
                            )
                    # Check for meta files for this package
                    meta_files = [
                        x for x in map(EXT_META_REGEXP.match, files) if x is not None
                    ]
                    if meta_files:
                        # We should have one meta file at most
                        if len(meta_files) > 1:
                            raise RuntimeError(
                                "Package at '%s' defines more than one meta file: %s"
                                % (
                                    package_path,
                                    ", and ".join(
                                        ["'%s'" % x.group(0) for x in meta_files]
                                    ),
                                )
                            )
                        else:
                            meta_module = ".".join(
                                [relative_module, meta_files[0].group(0)[:-3]]
                            )

                elif cur_depth > base_depth + 1:
                    # We want at least a TL name and something under
                    tl_name = parts[base_depth]
                    tl_fullname = "%s[%s]" % (package_path, tl_name)
                    prefix_match = parts[base_depth + 1 :]
                    for idx, ext_list in enumerate(list_ext_points):
                        if prefix_match == ext_list:
                            # Check for any "init" files
                            init_files = [
                                x
                                for x in map(EXT_CONFIG_REGEXP.match, files)
                                if x is not None
                            ]
                            if "__init__.py" in files:
                                init_files.append("__init__.py")

                            config_module = None
                            if len(init_files) > 1:
                                raise RuntimeError(
                                    "Package at '%s' defines more than one configuration "
                                    "file for '%s': %s"
                                    % (
                                        tl_fullname,
                                        ".".join(prefix_match),
                                        ", and ".join(
                                            ["'%s'" % x.group(0) for x in init_files]
                                        ),
                                    )
                                )
                            elif len(init_files) == 1:
                                config_module = ".".join(
                                    [relative_module, init_files[0].group(0)[:-3]]
                                )
                                config_to_pkg[config_module].append(tl_fullname)

                            d = extension_points_to_pkg[_extension_points[idx]][
                                package_name
                            ] = dict()
                            d[tl_name] = MFExtPackage(
                                package_name=package_name,
                                tl_package=tl_name,
                                config_module=config_module,
                            )
                            _ext_debug(
                                "\tExtends '%s' with config '%s'"
                                % (_extension_points[idx], config_module)
                            )
            mf_pkg_list.append(package_name)
            mf_ext_packages[package_name] = {
                "root_path": None,
                "meta_module": meta_module,
                "files": files_to_include,
            }

    # Sanity check that we only have one package per configuration file
    errors = []
    for m, packages in config_to_pkg.items():
        if len(packages) > 1:
            errors.append(
                "\tPackages %s define the same configuration module '%s'"
                % (", and ".join(["'%s'" % p for p in packages]), m)
            )
    for m, packages in meta_to_pkg.items():
        if len(packages) > 1:
            errors.append(
                "\tPackages %s define the same meta module '%s'"
                % (", and ".join(["'%s'" % p for p in packages]), m)
            )
    if errors:
        raise RuntimeError(
            "Conflicts in '%s' files:\n%s" % (EXT_PKG, "\n".join(errors))
        )

    extension_points_to_pkg.default_factory = None
    # Figure out the per extension point order
    for k, v in extension_points_to_pkg.items():
        # v is a dict distributionName/packagePath -> (dict tl_name -> MFPackage)
        l = [v[pkg].values() for pkg in mf_pkg_list if pkg in v]
        # In the case of the plugins.cards extension, we allow those packages
        # to be ns packages so we only list the package once (in its first position).
        # In all other cases, we error out if we don't have a configuration file for the
        # package (either a __init__.py of an explicit mfextinit_*.py)
        final_list = []
        have_null_config = False
        for pkg in chain(*l):
            if pkg.config_module is None:
                if k == "plugins.cards":
                    # This is allowed here but we only keep one
                    if have_null_config:
                        continue
                    have_null_config = True
                else:
                    package_path = package_name_to_path.get(pkg.package_name)
                    if package_path:
                        package_path = "at '%s'" % package_path
                    else:
                        package_path = "'%s'" % pkg.package_name
                    raise RuntimeError(
                        "Package %s does not define a configuration file for '%s'"
                        % (package_path, k)
                    )
            final_list.append(pkg)

        extension_points_to_pkg[k] = final_list
    return mf_ext_packages, extension_points_to_pkg


_all_packages, _pkgs_per_extension_point = _get_extension_packages()


def _attempt_load_module(module_name):
    try:
        extension_module = importlib.import_module(module_name)
    except ImportError as e:
        if _py_ver >= 306:
            # e.name is set to the name of the package that fails to load
            # so don't error ONLY IF the error is importing this module (but do
            # error if there is a transitive import error)
            errored_names = [EXT_PKG]
            parts = module_name.split(".")
            for p in parts[1:]:
                errored_names.append("%s.%s" % (errored_names[-1], p))
            if not (isinstance(e, ModuleNotFoundError) and e.name in errored_names):
                print(
                    "The following exception ocurred while trying to load '%s' ('%s')"
                    % (EXT_PKG, module_name)
                )
                raise
            else:
                _ext_debug("\t\tUnknown error when loading '%s': %s" % (module_name, e))
                return None
    else:
        return extension_module


def _get_extension_config(distribution_name, tl_pkg, extension_point, config_module):
    if config_module is not None and not config_module.endswith("__init__"):
        module_name = config_module
    else:
        module_name = ".".join([EXT_PKG, tl_pkg, extension_point])

    _ext_debug("\t\tAttempting to load '%s'" % module_name)

    extension_module = _attempt_load_module(module_name)

    if extension_module:
        # We update the path to this module; this will be helpful if/when we need
        # to package modules so we have the base path for them.
        # Note that we cannot always get this initially because, in the case of Conda
        # and/or remote execution, the path of the files will change (Conda does symlinks
        # and creates intermediate directories and remote puts everything in one place)
        if _all_packages[distribution_name]["root_path"] is None:
            root_path = "/".join(
                extension_module.__file__.split("/")[: -len(module_name.split("."))]
            )

            _ext_debug(
                "Package '%s' is rooted at '%s'" % (distribution_name, root_path)
            )
            _all_packages[distribution_name]["root_path"] = root_path

        return MFExtModule(tl_package=tl_pkg, module=extension_module)
    return None


def _filter_files_package(package_name):
    pkg = _all_packages.get(package_name)
    if pkg and pkg["root_path"] and pkg["meta_module"]:
        meta_module = _attempt_load_module(pkg["meta_module"])
        if meta_module:
            include_suffixes = meta_module.__dict__.get("include_suffixes")
            exclude_suffixes = meta_module.__dict__.get("exclude_suffixes")

            # Behavior is as follows:
            #  - if nothing specified, include all files (so do nothing here)
            #  - if include_suffixes, only include those suffixes
            #  - if *not* include_suffixes but exclude_suffixes, include everthing *except*
            #    files ending with that suffix
            if include_suffixes:
                new_files = [
                    f
                    for f in pkg["files"]
                    if any([f.endswith(suffix) for suffix in include_suffixes])
                ]
            elif exclude_suffixes:
                new_files = [
                    f
                    for f in pkg["files"]
                    if not any([f.endswith(suffix) for suffix in exclude_suffixes])
                ]
            else:
                new_files = pkg["files"]
            pkg["files"] = new_files


def _filter_files_all():
    for p in _all_packages:
        _filter_files_package(p)


class _AliasLoader(Loader):
    def __init__(self, alias, orig):
        self._alias = alias
        self._orig = orig

    def create_module(self, spec):
        _ext_debug(
            "Loading aliased module '%s' at '%s' " % (str(self._orig), spec.name)
        )
        if isinstance(self._orig, str):
            try:
                return importlib.import_module(self._orig)
            except ImportError:
                raise ImportError(
                    "No module found '%s' (aliasing '%s')" % (spec.name, self._orig)
                )
        elif isinstance(self._orig, types.ModuleType):
            # We are aliasing a module so we just return that one
            return self._orig
        else:
            return super().create_module(spec)

    def exec_module(self, module):
        # Override the name to make it a bit nicer. We keep the old name so that
        # we can refer to it when we load submodules
        if not hasattr(module, "__orig_name__"):
            module.__orig_name__ = module.__name__
            module.__name__ = self._alias


class _OrigLoader(Loader):
    def __init__(self, fullname, orig_loader, previously_loaded_module=None):
        self._fullname = fullname
        self._orig_loader = orig_loader
        self._previously_loaded_module = previously_loaded_module

    def create_module(self, spec):
        _ext_debug(
            "Loading original module '%s' (will be loaded at '%s')"
            % (spec.name, self._fullname)
        )
        self._orig_name = spec.name
        return self._orig_loader.create_module(spec)

    def exec_module(self, module):
        try:
            # Perform all actions of the original loader
            self._orig_loader.exec_module(module)
        except BaseException:
            raise  # We re-raise it always; the finally clause will still restore things
        else:
            # It loaded, we move and rename appropriately
            module.__spec__.name = self._fullname
            module.__orig_name__ = module.__name__
            module.__name__ = self._fullname
            sys.modules[self._fullname] = module
            del sys.modules[self._orig_name]

        finally:
            # At this point, the original module is loaded with the original name. We
            # want to replace it with previously_loaded_module if it exists and, in both
            # cases, change the name to fullname (which has _orig in it)
            if self._previously_loaded_module:
                sys.modules[self._orig_name] = self._previously_loaded_module


class _LazyFinder(MetaPathFinder):
    # This _LazyFinder implements the Importer Protocol defined in PEP 302

    def __init__(self, handled):
        # Dictionary:
        # Key: name of the module to handle
        # Value:
        #   - A string: a pathspec to the module to load
        #   - A module: the module to load
        self._handled = handled if handled else {}

        # This is used to revert back to regular loading when trying to load
        # the over-ridden module
        self._temp_excluded_prefix = set()

    def find_spec(self, fullname, path, target=None):
        # If we are trying to load a shadowed module (ending in ._orig), we don't
        # say we handle it
        if any([fullname.startswith(e) for e in self._temp_excluded_prefix]):
            return None

        # If this is something we directly handle, return our loader
        if fullname in self._handled:
            return importlib.util.spec_from_loader(
                fullname, _AliasLoader(fullname, self._handled[fullname])
            )

        # For the first pass when we try to load a shadowed module, we send it back
        # without the ._orig and that will find the original spec of the module
        # Note that we handle mymodule._orig.orig_submodule as well as mymodule._orig.
        # Basically, the original module and any of the original submodules are
        # available under _orig.
        name_parts = fullname.split(".")
        try:
            orig_idx = name_parts.index("_orig")
        except ValueError:
            orig_idx = -1
        if orig_idx > -1 and ".".join(name_parts[:orig_idx]) in self._handled:
            orig_name = ".".join(name_parts[:orig_idx] + name_parts[orig_idx + 1 :])
            _ext_debug("Looking for original module '%s'" % orig_name)
            prefix = ".".join(name_parts[:orig_idx])
            self._temp_excluded_prefix.add(prefix)
            # We also have to remove the module temporarily while we look for the
            # new spec since otherwise it returns the spec of that loaded module.
            # module is also restored *after* we call `create_module` in the loader
            # otherwise it just returns None
            loaded_module = sys.modules.get(orig_name)
            if loaded_module:
                del sys.modules[orig_name]

            # This finds the spec that would have existed had we not added all our
            # _LazyFinders
            spec = importlib.util.find_spec(orig_name)

            self._temp_excluded_prefix.remove(prefix)

            if not spec:
                return None

            _ext_debug("Found original spec %s" % spec)

            # Change the spec
            spec.loader = _OrigLoader(fullname, spec.loader, loaded_module)

            return spec

        if len(name_parts) > 1:
            # This checks for submodules of things we handle. We check for the most
            # specific submodule match and use that
            chop_idx = 1
            while chop_idx < len(name_parts):
                parent_name = ".".join(name_parts[:-chop_idx])
                if parent_name in self._handled:
                    orig = self._handled[parent_name]
                    if isinstance(orig, types.ModuleType):
                        orig_name = ".".join(
                            [orig.__orig_name__] + name_parts[-chop_idx:]
                        )
                    else:
                        orig_name = ".".join([orig] + name_parts[-chop_idx:])
                    return importlib.util.spec_from_loader(
                        fullname, _AliasLoader(fullname, orig_name)
                    )
                chop_idx += 1
        return None
