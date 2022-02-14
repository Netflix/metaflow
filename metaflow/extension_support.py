import importlib
import json
import os
import re
import sys
import types

from collections import defaultdict, namedtuple

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
        m = _get_extension_config(pkg.tl_package, extension_point, pkg.config_module)
        if m:
            modules_to_load.append(m)
    _ext_debug("\tLoaded %s" % str(modules_to_load))
    return modules_to_load


def dump_module_info():
    return "ext_info", [_all_packages, _pkgs_per_extension_point]


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
        sys.meta_path = [_LazyLoader(aliases)] + sys.meta_path


def multiload_globals(modules, dst_globals):
    for m in modules:
        load_globals(m.module, dst_globals, extra_indent=True)


def multiload_all(modules, extension_point, dst_globals):
    for m in modules:
        # Note that we load aliases separately (as opposed to ine one fell swoop) so
        # modules loaded later in `modules` can depend on them
        lazy_load_aliases(
            alias_submodules(m.module, m.tl_package, extension_point, extra_indent=True)
        )
        load_globals(m.module, dst_globals)


_py_ver = sys.version_info[0] * 10 + sys.version_info[1]
_mfext_supported = False

if _py_ver >= 34:
    import importlib.util
    from importlib.machinery import ModuleSpec

    if _py_ver >= 38:
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
        return [], {}

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
        if _py_ver >= 36:
            # e.name is set to the name of the package that fails to load
            # so don't error ONLY IF the error is importing this module (but do
            # error if there is a transitive import error)
            if not (isinstance(e, ModuleNotFoundError) and e.name == EXT_PKG):
                raise
            return [], {}

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
    mf_ext_packages = set([])
    # Key: distribution name/full path to package
    # Value:
    #  Key: TL package name
    #  Value: MFExtPackage
    extension_points_to_pkg = defaultdict(dict)
    config_to_pkg = defaultdict(list)
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
            all_paths.discard(dist.locate_file(EXT_PKG).as_posix())

            mf_ext_packages.add(dist.metadata["Name"])

            # At this point, we check to see what extension points this package
            # contributes to. This is to enable multiple namespace packages to contribute
            # to the same extension point (for example, you may have multiple packages
            # that have plugins)
            for f in dist.files:
                # Make sure EXT_PKG is a ns package
                if f.as_posix() == "%s/__init__.py" % EXT_PKG:
                    raise RuntimeError(
                        "Package '%s' providing '%s' is not an implicit namespace "
                        "package as required" % (dist.metadata["Name"], EXT_PKG)
                    )

                parts = list(f.parts)
                if (
                    len(parts) > 1
                    and parts[0] == EXT_PKG
                    and parts[1] in init_ext_points
                ):
                    # This is most likely a problem as we need an intermediate "identifier"
                    raise RuntimeError(
                        "Package '%s' should conform to %s.X.%s and not %s.%s where "
                        "X is your organization's name for example"
                        % (dist.metadata["Name"], EXT_PKG, parts[1], EXT_PKG, parts[1])
                    )

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
                                        "\tTL %s found config file '%s'"
                                        % (parts[1], config_module)
                                    )
                                    extension_points_to_pkg[_extension_points[idx]][
                                        dist.metadata["Name"]
                                    ][parts[1]] = MFExtPackage(
                                        package_name=dist_full_name,
                                        tl_package=parts[1],
                                        config_module=config_module,
                                    )
                            else:
                                _ext_debug(
                                    "\tTL %s extends '%s' with config '%s'"
                                    % (parts[1], _extension_points[idx], config_module)
                                )
                                extension_points_to_pkg[_extension_points[idx]][
                                    dist.metadata["Name"]
                                ][parts[1]] = MFExtPackage(
                                    package_name=dist_full_name,
                                    tl_package=parts[1],
                                    config_module=config_module,
                                )
                            break

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
            "Unresolved dependencies in %s: %s" % (EXT_PKG, str(pkg_to_reqs_count))
        )

    # We check if we have any additional packages that were not yet installed that
    # we need to use. We always put them *last*.
    if len(all_paths) > 0:
        _ext_debug("Non installed packages present at %s" % str(all_paths))
        packages_to_add = set()
        for package_path in all_paths:
            _ext_debug("Walking path %s" % package_path)
            base_depth = len(package_path.split("/"))
            for root, dirs, files in os.walk(package_path):
                parts = root.split("/")
                cur_depth = len(parts)
                if cur_depth == base_depth:
                    if "__init__.py" in files:
                        raise RuntimeError(
                            "%s at '%s' is not an implicit namespace package as required"
                            % (EXT_PKG, root)
                        )
                    for d in dirs:
                        if d in init_ext_points:
                            raise RuntimeError(
                                "Package at %s should conform to %s.X.%s and not %s.%s "
                                "where X is your organization's name for example"
                                % (root, EXT_PKG, d, EXT_PKG, d)
                            )
                elif cur_depth > base_depth + 1:
                    # We want at least a TL name and something under
                    tl_name = parts[base_depth]
                    tl_fullname = "/".join([package_path, tl_name])
                    prefix_match = parts[base_depth + 1 :]
                    next_dirs = None
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
                                    "Package at %s defines more than one configuration "
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
                                    parts[base_depth - 1 :]
                                    + [init_files[0].group(0)[:-3]]
                                )
                                config_to_pkg[config_module].append(tl_fullname)
                            d = extension_points_to_pkg[_extension_points[idx]][
                                tl_fullname
                            ] = dict()
                            d[tl_name] = MFExtPackage(
                                package_name=tl_fullname,
                                tl_package=tl_name,
                                config_module=config_module,
                            )
                            _ext_debug(
                                "\tExtends '%s' with config '%s'"
                                % (_extension_points[idx], config_module)
                            )
                            packages_to_add.add(tl_fullname)
                        else:
                            # Check what directories we need to go down if any
                            if len(ext_list) > 1 and prefix_match == ext_list[:-1]:
                                if next_dirs is None:
                                    next_dirs = []
                                next_dirs.append(ext_list[-1])
                    if next_dirs is not None:
                        dirs[:] = next_dirs[:]

        # Add all these new packages to the list of packages as well.
        packages_to_add = list(packages_to_add)
        packages_to_add.sort()
        mf_pkg_list.extend(packages_to_add)

    # Sanity check that we only have one package per configuration file
    errors = []
    for m, packages in config_to_pkg.items():
        if len(packages) > 1:
            errors.append(
                "\tPackages %s define the same configuration module '%s'"
                % (", and ".join(packages), m)
            )
    if errors:
        raise RuntimeError(
            "Conflicts in %s configuration files:\n%s" % (EXT_PKG, "\n".join(errors))
        )

    extension_points_to_pkg.default_factory = None
    # Figure out the per extension point order
    for k, v in extension_points_to_pkg.items():
        l = [v[pkg].values() for pkg in mf_pkg_list if pkg in v]
        # In the case of the plugins.cards extension, we allow those packages
        # to be ns packages so we only list the package once (in its last position).
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
                    raise RuntimeError(
                        "Package '%s' does not define a configuration file for '%s'"
                        % (pkg.package_name, k)
                    )
                final_list.append(pkg)

        extension_points_to_pkg[k] = final_list
    return mf_pkg_list, extension_points_to_pkg


_all_packages, _pkgs_per_extension_point = _get_extension_packages()


def _attempt_load_module(module_name):
    try:
        extension_module = importlib.import_module(module_name)
    except ImportError as e:
        if _py_ver >= 36:
            # e.name is set to the name of the package that fails to load
            # so don't error ONLY IF the error is importing this module (but do
            # error if there is a transitive import error)
            errored_names = [EXT_PKG]
            parts = module_name.split(".")
            for p in parts[1:]:
                errored_names.append("%s.%s" % (errored_names[-1], p))
            if not (isinstance(e, ModuleNotFoundError) and e.name in errored_names):
                print(
                    "The following exception ocurred while trying to load %s ('%s')"
                    % (EXT_PKG, module_name)
                )
                raise
            else:
                _ext_debug("\t\tUnknown error when loading '%s': %s" % (module_name, e))
                return None
    else:
        return extension_module


def _get_extension_config(tl_pkg, extension_point, config_module):
    module_name = ".".join([EXT_PKG, tl_pkg, extension_point])
    if config_module is not None and not config_module.endswith("__init__"):
        _ext_debug("\t\tAttempting to load '%s'" % config_module)
        extension_module = _attempt_load_module(config_module)
    else:
        _ext_debug("\t\tAttempting to load '%s'" % module_name)
        extension_module = _attempt_load_module(module_name)
    if extension_module:
        return MFExtModule(tl_package=tl_pkg, module=extension_module)
    return None


class _LazyLoader(object):
    # This _LazyLoader implements the Importer Protocol defined in PEP 302
    # TODO: Need to move to find_spec, exec_module and create_module as
    # find_module and load_module are deprecated

    def __init__(self, handled):
        # Modules directly loaded (this is either new modules or overrides of existing ones)
        self._handled = handled if handled else {}

        # This is used to revert back to regular loading when trying to load
        # the over-ridden module
        self._tempexcluded = set()

        # This is used when loading a module alias to load any submodule
        self._alias_to_orig = {}

    def find_module(self, fullname, path=None):
        if fullname in self._tempexcluded:
            return None
        if fullname in self._handled or (
            fullname.endswith("._orig") and fullname[:-6] in self._handled
        ):
            return self
        name_parts = fullname.split(".")
        if len(name_parts) > 1 and name_parts[-1] != "_orig":
            # We check if we had an alias created for this module and if so,
            # we are going to load it to properly fully create aliases all
            # the way down.
            parent_name = ".".join(name_parts[:-1])
            if parent_name in self._alias_to_orig:
                return self
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        if not self._can_handle_orig_module() and fullname.endswith("._orig"):
            # We return a nicer error message
            raise ImportError(
                "Attempting to load '%s' -- loading shadowed modules in Metaflow "
                "Extensions are only supported in Python 3.4+" % fullname
            )
        to_import = self._handled.get(fullname, None)

        # If to_import is None, two cases:
        #  - we are loading a ._orig module
        #  - OR we are loading a submodule
        if to_import is None:
            if fullname.endswith("._orig"):
                try:
                    # We exclude this module temporarily from what we handle to
                    # revert back to the non-shadowing mode of import
                    self._tempexcluded.add(fullname)
                    to_import = importlib.util.find_spec(fullname)
                finally:
                    self._tempexcluded.remove(fullname)
            else:
                name_parts = fullname.split(".")
                submodule = name_parts[-1]
                parent_name = ".".join(name_parts[:-1])
                to_import = ".".join([self._alias_to_orig[parent_name], submodule])

        if isinstance(to_import, str):
            try:
                to_import_mod = importlib.import_module(to_import)
            except ImportError:
                raise ImportError(
                    "No module found '%s' (aliasing %s)" % (fullname, to_import)
                )
            sys.modules[fullname] = to_import_mod
            self._alias_to_orig[fullname] = to_import_mod.__name__
        elif isinstance(to_import, types.ModuleType):
            sys.modules[fullname] = to_import
            self._alias_to_orig[fullname] = to_import.__name__
        elif self._can_handle_orig_module() and isinstance(to_import, ModuleSpec):
            # This loads modules that end in _orig
            m = importlib.util.module_from_spec(to_import)
            to_import.loader.exec_module(m)
            sys.modules[fullname] = m
        elif to_import is None and fullname.endswith("._orig"):
            # This happens when trying to access a shadowed ._orig module
            # when actually, there is no shadowed module; print a nicer message
            # Condition is a bit overkill and most likely only checking to_import
            # would be OK. Being extra sure in case _LazyLoader is misused and
            # a None value is passed in.
            raise ImportError(
                "Metaflow Extensions shadowed module '%s' does not exist" % fullname
            )
        else:
            raise ImportError
        return sys.modules[fullname]

    @staticmethod
    def _can_handle_orig_module():
        return sys.version_info[0] >= 3 and sys.version_info[1] >= 4
