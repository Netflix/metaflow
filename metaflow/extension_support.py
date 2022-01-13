import importlib
import json
import os
import re
import sys
import types

from collections import defaultdict, namedtuple

__all__ = (
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
EXT_CONFIG_REGEXP = re.compile(r"^mfextinit_[a-zA-Z0-9]+\.py$")

METAFLOW_DEBUG_EXT_MECHANISM = os.environ.get("METAFLOW_DEBUG_EXT", False)


MFExtPackage = namedtuple("MFExtPackage", "package_name tl_package config_module")
MFExtModule = namedtuple("MFExtModule", "tl_package module")


def get_modules(extension_point):
    modules_to_load = []
    if not _mfext_supported:
        _ext_debug(
            "Metaflow extensions not supported -- make sure you are using Python 3.5+"
        )
        return []
    if extension_point not in _extension_points:
        raise RuntimeError(
            "Metaflow extension point '%s' not supported" % extension_point
        )
    _ext_debug("Getting modules for extension point '%s'..." % extension_point)
    for pkg in _pkgs_per_extension_point[extension_point]:
        _ext_debug("\tFound TL '%s' from '%s'" % (pkg.tl_package, pkg.package_name))
        m = _get_extension_config(pkg.tl_package, extension_point, pkg.config_module)
        if m:
            modules_to_load.append(m)
    _ext_debug("\tLoaded %s" % str(modules_to_load))
    return modules_to_load


def dump_module_info():
    return "ext_info", [_all_packages, _pkgs_per_extension_point]


def load_globals(module, dst_globals):
    for n, o in module.__dict__.items():
        if not n.startswith("__") and not isinstance(o, types.ModuleType):
            dst_globals[n] = o


def alias_submodules(module, tl_package, extension_point):
    lazy_load_custom_modules = {}
    addl_modules = module.__dict__.get("__mf_promote_submodules__")
    if addl_modules:
        # We make an alias for these modules which the extension author wants to
        # expose but since it may not already be loaded, we don't load it either

        # TODO: This does not properly work for multiple packages that overwrite
        # their submodule for example if metaflow_extensions.X.datatools.Y is provided
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
            lazy_load_custom_modules.update(
                {
                    "metaflow.%s" % k: "%s.%s.%s" % (EXT_PKG, tl_package, k)
                    for k in addl_modules
                }
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
        "\tWill create the following module aliases: %s" % str(lazy_load_custom_modules)
    )
    return lazy_load_custom_modules


def lazy_load_aliases(aliases):
    if aliases:
        sys.meta_path = [_LazyLoader(aliases)] + sys.meta_path


def multiload_globals(modules, dst_globals):
    for m in modules:
        load_globals(m.module, dst_globals)


def multiload_all(modules, extension_point, dst_globals):
    lazy_load_custom_modules = {}
    for m in modules:
        lazy_load_custom_modules.update(
            alias_submodules(m.module, m.tl_package, extension_point)
        )
        load_globals(m.module, dst_globals)
    lazy_load_aliases(lazy_load_custom_modules)


_py_ver = sys.version_info[0] * 10 + sys.version_info[1]
_mfext_supported = False

if _py_ver >= 34:
    import importlib.util
    from importlib.machinery import ModuleSpec

    if _py_ver >= 38:
        from importlib import metadata
    else:
        import importlib_metadata as metadata
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
        print(*args, **kwargs)


def _get_extension_packages():
    if not _mfext_supported:
        return []

    # If we have an INFO file with the appropriate information (if running from a saved
    # code package for example), we use that directly
    # Pre-compute on _extension_points
    from metaflow import INFO_FILE

    try:
        with open(INFO_FILE, "r") as contents:
            all_pkg, ext_to_pkg = json.load(contents).get("ext_info", (None, None))
            if all_pkg is not None and ext_to_pkg is not None:
                # We need to properly convert stuff in ext_to_pkg
                for k, v in ext_to_pkg.items():
                    v = [MFExtPackage(*d) for d in v]
                    ext_to_pkg[k] = v
                return all_pkg, ext_to_pkg
    except IOError:
        pass

    _list_ext_points = [x.split(".") for x in _extension_points]
    # TODO: This relies only on requirements to determine import order; we may want
    # other ways of specifying "load me after this if it exists" without depending on
    # the package. One way would be to rely on the description and have that info there.
    # Not sure of the use though so maybe we can skip for now.
    mf_ext_packages = []
    extension_points_to_pkg = defaultdict(dict)
    for dist in metadata.distributions():
        if any(
            [pkg == EXT_PKG for pkg in (dist.read_text("top_level.txt") or "").split()]
        ):
            _ext_debug("Found extension package '%s'..." % dist.metadata["Name"])
            mf_ext_packages.append(dist.metadata["Name"])
            # At this point, we check to see what extension points this package
            # contributes to. This is to enable multiple namespace packages to contribute
            # to the same extension point (for example, you may have multiple packages
            # that have plugins)
            for f in dist.files:
                parts = list(f.parts)
                if len(parts) > 3 and parts[0] == EXT_PKG:
                    # We go over _extension_points *in order* to make sure we get more
                    # specific paths first
                    for idx, ext_list in enumerate(_list_ext_points):
                        if (
                            len(parts) > len(ext_list) + 2
                            and parts[2 : 2 + len(ext_list)] == ext_list
                        ):
                            # Check if this is an "init" file
                            config_module = None
                            if (
                                len(parts) == len(ext_list) + 3
                                and EXT_CONFIG_REGEXP.match(parts[-1]) is not None
                            ):
                                parts[-1] = parts[-1][:-3]  # Remove the .py
                                config_module = ".".join(parts)

                            cur_pkg = extension_points_to_pkg[
                                _extension_points[idx]
                            ].get(dist.metadata["Name"], None)
                            if cur_pkg is not None:
                                if (
                                    config_module is not None
                                    and cur_pkg.config_module is not None
                                ):
                                    raise RuntimeError(
                                        "Package '%s' defines more than one "
                                        "configuration file for '%s': '%s' and '%s'"
                                        % (
                                            dist.metadata["Name"],
                                            _extension_points[idx],
                                            config_module,
                                            cur_pkg.config_module,
                                        )
                                    )
                                if config_module is not None:
                                    _ext_debug(
                                        "\tFound config file '%s'" % config_module
                                    )
                                    extension_points_to_pkg[_extension_points[idx]][
                                        dist.metadata["Name"]
                                    ] = MFExtPackage(
                                        package_name=dist.metadata["Name"],
                                        tl_package=parts[1],
                                        config_module=config_module,
                                    )
                            else:
                                _ext_debug(
                                    "\tExtends '%s' with config '%s'"
                                    % (_extension_points[idx], config_module)
                                )
                                extension_points_to_pkg[_extension_points[idx]][
                                    dist.metadata["Name"]
                                ] = MFExtPackage(
                                    package_name=dist.metadata["Name"],
                                    tl_package=parts[1],
                                    config_module=config_module,
                                )
                            break
    extension_points_to_pkg.default_factory = list

    # At this point, we have all the packages that contribute to EXT_PKG,
    # we now check to see if there is an order to respect based on dependencies. We will
    # return an ordered list that respects that order and is ordered alphabetically in
    # case of ties.
    pkg_to_reqs_count = {}
    req_to_dep = {}
    mf_ext_packages_set = set(mf_ext_packages)
    for pkg_name in mf_ext_packages:
        req_count = 0
        req_pkgs = [x.split()[0] for x in metadata.requires(pkg_name)]
        for req_pkg in req_pkgs:
            if req_pkg in mf_ext_packages_set:
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

    # Figure out the per extension point order
    for k, v in extension_points_to_pkg.items():
        l = [v[pkg] for pkg in mf_pkg_list if pkg in v]
        extension_points_to_pkg[k] = l
    return mf_pkg_list, extension_points_to_pkg


_all_packages, _pkgs_per_extension_point = _get_extension_packages()


def _get_extension_config(tl_pkg, extension_point, config_module):
    module_name = ".".join([EXT_PKG, tl_pkg, extension_point])
    try:
        if config_module is not None:
            _ext_debug("\tAttempting to load '%s'" % config_module)
            extension_module = importlib.import_module(config_module)
        else:
            _ext_debug("\tAttempting to load '%s'" % module_name)
            extension_module = importlib.import_module(module_name)
    except ImportError as e:
        if _py_ver >= 36:
            # e.name is set to the name of the package that fails to load
            # so don't error ONLY IF the error is importing this module (but do
            # error if there is a transitive import error)
            errored_names = [EXT_PKG, module_name]
            if not (isinstance(e, ModuleNotFoundError) and e.name in errored_names):
                print(
                    "Cannot load %s (%s) module -- "
                    "if you want to ignore, uninstall %s packages"
                    % (EXT_PKG, module_name, EXT_PKG)
                )
                raise
            _ext_debug("\tError while loading '%s': %s" % (module_name, e))
            return None
    else:
        return MFExtModule(tl_package=tl_pkg, module=extension_module)


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
