import importlib
import traceback

from metaflow.metaflow_config_funcs import from_conf

from . import _ext_debug, alias_submodules, get_modules, lazy_load_aliases


def process_plugins(module_globals):
    _resolve_relative_paths(module_globals)
    # Set ENABLED_ and _TOGGLE_ variables. The ENABLED_* variables are read from
    # configuration and the _TOGGLE_* variables are initialized to empty lists to be
    # appended to from the extensions.
    for plugin_category in _plugin_categories:
        upper_category = plugin_category.upper()
        globals()["ENABLED_%s" % upper_category] = from_conf(
            "ENABLED_%s" % upper_category
        )
        globals()["_TOGGLE_%s" % upper_category] = []

        # Initialize the list of available plugins to what is available in Metaflow core
        globals()[_list_for_category(plugin_category)] = _get_ext_plugins(
            module_globals, plugin_category
        )

    try:
        modules_to_import = get_modules("plugins")
        # This is like multiload_all but we load globals independently since we just care
        # about the TOGGLE and ENABLED values
        for m in modules_to_import:
            lazy_load_aliases(
                alias_submodules(m.module, m.tl_package, "plugins", extra_indent=True)
            )
            for n, o in m.module.__dict__.items():
                if n.startswith("TOGGLE_") and n[7:].lower() in _plugin_categories:
                    # Extensions append to the TOGGLE list
                    globals()["_TOGGLE_%s" % n[7:]].extend(o)
                elif n.startswith("ENABLED_") and n[8:].lower() in _plugin_categories:
                    # Extensions override the ENABLED_ setting.
                    globals()[n] = o

            _resolve_relative_paths(m.module.__dict__)
            for plugin_category in _plugin_categories:
                # Collect all the plugins present
                globals()[_list_for_category(plugin_category)].extend(
                    _get_ext_plugins(m.module.__dict__, plugin_category)
                )
    except Exception as e:
        _ext_debug("\tWARNING: ignoring all plugins due to error during import: %s" % e)
        print(
            "WARNING: Plugins did not load -- ignoring all of them which may not "
            "be what you want: %s" % e
        )
        traceback.print_exc()

    # At this point, we have _all_<category>s populated with all the tuples
    # (name, module_class) from all the plugins in all the extensions (if any)
    # We build a dictionary taking the latest presence for each name (so plugins
    # override metaflow core)
    for plugin_category in _plugin_categories:
        upper_category = plugin_category.upper()
        d = globals()[_dict_for_category(plugin_category)] = {}
        for name, class_path in globals()["_all_%ss" % plugin_category]:
            _ext_debug(
                "    Adding %s '%s' from '%s'" % (plugin_category, name, class_path)
            )
            d[name] = class_path

        # Resolve all the ENABLED_* variables. The rules are the following:
        #  - if ENABLED_* is non None, it means it was either set directly by the user
        #    in a configuration file, on the command line or by an extension. In that case
        #    we honor those wishes and completely ignore the extensions' toggles.
        #  - if ENABLED_* is None, we populate it with everything included here and in
        #    all the extensions and use the TOGGLE_ list to produce the final list.
        # The rationale behind this is to support both a configuration option where the
        # plugins enabled are explicitly listed (typical in a lot of software) but also to
        # support a "configuration-less" version where the installation of the extensions
        # determines what is activated.
        if globals()["ENABLED_%s" % upper_category] is None:
            globals()["ENABLED_%s" % upper_category] = (
                list(d) + globals()["_TOGGLE_%s" % upper_category]
            )


def merge_lists(base, overrides, attr):
    # Merge two lists of classes by comparing them for equality using 'attr'.
    # This function prefers anything in 'overrides'. In other words, if a class
    # is present in overrides and matches (according to the equality criterion) a class in
    # base, it will be used instead of the one in base.
    l = list(overrides)
    existing = set([getattr(o, attr) for o in overrides])
    l.extend([d for d in base if getattr(d, attr) not in existing])
    base[:] = l[:]


def resolve_plugins(category):
    # Called to return a list of classes that are the available plugins for 'category'

    # The ENABLED_<category> variable is set in process_plugins
    # based on all the plugins that are found; it can contain either names of
    # plugins or -/+<name_of_plugin> indicating a "toggle" to activate/de-activate
    # a plugin.
    list_of_plugins = globals()["ENABLED_%s" % category.upper()]
    _ext_debug("    Resolving %s plugins" % category)
    _ext_debug("        Raw list of plugins is: %s" % str(list_of_plugins))
    set_of_plugins = set()
    for p in list_of_plugins:
        if p.startswith("-"):
            set_of_plugins.discard(p[1:])
        elif p.startswith("+"):
            set_of_plugins.add(p[1:])
        else:
            set_of_plugins.add(p)

    available_plugins = globals()[_dict_for_category(category)]
    name_extractor = _plugin_categories[category]
    if not name_extractor:
        # If we have no name function, it means we just use the name in the dictionary
        # and we return a dictionary. This is for sidecars mostly as they do not have
        # a field that indicates their name
        to_return = {}
    else:
        to_return = []
    _ext_debug("        Resolved list of plugins is: %s" % str(set_of_plugins))
    # Various error checks to make sure the plugin exists -- basically converts a string
    # representing a class path to the actual class. We try to give useful messages
    # in case of errors.
    for name in set_of_plugins:
        class_path = available_plugins.get(name, None)
        if class_path is None:
            raise ValueError(
                "Configuration requested %s plugin '%s' but no such plugin is available"
                % (category, name)
            )
        path, cls_name = class_path.rsplit(".", 1)
        try:
            plugin_module = importlib.import_module(path)
        except ImportError:
            raise ValueError(
                "Cannot locate %s plugin '%s' at '%s'" % (category, name, path)
            )
        cls = getattr(plugin_module, cls_name, None)
        if cls is None:
            raise ValueError(
                "Cannot locate '%s' class for %s plugin at '%s'"
                % (cls_name, category, path)
            )
        if name_extractor and name_extractor(cls) != name:
            raise ValueError(
                "Class '%s' at '%s' for %s plugin expected to be named '%s' but got '%s'"
                % (cls_name, path, category, name, name_extractor(cls))
            )
        globals()[cls_name] = cls
        if name_extractor is not None:
            to_return.append(cls)
        else:
            to_return[name] = cls
        _ext_debug(
            "        Added %s plugin '%s' from '%s'" % (category, name, class_path)
        )
    return to_return


# Some plugins do not have a field in them indicating their name.
# This is the case for sidecars.
# All other plugins contain a field that indicates their name.
# _plugin_categories contains all the types of plugins and, for ones that have
# a field indicating their name,
# an additional function indicating how to extract the name of the plugin is provided.

# key is the type of plugin
# value is either:
#  - a function to extract the name of the plugin from the plugin itself
#  - None if this is a plugin with no field for its name
_plugin_categories = {
    "step_decorator": lambda x: x.name,
    "flow_decorator": lambda x: x.name,
    "environment": lambda x: x.TYPE,
    "metadata_provider": lambda x: x.TYPE,
    "datastore": lambda x: x.TYPE,
    "secrets_provider": lambda x: x.TYPE,
    "gcp_client_provider": lambda x: x.name,
    "deployer_impl_provider": lambda x: x.TYPE,
    "azure_client_provider": lambda x: x.name,
    "sidecar": None,
    "logging_sidecar": None,
    "monitor_sidecar": None,
    "aws_client_provider": lambda x: x.name,
    "cli": lambda x: (
        list(x.commands)[0] if len(x.commands) == 1 else "too many commands"
    ),
}


def _list_for_category(category):
    # Convenience function to name the variable containing List[Tuple[str, str]] where
    # each tuple contains:
    #  - the name of the plugin
    #  - the classpath of the plugin
    return "_all_%ss" % category


def _dict_for_category(category):
    # Convenience function to name the variable containing the same thing as
    # _list_for_category except that it is now in dict form where the key is the name
    # of the plugin
    return "_all_%ss_dict" % category


def _get_ext_plugins(module_globals, category):
    # Convenience function to get the list of Tuple[str, str] describing the plugins
    # available from the extension. This defaults to [] so not all plugins need to be
    # listed.
    return module_globals.get("%sS_DESC" % category.upper(), [])


def _set_ext_plugins(module_globals, category, val):
    module_globals["%sS_DESC" % category.upper()] = val


def _resolve_relative_paths(module_globals):
    # We want to modify all the relevant lists so that the relative paths
    # are made fully qualified paths for the modules
    pkg_path = module_globals["__package__"]
    pkg_components = pkg_path.split(".")

    def resolve_path(class_path):
        # Converts a relative class_path to an absolute one considering that the
        # relative class_path is present in a package pkg_path
        if class_path[0] == ".":
            i = 1
            # Check for multiple "." at the start of the class_path
            while class_path[i] == ".":
                i += 1
            if i > len(pkg_components):
                raise ValueError(
                    "Path '%s' exits out of Metaflow module at %s"
                    % (class_path, pkg_path)
                )
            return (
                ".".join(pkg_components[: -i + 1] if i > 1 else pkg_components)
                + class_path[i - 1 :]
            )
        return class_path

    for plugin_category in _plugin_categories:
        _set_ext_plugins(
            module_globals,
            plugin_category,
            list(
                map(
                    lambda p: (p[0], resolve_path(p[1])),
                    _get_ext_plugins(module_globals, plugin_category),
                )
            ),
        )
