import importlib
import traceback

from metaflow.metaflow_config_funcs import from_conf

from . import _ext_debug, get_modules

# This file is similar in functionality to the cmd.py file. Please refer to that
# one for more information on what the functions do.


def process_integration_aliases(module_globals):
    _resolve_relative_paths(module_globals)

    all_aliases = _get_ext_aliases(module_globals)
    all_aliases_dict = {}

    toggle_alias = []
    list_of_aliases = from_conf("ENABLED_INTEGRATION_ALIAS")

    try:
        modules_to_import = get_modules("alias")
        # This is like multiload_all but we load globals independently since we just care
        # about the TOGGLE and ENABLED values
        for m in modules_to_import:
            for n, o in m.module.__dict__.items():
                if n == "TOGGLE_INTEGRATION_ALIAS":
                    toggle_alias.extend(o)
                elif n == "ENABLED_INTEGRATION_ALIAS":
                    list_of_aliases = o
            _resolve_relative_paths(m.module.__dict__)
            all_aliases.extend(_get_ext_aliases(m.module.__dict__))
    except Exception as e:
        _ext_debug(
            "\tWARNING: ignoring all integration aliases due to error during import: %s"
            % e
        )
        print(
            "WARNING: Integration aliases did not load -- ignoring all of them which "
            "may not be what you want: %s" % e
        )
        traceback.print_exc()

    # At this point, we have _all_aliases populated with all the tuples
    # (name, module_class) from all the aliases in all the extensions (if any)
    # We build a dictionary taking the latest presence for each name (so plugins
    # override metaflow core)
    for name, obj_path in all_aliases:
        _ext_debug("    Adding integration alias '%s' from '%s'" % (name, obj_path))
        all_aliases_dict[name] = obj_path

    # Resolve the ENABLED_INTEGRATION_ALIAS variable. The rules are the following:
    #  - if ENABLED_INTEGRATION_ALIAS is non None, it means it was either set directly
    #    by the user in a configuration file, on the command line or by an extension.
    #    In that case we honor those wishes and completely ignore the extensions' toggles.
    #  - if ENABLED_INTEGRATION_ALIAS is None, we populate it with everything included
    #    here and in all the extensions and use the TOGGLE_ list to produce the final list.
    # The rationale behind this is to support both a configuration option where the
    # aliases enabled are explicitly listed (typical in a lot of software) but also to
    # support a "configuration-less" version where the installation of the extensions
    # determines what is activated.
    if list_of_aliases is None:
        list_of_aliases = list(all_aliases_dict) + toggle_alias

    _ext_debug("    Resolving metaflow integration aliases")
    _ext_debug("        Raw list is: %s" % str(list_of_aliases))

    set_of_aliases = set()
    for p in list_of_aliases:
        if p.startswith("-"):
            set_of_aliases.discard(p[1:])
        elif p.startswith("+"):
            set_of_aliases.add(p[1:])
        else:
            set_of_aliases.add(p)
    _ext_debug("        Resolved list is: %s" % str(set_of_aliases))

    for name in set_of_aliases:
        obj_path = all_aliases_dict.get(name, None)
        if obj_path is None:
            raise ValueError(
                "Configuration requested integration alias '%s' but no such alias "
                "is available" % name
            )
        path, obj_name = obj_path.rsplit(".", 1)
        try:
            alias_module = importlib.import_module(path)
        except ImportError:
            raise ValueError(
                "Cannot locate integration alias '%s' at '%s'" % (name, path)
            )

        obj = getattr(alias_module, obj_name, None)
        if obj is None:
            raise ValueError(
                "Cannot locate '%s' object for integration alias at '%s'"
                % (obj_name, path)
            )
        _ext_debug("        Added integration alias '%s' from '%s'" % (name, obj_path))
        module_globals[name] = obj


def _get_ext_aliases(module_globals):
    return module_globals.get("ALIASES_DESC", [])


def _set_ext_aliases(module_globals, value):
    module_globals["ALIASES_DESC"] = value


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

    _set_ext_aliases(
        module_globals,
        list(
            map(lambda p: (p[0], resolve_path(p[1])), _get_ext_aliases(module_globals))
        ),
    )
