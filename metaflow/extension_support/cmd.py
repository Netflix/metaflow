import importlib
import traceback

from metaflow.metaflow_config_funcs import from_conf

from . import _ext_debug, get_modules

_all_cmds = []
_all_cmds_dict = {}

# Set ENABLED_ and _TOGGLE_ variables for commands
ENABLED_CMD = from_conf("ENABLED_CMD")
_TOGGLE_CMD = []

# This file is identical in functionality to the plugins.py file. Please refer to that
# one for more information on what the functions do.


def process_cmds(module_globals):
    global _all_cmds, _all_cmds_dict, ENABLED_CMD, _TOGGLE_CMD

    _resolve_relative_paths(module_globals)

    _all_cmds = _get_ext_cmds(module_globals)

    try:
        modules_to_import = get_modules("cmd")
        # This is like multiload_all but we load globals independently since we just care
        # about the TOGGLE and ENABLED values
        for m in modules_to_import:
            for n, o in m.module.__dict__.items():
                if n == "TOGGLE_CMD":
                    _TOGGLE_CMD.extend(o)
                elif n == "ENABLED_CMD":
                    ENABLED_CMD = o
            _resolve_relative_paths(m.module.__dict__)
            _all_cmds.extend(_get_ext_cmds(m.module.__dict__))
    except Exception as e:
        _ext_debug("\tWARNING: ignoring all cmds due to error during import: %s" % e)
        print(
            "WARNING: Cmds did not load -- ignoring all of them which may not "
            "be what you want: %s" % e
        )
        traceback.print_exc()

    # At this point, we have _all_cmds populated with all the tuples
    # (name, module_class) from all the cmds in all the extensions (if any)
    # We build a dictionary taking the latest presence for each name (so plugins
    # override metaflow core)
    for name, class_path in _all_cmds:
        _ext_debug("    Adding command '%s' from '%s'" % (name, class_path))
        _all_cmds_dict[name] = class_path

    # Resolve the ENABLED_CMD variable. The rules are the following:
    #  - if ENABLED_CMD is non None, it means it was either set directly by the user
    #    in a configuration file, on the command line or by an extension. In that case
    #    we honor those wishes and completely ignore the extensions' toggles.
    #  - if ENABLED_CMD is None, we populate it with everything included here and in
    #    all the extensions and use the TOGGLE_ list to produce the final list.
    # The rationale behind this is to support both a configuration option where the
    # cmds enabled are explicitly listed (typical in a lot of software) but also to
    # support a "configuration-less" version where the installation of the extensions
    # determines what is activated.
    if ENABLED_CMD is None:
        ENABLED_CMD = list(_all_cmds_dict) + _TOGGLE_CMD


def resolve_cmds():
    _ext_debug("    Resolving metaflow commands")
    list_of_cmds = ENABLED_CMD
    _ext_debug("        Raw list is: %s" % str(list_of_cmds))

    set_of_commands = set()
    for p in list_of_cmds:
        if p.startswith("-"):
            set_of_commands.discard(p[1:])
        elif p.startswith("+"):
            set_of_commands.add(p[1:])
        else:
            set_of_commands.add(p)
    _ext_debug("        Resolved list is: %s" % str(set_of_commands))

    to_return = []

    for name in set_of_commands:
        class_path = _all_cmds_dict.get(name, None)
        if class_path is None:
            raise ValueError(
                "Configuration requested command '%s' but no such command is available"
                % name
            )
        path, cls_name = class_path.rsplit(".", 1)
        try:
            cmd_module = importlib.import_module(path)
        except ImportError:
            raise ValueError("Cannot locate command '%s' at '%s'" % (name, path))

        cls = getattr(cmd_module, cls_name, None)
        if cls is None:
            raise ValueError(
                "Cannot locate '%s' class for command at '%s'" % (cls_name, path)
            )
        all_cmds = list(cls.commands)
        if len(all_cmds) > 1:
            raise ValueError("%s defines more than one command -- use a group" % path)
        if all_cmds[0] != name:
            raise ValueError(
                "%s: expected name to be '%s' but got '%s' instead"
                % (path, name, all_cmds[0])
            )
        to_return.append(cls)
        _ext_debug("        Added command '%s' from '%s'" % (name, class_path))

    return to_return


def _get_ext_cmds(module_globals):
    return module_globals.get("CMDS_DESC", [])


def _set_ext_cmds(module_globals, value):
    module_globals["CMDS_DESC"] = value


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

    _set_ext_cmds(
        module_globals,
        list(map(lambda p: (p[0], resolve_path(p[1])), _get_ext_cmds(module_globals))),
    )
