from metaflow.extension_support import _ext_debug


def add_cmd_support(g, base_init=False):
    # See the similar add_plugin_support function in plugin/__init__.py
    g["__cmds"] = {}

    if base_init:
        g["__ext_add_cmds"] = []

    def _add(name, path, cli, pkg=g["__package__"], add_to=g["__cmds"]):
        if path[0] == ".":
            pkg_components = pkg.split(".")
            i = 1
            while i < len(path) and path[i] == ".":
                i += 1
            # We deal with multiple periods at the start
            if i > len(pkg_components):
                raise ValueError("Path '%s' exits out of metaflow module" % path)
            path = (
                ".".join(pkg_components[: -i + 1] if i > 1 else pkg_components)
                + path[i - 1 :]
            )
        _ext_debug("    Adding cmd: %s from %s.%s" % (name, path, cli))
        add_to[name] = (path, cli)

    g["cmd_add"] = _add
