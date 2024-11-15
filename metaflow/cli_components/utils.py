import importlib
from metaflow._vendor import click
from metaflow.extension_support.plugins import get_plugin


class LazyPluginCommandCollection(click.CommandCollection):
    # lazy_source should only point to things that are resolved as CLI plugins.
    def __init__(self, *args, lazy_sources=None, **kwargs):
        super().__init__(*args, **kwargs)
        # lazy_sources is a list of strings in the form
        # "{plugin_name}" -> "{module-name}.{command-object-name}"
        self.lazy_sources = lazy_sources or {}
        self._lazy_loaded = {}

    def list_commands(self, ctx):
        base = super().list_commands(ctx)
        for source_name, source in self.lazy_sources.items():
            subgroup = self._lazy_load(source_name, source)
            base.extend(subgroup.list_commands(ctx))
        return base

    def get_command(self, ctx, cmd_name):
        base_cmd = super().get_command(ctx, cmd_name)
        if base_cmd is not None:
            return base_cmd
        for source_name, source in self.lazy_sources.items():
            subgroup = self._lazy_load(source_name, source)
            cmd = subgroup.get_command(ctx, cmd_name)
            if cmd is not None:
                return cmd
        return None

    def _lazy_load(self, source_name, source_path):
        if source_name in self._lazy_loaded:
            return self._lazy_loaded[source_name]
        cmd_object = get_plugin("cli", source_path, source_name)
        if not isinstance(cmd_object, click.Group):
            raise ValueError(
                f"Lazy loading of {source_name} failed by returning "
                "a non-group object"
            )
        self._lazy_loaded[source_name] = cmd_object
        return cmd_object


class LazyGroup(click.Group):
    def __init__(self, *args, lazy_subcommands=None, **kwargs):
        super().__init__(*args, **kwargs)
        # lazy_subcommands is a list of strings in the form
        # "{command} -> "{module-name}.{command-object-name}"
        self.lazy_subcommands = lazy_subcommands or {}
        self._lazy_loaded = {}

    def list_commands(self, ctx):
        base = super().list_commands(ctx)
        lazy = sorted(self.lazy_subcommands.keys())
        return base + lazy

    def get_command(self, ctx, cmd_name):
        if cmd_name in self.lazy_subcommands:
            return self._lazy_load(cmd_name)
        return super().get_command(ctx, cmd_name)

    def _lazy_load(self, cmd_name):
        if cmd_name in self._lazy_loaded:
            return self._lazy_loaded[cmd_name]

        import_path = self.lazy_subcommands[cmd_name]
        modname, cmd = import_path.rsplit(".", 1)
        # do the import
        mod = importlib.import_module(modname)
        # get the Command object from that module
        cmd_object = getattr(mod, cmd)
        # check the result to make debugging easier. note that wrapped BaseCommand
        # can be functions
        if not isinstance(cmd_object, click.BaseCommand):
            raise ValueError(
                f"Lazy loading of {import_path} failed by returning "
                f"a non-command object {type(cmd_object)}"
            )
        self._lazy_loaded[cmd_name] = cmd_object
        return cmd_object
