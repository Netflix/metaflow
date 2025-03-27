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

    def invoke(self, ctx):
        # NOTE: This is copied from MultiCommand.invoke. The change is that we
        # behave like chain in the sense that we evaluate the subcommand *after*
        # invoking the base command but we don't chain the commands like self.chain
        # would otherwise indicate.
        # The goal of this is to make sure that the first command is properly executed
        # *first* prior to loading the other subcommands. It's more a lazy_subcommand_load
        # than a chain.
        # Look for CHANGE HERE in this code to see where the changes are made.
        # If click is updated, this may also need to be updated. This version is for
        # click 7.1.2.
        def _process_result(value):
            if self.result_callback is not None:
                value = ctx.invoke(self.result_callback, value, **ctx.params)
            return value

        if not ctx.protected_args:
            # If we are invoked without command the chain flag controls
            # how this happens.  If we are not in chain mode, the return
            # value here is the return value of the command.
            # If however we are in chain mode, the return value is the
            # return value of the result processor invoked with an empty
            # list (which means that no subcommand actually was executed).
            if self.invoke_without_command:
                # CHANGE HERE: We behave like self.chain = False here

                # if not self.chain:
                return click.Command.invoke(self, ctx)
                # with ctx:
                #    click.Command.invoke(self, ctx)
                #    return _process_result([])

            ctx.fail("Missing command.")

        # Fetch args back out
        args = ctx.protected_args + ctx.args
        ctx.args = []
        ctx.protected_args = []
        # CHANGE HERE: Add saved_args so we have access to it in the command to be
        # able to infer what we are calling next
        ctx.saved_args = args

        # If we're not in chain mode, we only allow the invocation of a
        # single command but we also inform the current context about the
        # name of the command to invoke.
        # CHANGE HERE: We change this block to do the invoke *before* the resolve_command
        # Make sure the context is entered so we do not clean up
        # resources until the result processor has worked.
        with ctx:
            ctx.invoked_subcommand = "*" if args else None
            click.Command.invoke(self, ctx)
            cmd_name, cmd, args = self.resolve_command(ctx, args)
            sub_ctx = cmd.make_context(cmd_name, args, parent=ctx)
            with sub_ctx:
                return _process_result(sub_ctx.command.invoke(sub_ctx))

        # CHANGE HERE: Removed all the part of chain mode.

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
