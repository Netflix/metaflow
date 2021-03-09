# This module provides a global singleton `cli_args` which stores the `top` and
# `step` level options for the metaflow CLI.
# This allows decorators to have access to the CLI options instead of relying
# (solely) on the click context.
# TODO(crk): Fold `dict_to_cli_options` as a private method of this `CLIArgs`
# once all other callers of `step` [titus, meson etc.] are unified.
from .util import dict_to_cli_options

class CLIArgs(object):
    def __init__(self):
        self._top_kwargs = {}
        self._step_kwargs = {}

    def _set_step_kwargs(self, kwargs):
        self._step_kwargs = kwargs

    def _set_top_kwargs(self, kwargs):
        self._top_kwargs = kwargs

    @property
    def top_kwargs(self):
        return self._top_kwargs

    @property
    def step_kwargs(self):
        return self._step_kwargs

    def step_command(self,
                     executable,
                     script,
                     step_name,
                     top_kwargs=None,
                     step_kwargs=None):
        cmd = [executable, '-u', script]
        if top_kwargs is None:
            top_kwargs = self._top_kwargs
        if step_kwargs is None:
            step_kwargs = self._step_kwargs

        top_args_list = [arg for arg in dict_to_cli_options(top_kwargs)]
        cmd.extend(top_args_list)
        cmd.extend(['step', step_name])
        step_args_list = [arg for arg in dict_to_cli_options(step_kwargs)]
        cmd.extend(step_args_list)

        return cmd

cli_args = CLIArgs()
