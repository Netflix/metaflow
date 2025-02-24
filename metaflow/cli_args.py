# This class provides a global singleton `cli_args` which stores the `top` and
# `step` level options for the metaflow CLI. This allows decorators to have
# access to the CLI options instead of relying (solely) on the click context.
# TODO: We have two CLIArgs:
#  - this one, which captures the top level and step-level options passed to the
#    step command and is used primarily for UBF to replicate the exact command
#    line passed
#  - one in runtime.py which is used to construct the step command and modified by
#    runtime_step_cli. Both are similar in nature and should be unified in some way
#
# TODO: dict_to_cli_options uses shlex which causes some issues with this as
# well as the converting of options in runtime.py. We should make it so that we
# can properly shlex things and un-shlex when using. Ideally this should all be
# done in one place.
#
# NOTE: There is an important between these two as well:
#  - this one will include local_config_file whereas the other one WILL NOT.
#    This is because this is used when constructing the parallel UBF command which
#    executes locally and therefore needs the local_config_file but the other (remote)
#    commands do not.

from .user_configs.config_options import ConfigInput
from .util import to_unicode


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

    def step_command(
        self, executable, script, step_name, top_kwargs=None, step_kwargs=None
    ):
        cmd = [executable, "-u", script]
        if top_kwargs is None:
            top_kwargs = self._top_kwargs
        if step_kwargs is None:
            step_kwargs = self._step_kwargs

        top_args_list = list(self._options(top_kwargs))
        cmd.extend(top_args_list)
        cmd.extend(["step", step_name])
        step_args_list = list(self._options(step_kwargs))
        cmd.extend(step_args_list)

        return cmd

    @staticmethod
    def _options(mapping):
        for k, v in mapping.items():

            # None or False arguments are ignored
            # v needs to be explicitly False, not falsy, e.g. 0 is an acceptable value
            if v is None or v is False:
                continue

            # we need special handling for 'with' since it is a reserved
            # keyword in Python, so we call it 'decospecs' in click args
            if k == "decospecs":
                k = "with"
            if k in ("config", "config_value"):
                # Special handling here since we gather them all in one option but actually
                # need to send them one at a time using --config-value <name> kv.<name>.
                # Note it can be either config or config_value depending
                # on click processing order.
                for config_name in v.keys():
                    yield "--config-value"
                    yield to_unicode(config_name)
                    yield to_unicode(ConfigInput.make_key_name(config_name))
                continue
            k = k.replace("_", "-")
            v = v if isinstance(v, (list, tuple, set)) else [v]
            for value in v:
                yield "--%s" % k
                if not isinstance(value, bool):
                    yield to_unicode(value)


cli_args = CLIArgs()
