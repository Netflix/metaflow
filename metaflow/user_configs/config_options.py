import json
import os

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from metaflow._vendor import click

from .config_parameters import CONFIG_FILE, ConfigValue
from ..exception import MetaflowException, MetaflowInternalError
from ..parameters import DeployTimeField, ParameterContext, current_flow
from ..util import get_username


def _load_config_values(info_file: Optional[str] = None) -> Optional[Dict[Any, Any]]:
    if info_file is None:
        info_file = os.path.basename(CONFIG_FILE)
    try:
        with open(info_file, encoding="utf-8") as contents:
            return json.load(contents).get("user_configs", {})
    except IOError:
        return None


class ConvertPath(click.Path):
    name = "ConvertPath"

    def convert(self, value, param, ctx):
        if isinstance(value, str) and value.startswith("converted:"):
            return value
        v = super().convert(value, param, ctx)
        return self.convert_value(v)

    @staticmethod
    def convert_value(value):
        if value is None:
            return None
        try:
            with open(value, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError:
            return "converted:!!NO_FILE!!%s" % value
        return "converted:" + content


class ConvertDictOrStr(click.ParamType):
    name = "ConvertDictOrStr"

    def convert(self, value, param, ctx):
        return self.convert_value(value)

    @staticmethod
    def convert_value(value):
        if value is None:
            return None

        if isinstance(value, dict):
            return "converted:" + json.dumps(value)

        if value.startswith("converted:"):
            return value

        return "converted:" + value


class MultipleTuple(click.Tuple):
    # Small wrapper around a click.Tuple to allow the environment variable for
    # configurations to be a JSON string. Otherwise the default behavior is splitting
    # by whitespace which is totally not what we want
    # You can now pass multiple configuration options through an environment variable
    # using something like:
    # METAFLOW_FLOW_CONFIG='{"config1": "filenameforconfig1.json", "config2": {"key1": "value1"}}'

    def split_envvar_value(self, rv):
        loaded = json.loads(rv)
        return list(
            item if isinstance(item, str) else json.dumps(item)
            for pair in loaded.items()
            for item in pair
        )


class ConfigInput:
    # ConfigInput is an internal class responsible for processing all the --config and
    # --config-value options.
    # It gathers information from the --local-config-file (to figure out
    # where options are stored) and is also responsible for processing any `--config` or
    # `--config-value` options. Note that the process_configs function will be called
    # *twice* (once for the configs and another for the config-values). This makes
    # this function a little bit more tricky. We need to wait for both calls before
    # being able to process anything.

    # It will then store this information in the flow spec for use later in processing.
    # It is stored in the flow spec to avoid being global to support the Runner.

    loaded_configs = None  # type: Optional[Dict[str, Dict[Any, Any]]]
    config_file = None  # type: Optional[str]

    def __init__(
        self,
        req_configs: List[str],
        defaults: Dict[str, Tuple[Union[str, Dict[Any, Any]], bool]],
        parsers: Dict[str, Callable[[str], Dict[Any, Any]]],
    ):
        self._req_configs = set(req_configs)
        self._defaults = defaults
        self._parsers = parsers
        self._path_values = None
        self._value_values = None

    @staticmethod
    def make_key_name(name: str) -> str:
        # Special mark to indicate that the configuration value is not content or a file
        # name but a value that should be read in the config file (effectively where
        # the value has already been materialized).
        return "kv." + name.lower()

    @classmethod
    def set_config_file(cls, config_file: str):
        cls.config_file = config_file

    @classmethod
    def get_config(cls, config_name: str) -> Optional[Dict[Any, Any]]:
        if cls.loaded_configs is None:
            all_configs = _load_config_values(cls.config_file)
            if all_configs is None:
                raise MetaflowException(
                    "Could not load expected configuration values "
                    "from the CONFIG_PARAMETERS file. This is a Metaflow bug. "
                    "Please contact support."
                )
            cls.loaded_configs = all_configs
        return cls.loaded_configs.get(config_name, None)

    def process_configs(self, ctx, param, value):
        from ..cli import echo_always, echo_dev_null  # Prevent circular import
        from ..flowspec import _FlowState  # Prevent circular import

        flow_cls = getattr(current_flow, "flow_cls", None)
        if flow_cls is None:
            # This is an error
            raise MetaflowInternalError(
                "Config values should be processed for a FlowSpec"
            )

        # This function is called by click when processing all the --config and
        # --config-value options.
        # The value passed in is a list of tuples (name, value).
        # Click will provide:
        #   - all the defaults if nothing is provided on the command line
        #   - provide *just* the passed in value if anything is provided on the command
        #     line.
        #
        # We need to get all config and config-value options and click will call this
        # function twice. We will first get all the values on the command line and
        # *then* merge with the defaults to form a full set of values.
        # We therefore get a full set of values where:
        #  - the name will correspond to the configuration name
        #  - the value will be the default (including None if there is no default) or
        #    the string representation of the value (this will always include
        #    the "converted:" prefix as it will have gone through the ConvertPath or
        #    ConvertDictOrStr conversion function).
        #    A value of None basically means that the config has
        #    no default and was not specified on the command line.

        print("Got arg name %s and values %s" % (param.name, str(value)))
        do_return = self._value_values is None and self._path_values is None
        if param.name == "config_value_options":
            self._value_values = {k.lower(): v for k, v in value if v is not None}
        else:
            self._path_values = {k.lower(): v for k, v in value if v is not None}
        if do_return:
            # One of config_value_options or config_file_options will be None
            return None

        # The second go around, we process all the values and merge them.
        # Check that the user didn't provide *both* a path and a value. We know that
        # defaults are not both non None (this is an error) so if they are both
        # non-None (and actual files) here, it means the user explicitly provided both.
        common_keys = set(self._value_values or []).intersection(
            [
                k
                for k, v in self._path_values.items()
                if v and not v.startswith("converted:!!NO_FILE!!")
            ]
            or []
        )
        if common_keys:
            raise click.UsageError(
                "Cannot provide both a value and a file for the same configuration. "
                "Found such values for '%s'" % "', '".join(common_keys)
            )

        # NOTE: Important to start with _path_values as they can have the
        # NO_FILE special value. They will be used (and trigger an error) iff there is
        # no other value provided.
        all_values = dict(self._path_values or {})
        all_values.update(self._value_values or {})

        print("Got all values: %s" % str(all_values))
        flow_cls._flow_state[_FlowState.CONFIGS] = {}

        to_return = {}
        merged_configs = {}
        for name, (val, is_path) in self._defaults.items():
            n = name.lower()
            if n in all_values:
                merged_configs[n] = all_values[n]
            else:
                if isinstance(val, DeployTimeField):
                    # This supports a default value that is a deploy-time field (similar
                    # to Parameter).)
                    # We will form our own context and pass it down -- note that you cannot
                    # use configs in the default value of configs as this introduces a bit
                    # of circularity. Note also that quiet and datastore are *eager*
                    # options so are available here.
                    param_ctx = ParameterContext(
                        flow_name=ctx.obj.flow.name,
                        user_name=get_username(),
                        parameter_name=n,
                        logger=echo_dev_null if ctx.params["quiet"] else echo_always,
                        ds_type=ctx.params["datastore"],
                        configs=None,
                    )
                    val = val.fun(param_ctx)
                if is_path:
                    # This is a file path
                    merged_configs[n] = ConvertPath.convert_value(val)
                else:
                    # This is a value
                    merged_configs[n] = ConvertDictOrStr.convert_value(val)

        missing_configs = set()
        no_file = []
        msgs = []
        for name, val in merged_configs.items():
            if val is None:
                missing_configs.add(name)
                continue
            if val.startswith("converted:!!NO_FILE!!"):
                no_file.append(name)
                continue
            val = val[10:]  # Remove the "converted:" prefix
            if val.startswith("kv."):
                # This means to load it from a file
                read_value = self.get_config(val[3:])
                if read_value is None:
                    raise click.UsageError(
                        "Could not find configuration '%s' in INFO file" % val
                    )
                flow_cls._flow_state[_FlowState.CONFIGS][name] = read_value
                to_return[name] = ConfigValue(read_value)
            else:
                if self._parsers[name]:
                    read_value = self._parsers[name](val)
                else:
                    try:
                        read_value = json.loads(val)
                    except json.JSONDecodeError as e:
                        msgs.append(
                            "configuration value for '%s' is not valid JSON: %s"
                            % (name, e)
                        )
                        continue
                    # TODO: Support YAML
                flow_cls._flow_state[_FlowState.CONFIGS][name] = read_value
                to_return[name] = ConfigValue(read_value)

        reqs = missing_configs.intersection(self._req_configs)
        for missing in reqs:
            msgs.append("missing configuration for '%s'" % missing)
        for missing in no_file:
            msgs.append(
                "configuration file '%s' could not be read for '%s'"
                % (merged_configs[missing][21:], missing)
            )
        if msgs:
            raise click.UsageError(
                "Bad values passed for configuration options: %s" % ", ".join(msgs)
            )
        return to_return

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "ConfigInput"


class LocalFileInput(click.Path):
    # Small wrapper around click.Path to set the value from which to read configuration
    # values. This is set immediately upon processing the --local-config-file
    # option and will therefore then be available when processing any of the other
    # --config options (which will call ConfigInput.process_configs
    name = "LocalFileInput"

    def convert(self, value, param, ctx):
        v = super().convert(value, param, ctx)
        ConfigInput.set_config_file(value)
        return v

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "LocalFileInput"


def config_options(cmd):
    help_strs = []
    required_names = []
    defaults = {}
    config_seen = set()
    parsers = {}
    flow_cls = getattr(current_flow, "flow_cls", None)
    if flow_cls is None:
        return cmd

    parameters = [p for _, p in flow_cls._get_parameters() if p.IS_FLOW_PARAMETER]
    # List all the configuration options
    for arg in parameters[::-1]:
        save_default = arg.kwargs.get("default", None)
        kwargs = arg.option_kwargs(False)
        if arg.name.lower() in config_seen:
            msg = (
                "Multiple configurations use the same name '%s'. Note that names are "
                "case-insensitive. Please change the "
                "names of some of your configurations" % arg.name
            )
            raise MetaflowException(msg)
        config_seen.add(arg.name.lower())
        if kwargs["required"]:
            required_names.append(arg.name)
        defaults[arg.name.lower()] = (save_default, arg._default_is_file)
        help_strs.append("  - %s: %s" % (arg.name.lower(), kwargs.get("help", "")))
        parsers[arg.name.lower()] = arg.parser

    if not config_seen:
        # No configurations -- don't add anything
        return cmd

    help_str = (
        "Configuration options for the flow. "
        "Multiple configurations can be specified."
    )
    help_str = "\n\n".join([help_str] + help_strs)
    cb_func = ConfigInput(required_names, defaults, parsers).process_configs

    cmd.params.insert(
        0,
        click.Option(
            ["--config-value", "config_value_options"],
            nargs=2,
            multiple=True,
            type=MultipleTuple([click.Choice(config_seen), ConvertDictOrStr()]),
            callback=cb_func,
            help=help_str,
            envvar="METAFLOW_FLOW_CONFIG_VALUE",
            show_default=False,
            default=[
                (k, v[0] if not callable(v[0]) and not v[1] else None)
                for k, v in defaults.items()
            ],
            required=False,
        ),
    )
    cmd.params.insert(
        0,
        click.Option(
            ["--config", "config_file_options"],
            nargs=2,
            multiple=True,
            type=MultipleTuple([click.Choice(config_seen), ConvertPath()]),
            callback=cb_func,
            help=help_str,
            envvar="METAFLOW_FLOW_CONFIG",
            show_default=False,
            default=[
                (k, v[0] if not callable(v) and v[1] else None)
                for k, v in defaults.items()
            ],
            required=False,
        ),
    )
    return cmd
