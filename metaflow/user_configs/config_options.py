import importlib
import json
import os

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from metaflow._vendor import click
from metaflow.debug import debug

from .config_parameters import ConfigValue
from ..exception import MetaflowException, MetaflowInternalError
from ..packaging_sys import MetaflowCodeContent
from ..parameters import DeployTimeField, ParameterContext, current_flow
from ..util import get_username


_CONVERT_PREFIX = "@!c!@:"
_DEFAULT_PREFIX = "@!d!@:"
_NO_FILE = "@!n!@:"

_CONVERTED_DEFAULT = _CONVERT_PREFIX + _DEFAULT_PREFIX
_CONVERTED_NO_FILE = _CONVERT_PREFIX + _NO_FILE
_CONVERTED_DEFAULT_NO_FILE = _CONVERTED_DEFAULT + _NO_FILE


def _load_config_values(info_file: Optional[str] = None) -> Optional[Dict[Any, Any]]:
    if info_file is None:
        config_content = MetaflowCodeContent.get_config()
    else:
        try:
            with open(info_file, encoding="utf-8") as f:
                config_content = json.load(f)
        except IOError:
            return None
    if config_content:
        return config_content.get("user_configs", {})
    return None


class ConvertPath(click.Path):
    name = "ConvertPath"

    def convert(self, value, param, ctx):
        if isinstance(value, str) and value.startswith(_CONVERT_PREFIX):
            return value
        is_default = False
        if value and value.startswith(_DEFAULT_PREFIX):
            is_default = True
            value = value[len(_DEFAULT_PREFIX) :]
        value = super().convert(value, param, ctx)
        return self.convert_value(value, is_default)

    @staticmethod
    def mark_as_default(value):
        if value is None:
            return None
        return _DEFAULT_PREFIX + str(value)

    @staticmethod
    def convert_value(value, is_default):
        default_str = _DEFAULT_PREFIX if is_default else ""
        if value is None:
            return None
        try:
            with open(value, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError:
            return _CONVERT_PREFIX + default_str + _NO_FILE + value
        return _CONVERT_PREFIX + default_str + content


class ConvertDictOrStr(click.ParamType):
    name = "ConvertDictOrStr"

    def convert(self, value, param, ctx):
        is_default = False
        if isinstance(value, str):
            if value.startswith(_CONVERT_PREFIX):
                return value
            if value.startswith(_DEFAULT_PREFIX):
                is_default = True
                value = value[len(_DEFAULT_PREFIX) :]

        return self.convert_value(value, is_default)

    @staticmethod
    def convert_value(value, is_default):
        default_str = _DEFAULT_PREFIX if is_default else ""
        if value is None:
            return None

        if isinstance(value, dict):
            return _CONVERT_PREFIX + default_str + json.dumps(value)

        if value.startswith(_CONVERT_PREFIX):
            return value

        return _CONVERT_PREFIX + default_str + value

    @staticmethod
    def mark_as_default(value):
        if value is None:
            return None
        if isinstance(value, dict):
            return _DEFAULT_PREFIX + json.dumps(value)
        return _DEFAULT_PREFIX + str(value)


class MultipleTuple(click.Tuple):
    # Small wrapper around a click.Tuple to allow the environment variable for
    # configurations to be a JSON string. Otherwise the default behavior is splitting
    # by whitespace which is totally not what we want
    # You can now pass multiple configuration options through an environment variable
    # using something like:
    # METAFLOW_FLOW_CONFIG_VALUE='{"config1": {"key0": "value0"}, "config2": {"key1": "value1"}}'
    # or METAFLOW_FLOW_CONFIG='{"config1": "file1", "config2": "file2"}'

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
        parsers: Dict[str, Union[str, Callable[[str], Dict[Any, Any]]]],
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
        return cls.loaded_configs[config_name]

    def process_configs(
        self,
        flow_name: str,
        param_name: str,
        param_value: Dict[str, Optional[str]],
        quiet: bool,
        datastore: str,
        click_obj: Optional[Any] = None,
    ):
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
        #  - the value will be:
        #       - the default (including None if there is no default). If the default is
        #         not None, it will start with _CONVERTED_DEFAULT since Click will make
        #         the value go through ConvertPath or ConvertDictOrStr
        #       - the actual value passed through prefixed with _CONVERT_PREFIX

        debug.userconf_exec(
            "Processing configs for %s -- incoming values: %s"
            % (param_name, str(param_value))
        )

        do_return = self._value_values is None and self._path_values is None
        # We only keep around non default values. We could simplify by checking just one
        # value and if it is default it means all are but this doesn't seem much more effort
        # and is clearer
        if param_name == "config_value":
            self._value_values = {
                k.lower(): v
                for k, v in param_value.items()
                if v is not None and not v.startswith(_CONVERTED_DEFAULT)
            }
        else:
            self._path_values = {
                k.lower(): v
                for k, v in param_value.items()
                if v is not None and not v.startswith(_CONVERTED_DEFAULT)
            }
        if do_return:
            # One of values["value"] or values["path"] is None -- we are in the first
            # go around
            debug.userconf_exec("Incomplete config options; waiting for more")
            return None

        # The second go around, we process all the values and merge them.

        # If we are processing options that start with kv., we know we are in a subprocess
        # and ignore other stuff. In particular, environment variables used to pass
        # down configurations (like METAFLOW_FLOW_CONFIG) could still be present and
        # would cause an issue -- we can ignore those as the kv. values should trump
        # everything else.
        # NOTE: These are all *non default* keys
        all_keys = set(self._value_values).union(self._path_values)

        if all_keys and click_obj:
            click_obj.has_cl_config_options = True
        # Make sure we have at least some non default keys (we need some if we have
        # all kv)
        has_all_kv = all_keys and all(
            self._value_values.get(k, "").startswith(_CONVERT_PREFIX + "kv.")
            for k in all_keys
        )

        flow_cls._flow_state[_FlowState.CONFIGS] = {}
        to_return = {}

        if not has_all_kv:
            # Check that the user didn't provide *both* a path and a value. Again, these
            # are only user-provided (not defaults)
            common_keys = set(self._value_values or []).intersection(
                [k for k, v in self._path_values.items()] or []
            )
            if common_keys:
                exc = click.UsageError(
                    "Cannot provide both a value and a file for the same configuration. "
                    "Found such values for '%s'" % "', '".join(common_keys)
                )
                if click_obj:
                    click_obj.delayed_config_exception = exc
                    return None
                raise exc

            all_values = dict(self._path_values)
            all_values.update(self._value_values)

            debug.userconf_exec("All config values: %s" % str(all_values))

            merged_configs = {}
            # Now look at everything (including defaults)
            for name, (val, is_path) in self._defaults.items():
                n = name.lower()
                if n in all_values:
                    # We have the value provided by the user -- use that.
                    merged_configs[n] = all_values[n]
                else:
                    # No value provided by the user -- use the default
                    if isinstance(val, DeployTimeField):
                        # This supports a default value that is a deploy-time field (similar
                        # to Parameter).)
                        # We will form our own context and pass it down -- note that you cannot
                        # use configs in the default value of configs as this introduces a bit
                        # of circularity. Note also that quiet and datastore are *eager*
                        # options so are available here.
                        param_ctx = ParameterContext(
                            flow_name=flow_name,
                            user_name=get_username(),
                            parameter_name=n,
                            logger=(echo_dev_null if quiet else echo_always),
                            ds_type=datastore,
                            configs=None,
                        )
                        val = val.fun(param_ctx)
                    if is_path:
                        # This is a file path
                        merged_configs[n] = ConvertPath.convert_value(val, True)
                    else:
                        # This is a value
                        merged_configs[n] = ConvertDictOrStr.convert_value(val, True)
        else:
            debug.userconf_exec("Fast path due to pre-processed values")
            merged_configs = self._value_values

        if click_obj:
            click_obj.has_config_options = True

        debug.userconf_exec("Configs merged with defaults: %s" % str(merged_configs))

        missing_configs = set()
        no_file = []
        no_default_file = []
        msgs = []
        for name, val in merged_configs.items():
            if val is None:
                missing_configs.add(name)
                to_return[name] = None
                flow_cls._flow_state[_FlowState.CONFIGS][name] = None
                continue
            if val.startswith(_CONVERTED_NO_FILE):
                no_file.append(name)
                continue
            if val.startswith(_CONVERTED_DEFAULT_NO_FILE):
                no_default_file.append(name)
                continue

            val = val[len(_CONVERT_PREFIX) :]  # Remove the _CONVERT_PREFIX
            if val.startswith(_DEFAULT_PREFIX):  # Remove the _DEFAULT_PREFIX if needed
                val = val[len(_DEFAULT_PREFIX) :]
            if val.startswith("kv."):
                # This means to load it from a file
                try:
                    read_value = self.get_config(val[3:])
                except KeyError as e:
                    exc = click.UsageError(
                        "Could not find configuration '%s' in INFO file" % val
                    )
                    if click_obj:
                        click_obj.delayed_config_exception = exc
                        return None
                    raise exc from e
                flow_cls._flow_state[_FlowState.CONFIGS][name] = read_value
                to_return[name] = (
                    ConfigValue(read_value) if read_value is not None else None
                )
            else:
                if self._parsers[name]:
                    read_value = self._call_parser(self._parsers[name], val)
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
                to_return[name] = (
                    ConfigValue(read_value) if read_value is not None else None
                )

        reqs = missing_configs.intersection(self._req_configs)
        for missing in reqs:
            msgs.append("missing configuration for '%s'" % missing)
        for missing in no_file:
            msgs.append(
                "configuration file '%s' could not be read for '%s'"
                % (merged_configs[missing][len(_CONVERTED_NO_FILE) :], missing)
            )
        for missing in no_default_file:
            msgs.append(
                "default configuration file '%s' could not be read for '%s'"
                % (merged_configs[missing][len(_CONVERTED_DEFAULT_NO_FILE) :], missing)
            )
        if msgs:
            exc = click.UsageError(
                "Bad values passed for configuration options: %s" % ", ".join(msgs)
            )
            if click_obj:
                click_obj.delayed_config_exception = exc
                return None
            raise exc

        debug.userconf_exec("Finalized configs: %s" % str(to_return))
        return to_return

    def process_configs_click(self, ctx, param, value):
        return self.process_configs(
            ctx.obj.flow.name,
            param.name,
            dict(value),
            ctx.params["quiet"],
            ctx.params["datastore"],
            click_obj=ctx.obj,
        )

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "ConfigInput"

    @staticmethod
    def _call_parser(parser, val):
        if isinstance(parser, str):
            if len(parser) and parser[0] == ".":
                parser = "metaflow" + parser
            path, func = parser.rsplit(".", 1)
            try:
                func_module = importlib.import_module(path)
            except ImportError as e:
                raise ValueError("Cannot locate parser %s" % parser) from e
            parser = getattr(func_module, func, None)
            if parser is None or not callable(parser):
                raise ValueError(
                    "Parser %s is either not part of %s or not a callable"
                    % (func, path)
                )
        return parser(val)


class LocalFileInput(click.Path):
    # Small wrapper around click.Path to set the value from which to read configuration
    # values. This is set immediately upon processing the --local-config-file
    # option and will therefore then be available when processing any of the other
    # --config options (which will call ConfigInput.process_configs)
    name = "LocalFileInput"

    def convert(self, value, param, ctx):
        v = super().convert(value, param, ctx)
        ConfigInput.set_config_file(value)
        return v

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "LocalFileInput"


def config_options_with_config_input(cmd):
    help_strs = []
    required_names = []
    defaults = {}
    config_seen = set()
    parsers = {}
    flow_cls = getattr(current_flow, "flow_cls", None)
    if flow_cls is None:
        return cmd, None

    parameters = [p for _, p in flow_cls._get_parameters() if p.IS_CONFIG_PARAMETER]
    # List all the configuration options
    for arg in parameters[::-1]:
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

        defaults[arg.name.lower()] = (
            arg.kwargs.get("default", None),
            arg._default_is_file,
        )
        help_strs.append("  - %s: %s" % (arg.name.lower(), kwargs.get("help", "")))
        parsers[arg.name.lower()] = arg.parser

    if not config_seen:
        # No configurations -- don't add anything; we set it to False so that it
        # can be checked whether or not we called this.
        return cmd, False

    help_str = (
        "Configuration options for the flow. "
        "Multiple configurations can be specified. Cannot be used with resume."
    )
    help_str = "\n\n".join([help_str] + help_strs)
    config_input = ConfigInput(required_names, defaults, parsers)
    cb_func = config_input.process_configs_click

    cmd.params.insert(
        0,
        click.Option(
            ["--config-value", "config_value"],
            nargs=2,
            multiple=True,
            type=MultipleTuple([click.Choice(config_seen), ConvertDictOrStr()]),
            callback=cb_func,
            help=help_str,
            envvar="METAFLOW_FLOW_CONFIG_VALUE",
            show_default=False,
            default=[
                (
                    k,
                    (
                        ConvertDictOrStr.mark_as_default(v[0])
                        if not callable(v[0]) and not v[1]
                        else None
                    ),
                )
                for k, v in defaults.items()
            ],
            required=False,
        ),
    )
    cmd.params.insert(
        0,
        click.Option(
            ["--config", "config"],
            nargs=2,
            multiple=True,
            type=MultipleTuple([click.Choice(config_seen), ConvertPath()]),
            callback=cb_func,
            help=help_str,
            envvar="METAFLOW_FLOW_CONFIG",
            show_default=False,
            default=[
                (
                    k,
                    (
                        ConvertPath.mark_as_default(v[0])
                        if not callable(v[0]) and v[1]
                        else None
                    ),
                )
                for k, v in defaults.items()
            ],
            required=False,
        ),
    )
    return cmd, config_input


def config_options(cmd):
    cmd, _ = config_options_with_config_input(cmd)
    return cmd
