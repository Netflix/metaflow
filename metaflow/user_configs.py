import collections.abc
import json
import os
import re

from typing import Any, Callable, Dict, List, Optional, Union, TYPE_CHECKING

from metaflow import INFO_FILE
from metaflow._vendor import click

from .exception import MetaflowException, MetaflowInternalError
from .parameters import (
    DeployTimeField,
    Parameter,
    ParameterContext,
    current_flow,
)

from .util import get_username

if TYPE_CHECKING:
    from metaflow import FlowSpec

# _tracefunc_depth = 0


# def tracefunc(func):
#     """Decorates a function to show its trace."""

#     @functools.wraps(func)
#     def tracefunc_closure(*args, **kwargs):
#         global _tracefunc_depth
#         """The closure."""
#         print(f"{_tracefunc_depth}: {func.__name__}(args={args}, kwargs={kwargs})")
#         _tracefunc_depth += 1
#         result = func(*args, **kwargs)
#         _tracefunc_depth -= 1
#         print(f"{_tracefunc_depth} => {result}")
#         return result

#     return tracefunc_closure


def dump_config_values(flow: "FlowSpec"):
    if flow._user_configs:
        return "user_configs", flow._user_configs
    return None, None


def load_config_values(info_file: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if info_file is None:
        info_file = INFO_FILE
    try:
        with open(info_file, encoding="utf-8") as contents:
            return json.load(contents).get("user_configs", {})
    except IOError:
        return None


class ConfigValue(collections.abc.Mapping):
    # Thin wrapper to allow configuration values to be accessed using a "." notation
    # as well as a [] notation.

    def __init__(self, data: Dict[str, Any]):
        self._data = data

        for key, value in data.items():
            if isinstance(value, dict):
                value = ConfigValue(value)
            setattr(self, key, value)

    def __getitem__(self, key):
        value = self._data[key]
        if isinstance(value, dict):
            value = ConfigValue(value)
        return value

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        return repr(self._data)

    def __str__(self):
        return json.dumps(self._data)


class PathOrStr(click.ParamType):
    name = "PathOrStr"

    @staticmethod
    def convert_value(value):
        if value is None:
            return None

        if isinstance(value, dict):
            return "converted:" + json.dumps(value)

        if value.startswith("converted:"):
            return value

        if os.path.isfile(value):
            try:
                with open(value, "r", encoding="utf-8") as f:
                    content = f.read()
            except OSError as e:
                raise click.UsageError(
                    "Could not read configuration file '%s'" % value
                ) from e
            return "converted:" + content
        return "converted:" + value

    def convert(self, value, param, ctx):
        return self.convert_value(value)


class ConfigInput:
    # Contains the values loaded from the INFO file. We make this a class method
    # so that if there are multiple configs, we just need to read the file once.
    # It is OK to be globally unique because this is only evoked in scenario A.2 (see
    # convert method) which means we are already just executing a single task and so
    # there is no concern about it "leaking" to things running with Runner for example
    # (ie: even if Runner is evoked in that task, we won't "share" this global value's
    # usage).
    loaded_configs = None  # type: Optional[Dict[str, Dict[str, Any]]]
    info_file = None  # type: Optional[str]

    def __init__(
        self,
        req_configs: List[str],
        defaults: Dict[str, Union[str, Dict[str, Any]]],
        parsers: Dict[str, Callable[[str], Dict[str, Any]]],
    ):
        self._req_configs = set(req_configs)
        self._defaults = defaults
        self._parsers = parsers

    @staticmethod
    def make_key_name(name: str) -> str:
        return "kv." + name.lower()

    @classmethod
    def set_info_file(cls, info_file: str):
        cls.info_file = info_file

    @classmethod
    def get_config(cls, config_name: str) -> Optional[Dict[str, Any]]:
        if cls.loaded_configs is None:
            all_configs = load_config_values(cls.info_file)
            if all_configs is None:
                raise MetaflowException(
                    "Could not load expected configuration values "
                    "from the INFO file. This is a Metaflow bug. Please contact support."
                )
            cls.loaded_configs = all_configs
        return cls.loaded_configs.get(config_name, None)

    def process_configs(self, ctx, param, value):
        from .cli import echo_always, echo_dev_null  # Prevent circular import

        flow_cls = getattr(current_flow, "flow_cls", None)
        if flow_cls is None:
            # This is an error
            raise MetaflowInternalError(
                "Config values should be processed for a FlowSpec"
            )

        # value is a list of tuples (name, value).
        # Click will provide:
        #   - all the defaults if nothing is provided on the command line
        #   - provide *just* the passed in value if anything is provided on the command
        #     line.
        #
        # We therefore "merge" the defaults with what we are provided by click to form
        # a full set of values
        # We therefore get a full set of values where:
        #  - the name will correspond to the configuration name
        #  - the value will be the default (including None if there is no default) or
        #    the string representation of the value (this will always include
        #    the "converted:" prefix as it will have gone through the PathOrStr
        #    conversion function). A value of None basically means that the config has
        #    no default and was not specified on the command line.
        to_return = {}

        merged_configs = dict(self._defaults)
        for name, val in value:
            # Don't replace by None -- this is needed to avoid replacing a function
            # default
            if val:
                merged_configs[name] = val

        print("PARAMS: %s" % str(ctx.params))
        missing_configs = set()
        for name, val in merged_configs.items():
            name = name.lower()
            # convert is idempotent so if it is already converted, it will just return
            # the value. This is used to make sure we process the defaults
            if isinstance(val, DeployTimeField):
                # We will form our own context and pass it down
                param_ctx = ParameterContext(
                    flow_name=ctx.obj.flow.name,
                    user_name=get_username(),
                    parameter_name=name,
                    logger=echo_dev_null if ctx.params["quiet"] else echo_always,
                    ds_type=ctx.params["datastore"],
                    configs=None,
                )
                val = val.fun(param_ctx)
            val = PathOrStr.convert_value(val)
            if val is None:
                missing_configs.add(name)
                continue
            val = val[10:]  # Remove the "converted:" prefix
            if val.startswith("kv."):
                # This means to load it from a file
                read_value = self.get_config(val[3:])
                if read_value is None:
                    raise click.UsageError(
                        "Could not find configuration '%s' in INFO file" % val
                    )
                flow_cls._user_configs[name] = read_value
                to_return[name] = ConfigValue(read_value)
            else:
                if self._parsers[name]:
                    read_value = self._parsers[name](val)
                else:
                    try:
                        read_value = json.loads(val)
                    except json.JSONDecodeError as e:
                        raise click.UsageError(
                            "Configuration value for '%s' is not valid JSON" % name
                        ) from e
                    # TODO: Support YAML
                flow_cls._user_configs[name] = read_value
                to_return[name] = ConfigValue(read_value)

        if missing_configs.intersection(self._req_configs):
            raise click.UsageError(
                "Missing configuration values for %s" % ", ".join(missing_configs)
            )
        return to_return

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "ConfigInput"


class LocalFileInput(click.Path):
    name = "LocalFileInput"

    def convert(self, value, param, ctx):
        super().convert(value, param, ctx)
        ConfigInput.set_info_file(value)
        # This purposefully returns None which means it is *not* passed down
        # when commands use ctx.parent.parent.params to get all the configuration
        # values.

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "LocalFileInput"


ConfigArgType = Union[str, Dict[str, Any]]


class DelayEvaluator:
    """
    Small wrapper that allows the evaluation of a Config() value in a delayed manner.
    This is used when we want to use config.* values in decorators for example.
    """

    id_pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    def __init__(self, ex: str):
        self._config_expr = ex
        if self.id_pattern.match(self._config_expr):
            # This is a variable only so allow things like config_expr("config").var
            self._is_var_only = True
            self._access = []
        else:
            self._is_var_only = False
            self._access = None

    def __getattr__(self, name):
        if self._access is None:
            raise AttributeError()
        self._access.append(name)
        return self

    def __call__(self, ctx=None, deploy_time=False):
        # Two additional arguments are only used by DeployTimeField which will call
        # this function with those two additional arguments. They are ignored.
        flow_cls = getattr(current_flow, "flow_cls", None)
        if flow_cls is None:
            # We are not executing inside a flow (ie: not the CLI)
            raise MetaflowException(
                "Config object can only be used directly in the FlowSpec defining them. "
                "If using outside of the FlowSpec, please use ConfigEval"
            )
        if self._access is not None:
            # Build the final expression by adding all the fields in access as . fields
            self._config_expr = ".".join([self._config_expr] + self._access)
        # Evaluate the expression setting the config values as local variables
        return eval(
            self._config_expr,
            globals(),
            {k: ConfigValue(v) for k, v in flow_cls._user_configs.items()},
        )


def config_expr(expr: str) -> DelayEvaluator:
    """
    Function to allow you to use an expression involving a config parameter in
    places where it may not be directory accessible or if you want a more complicated
    expression than just a single variable.

    You can use it as follows:
      - When the config is not directly accessible:

            @project(name=config_expr("config").project.name)
            class MyFlow(FlowSpec):
                config = Config("config")
                ...
      - When you want a more complex expression:
            class MyFlow(FlowSpec):
                config = Config("config")

                @environment(vars={"foo": config_expr("config.bar.baz.lower()")})
                @step
                def start(self):
                    ...

    Parameters
    ----------
    expr : str
        Expression using the config values.
    """
    return DelayEvaluator(expr)


def eval_config(f: Callable[["FlowSpec"], "FlowSpec"]) -> "FlowSpec":
    """
    Decorator to allow you to add Python decorators to a FlowSpec that makes use of
    user configurations.

    As an example:

    ```
    def parameterize(f):
        for s in f:
            # Iterate over all the steps
            if s.name in f.config.add_env_to_steps:
                setattr(f, s.name) = environment(vars={**f.config.env_vars})(s)
        return f

    @eval_config(parameterize)
    class MyFlow(FlowSpec):
        config = Config("config")
        ...
    ```

    allows you to add an environment decorator to all steps in `add_env_to_steps`. Both
    the steps to add this decorator to and the values to add are extracted from the
    configuration passed to the Flow through config.

    Parameters
    ----------
    f : Callable[[FlowSpec], FlowSpec]
        Decorator function

    Returns
    -------
    FlowSpec
        The modified FlowSpec
    """

    def _wrapper(flow_spec: "FlowSpec"):
        flow_spec._config_funcs.append(f)
        return flow_spec

    return _wrapper


class Config(Parameter):
    """
    Includes a configuration for this flow.

    `Config` is a special type of `Parameter` but differs in a few key areas:
      - it is immutable and determined at deploy time (or prior to running if not deploying
        to a scheduler)
      - as such, it can be used anywhere in your code including in Metaflow decorators


    Parameters
    ----------
    name : str
        User-visible configuration name.
    default : Union[str, Dict[str, Any], Callable[[ParameterContext], Union[str, Dict[str, Any]]]], optional, default None
        Default value for the parameter. A function
        implies that the value will be computed using that function.
    help : str, optional, default None
        Help text to show in `run --help`.
    required : bool, default False
        Require that the user specified a value for the parameter.
        `required=True` implies that the `default` is not used.
    parser : Callable[[str], Dict[str, Any]], optional, default None
    show_default : bool, default True
        If True, show the default value in the help text.
    """

    IS_FLOW_PARAMETER = True

    def __init__(
        self,
        name: str,
        default: Optional[
            Union[
                str,
                Dict[str, Any],
                Callable[[ParameterContext], Union[str, Dict[str, Any]]],
            ]
        ] = None,
        help: Optional[str] = None,
        required: bool = False,
        parser: Optional[Callable[[str], Dict[str, Any]]] = None,
        **kwargs: Dict[str, str]
    ):

        print("Config %s, default is %s" % (name, default))
        super(Config, self).__init__(
            name, default=default, required=required, help=help, type=str, **kwargs
        )

        if isinstance(kwargs.get("default", None), str):
            kwargs["default"] = json.dumps(kwargs["default"])
        self.parser = parser

    def load_parameter(self, v):
        return v

    def __getattr__(self, name):
        ev = DelayEvaluator(self.name)
        return ev.__getattr__(name)


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
    config_opt_required = False
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
            if save_default is None:
                # We need at least one option if we have a required configuration.
                config_opt_required = True
        defaults[arg.name.lower()] = save_default
        help_strs.append("  - %s: %s" % (arg.name.lower(), kwargs.get("help", "")))
        parsers[arg.name.lower()] = arg.parser

    print(
        "DEFAULTS %s"
        % str(dict((k, v if not callable(v) else "FUNC") for k, v in defaults.items()))
    )
    if not config_seen:
        # No configurations -- don't add anything
        return cmd

    help_str = (
        "Configuration options for the flow. "
        "Multiple configurations can be specified."
    )
    help_str = "\n\n".join([help_str] + help_strs)
    cmd.params.insert(
        0,
        click.Option(
            ["--config", "config_options"],
            nargs=2,
            multiple=True,
            type=click.Tuple([click.Choice(config_seen), PathOrStr()]),
            callback=ConfigInput(required_names, defaults, parsers).process_configs,
            help=help_str,
            envvar="METAFLOW_FLOW_CONFIG",
            show_default=False,
            default=[(k, v if not callable(v) else None) for k, v in defaults.items()],
            required=config_opt_required,
        ),
    )
    return cmd
