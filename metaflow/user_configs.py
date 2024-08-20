import json
import os

from typing import Any, Callable, Dict, Optional, Union, TYPE_CHECKING

from metaflow import INFO_FILE
from metaflow._vendor import click

from .exception import MetaflowException, MetaflowInternalError
from .parameters import (
    DelayedEvaluationParameter,
    Parameter,
    ParameterContext,
    current_flow,
)
import functools

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


class ConfigValue:
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

    def __repr__(self):
        return repr(self._data)

    def __str__(self):
        return json.dumps(self._data)


class ConfigInput(click.ParamType):
    name = "ConfigInput"

    # Contains the values loaded from the INFO file. We make this a class method
    # so that if there are multiple configs, we just need to read the file once.
    # It is OK to be globally unique because this is only evoked in scenario A.2 (see
    # convert method) which means we are already just executing a single task and so
    # there is no concern about it "leaking" to things running with Runner for example
    # (ie: even if Runner is evoked in that task, we won't "share" this global value's
    # usage).
    loaded_configs = None  # type: Optional[Dict[str, Dict[str, Any]]]
    info_file = None  # type: Optional[str]

    def __init__(self, parser: Optional[Callable[[str], Dict[str, Any]]] = None):
        self._parser = parser

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

    # @tracefunc
    def convert(self, value, param, ctx):
        flow_cls = getattr(current_flow, "flow_cls", None)
        if flow_cls is None:
            # We are not executing inside a flow (ie: not the CLI)
            raise MetaflowException("Config object can only be used in a FlowSpec")

        # Click can call convert multiple times, so we need to make sure to only
        # convert once.
        if isinstance(value, ConfigValue):
            return value

        # The value we get in to convert can be:
        #  - a dictionary
        #  - a path to a YAML or JSON file
        #  - the string representation of a YAML or JSON file
        # In all cases, we also store the configuration in the flow object under _user_configs.
        # It will *not* be stored as an artifact but is a good place to store it so we
        # can access it when packaging to store it in the INFO file. The config itself
        # will be stored as regular artifacts (the ConfigValue object basically)

        if isinstance(value, dict):
            if self._parser:
                value = self._parser(value)
            flow_cls._user_configs[param.name] = value
            return ConfigValue(value)
        elif not isinstance(value, str):
            raise MetaflowException(
                "Configuration value for '%s' must be a string or a dictionary"
                % param.name
            )

        # Here we are sure we have a string
        if value.startswith("kv."):
            value = self.get_config(value[3:])
            if value is None:
                raise MetaflowException(
                    "Could not find configuration '%s' in INFO file" % value
                )
            # We also set in flow_cls as this makes it easier to access
            flow_cls._user_configs[param.name] = value
            return ConfigValue(value)

        elif os.path.isfile(value):
            try:
                with open(value, "r") as f:
                    content = f.read()
            except OSError as e:
                raise MetaflowException(
                    "Could not read configuration file '%s'" % value
                ) from e
            if self._parser:
                value = self._parser(content)
            else:
                try:
                    if self._parser:
                        value = self._parser(content)

                    value = json.loads(content)
                except json.JSONDecodeError as e:
                    raise MetaflowException(
                        "Configuration file '%s' is not valid JSON" % value
                    ) from e
                # TODO: Support YAML
            flow_cls._user_configs[param.name] = value
        else:
            if self._parser:
                value = self._parser(value)
            else:
                try:
                    value = json.loads(value)
                except json.JSONDecodeError as e:
                    raise MetaflowException(
                        "Configuration value for '%s' is not valid JSON" % param.name
                    ) from e
                # TODO: Support YAML
            flow_cls._user_configs[param.name] = value
        return ConfigValue(value)

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

    def __init__(self, config_expr: str, is_var_only=True):
        self._config_expr = config_expr
        if is_var_only:
            self._access = []
        else:
            self._access = None
        self._is_var_only = is_var_only

    def __getattr__(self, name):
        if self._access is None:
            raise AttributeError()
        self._access.append(name)
        return self

    def __call__(self):
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


class FlowConfig(DelayEvaluator):
    def __init__(self, config_name: str):
        """
        Small wrapper to allow you to refer to a flow's configuration in a flow-level
        decorator.

        As an example:

        @project(name=FlowConfig("config").project.name)
        class MyFlow(FlowSpec):
            config = Config("config")
            ...

        This will allow you to specify a `project.name` value in your configuration
        and have it used in the flow-level decorator.

        Without this construct, it would be difficult to access `config` inside the
        arguments of the decorator.

        Parameters
        ----------
        config_name : str
            Name of the configuration being used. This should be the name given to
            the `Config` constructor.
        """
        super().__init__(config_name, is_var_only=True)


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
        **kwargs: Dict[str, str],
    ):
        super(Config, self).__init__(
            name,
            default=default,
            required=required,
            help=help,
            type=ConfigInput(parser),
            **kwargs,
        )

    def load_parameter(self, v):
        return v

    def __getattr__(self, name):
        ev = DelayEvaluator(self.name, is_var_only=True)
        return ev.__getattr__(name)
