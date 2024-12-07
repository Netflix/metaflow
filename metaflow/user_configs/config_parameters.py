import collections.abc
import json
import os
import re

from typing import Any, Callable, Dict, Optional, TYPE_CHECKING, Union


from ..exception import MetaflowException

from ..parameters import (
    Parameter,
    ParameterContext,
    current_flow,
)

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

CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "CONFIG_PARAMETERS"
)

ID_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

UNPACK_KEY = "_unpacked_delayed_"


def dump_config_values(flow: "FlowSpec"):
    from ..flowspec import _FlowState  # Prevent circular import

    configs = flow._flow_state.get(_FlowState.CONFIGS)
    if configs:
        return {"user_configs": configs}
    return {}


class ConfigValue(collections.abc.Mapping):
    """
    ConfigValue is a thin wrapper around an arbitrarily nested dictionary-like
    configuration object. It allows you to access elements of this nested structure
    using either a "." notation or a [] notation. As an example, if your configuration
    object is:
    {"foo": {"bar": 42}}
    you can access the value 42 using either config["foo"]["bar"] or config.foo.bar.

    All "keys"" need to be valid Python identifiers
    """

    # Thin wrapper to allow configuration values to be accessed using a "." notation
    # as well as a [] notation.

    def __init__(self, data: Dict[str, Any]):
        if any(not ID_PATTERN.match(k) for k in data.keys()):
            raise MetaflowException(
                "All keys in the configuration must be valid Python identifiers"
            )
        self._data = data

    def __getattr__(self, key: str) -> Any:
        """
        Access an element of this configuration

        Parameters
        ----------
        key : str
            Element to access

        Returns
        -------
        Any
            Element of the configuration
        """
        if key == "_data":
            # Called during unpickling. Special case to not run into infinite loop
            # below.
            raise AttributeError(key)

        if key in self._data:
            return self[key]
        raise AttributeError(key)

    def __setattr__(self, name: str, value: Any) -> None:
        # Prevent configuration modification
        if name == "_data":
            return super().__setattr__(name, value)
        raise TypeError("ConfigValue is immutable")

    def __getitem__(self, key: Any) -> Any:
        """
        Access an element of this configuration

        Parameters
        ----------
        key : Any
            Element to access

        Returns
        -------
        Any
            Element of the configuration
        """
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

    def to_dict(self) -> Dict[Any, Any]:
        """
        Returns a dictionary representation of this configuration object.

        Returns
        -------
        Dict[Any, Any]
            Dictionary equivalent of this configuration object.
        """
        return dict(self._data)


class DelayEvaluator(collections.abc.Mapping):
    """
    Small wrapper that allows the evaluation of a Config() value in a delayed manner.
    This is used when we want to use config.* values in decorators for example.

    It also allows the following "delayed" access on an obj that is a DelayEvaluation
      - obj.x.y.z (ie: accessing members of DelayEvaluator; accesses will be delayed until
        the DelayEvaluator is evaluated)
      - **obj (ie: unpacking the DelayEvaluator as a dictionary). Note that this requires
        special handling in whatever this is being unpacked into, specifically the handling
        of _unpacked_delayed_*
    """

    def __init__(self, ex: str):
        self._config_expr = ex
        if ID_PATTERN.match(self._config_expr):
            # This is a variable only so allow things like config_expr("config").var
            self._is_var_only = True
            self._access = []
        else:
            self._is_var_only = False
            self._access = None

    def __iter__(self):
        yield "%s%d" % (UNPACK_KEY, id(self))

    def __getitem__(self, key):
        if key == "%s%d" % (UNPACK_KEY, id(self)):
            return self
        raise KeyError(key)

    def __len__(self):
        return 1

    def __getattr__(self, name):
        if self._access is None:
            raise AttributeError()
        self._access.append(name)
        return self

    def __call__(self, ctx=None, deploy_time=False):
        from ..flowspec import _FlowState  # Prevent circular import

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
        try:
            return eval(
                self._config_expr,
                globals(),
                {
                    k: ConfigValue(v)
                    for k, v in flow_cls._flow_state.get(_FlowState.CONFIGS, {}).items()
                },
            )
        except NameError as e:
            potential_config_name = self._config_expr.split(".")[0]
            if potential_config_name not in flow_cls._flow_state.get(
                _FlowState.CONFIGS, {}
            ):
                raise MetaflowException(
                    "Config '%s' not found in the flow (maybe not required and not "
                    "provided?)" % potential_config_name
                ) from e
            raise


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


class Config(Parameter, collections.abc.Mapping):
    """
    Includes a configuration for this flow.

    `Config` is a special type of `Parameter` but differs in a few key areas:
      - it is immutable and determined at deploy time (or prior to running if not deploying
        to a scheduler)
      - as such, it can be used anywhere in your code including in Metaflow decorators

    The value of the configuration is determines as follows:
      - use the user-provided file path or value. It is an error to provide both
      - if none are present:
        - if a default file path (default) is provided, attempt to read this file
            - if the file is present, use that value. Note that the file will be used
              even if it has an invalid syntax
            - if the file is not present, and a default value is present, use that
      - if still None and is required, this is an error.

    Parameters
    ----------
    name : str
        User-visible configuration name.
    default : Union[str, Callable[[ParameterContext], str], optional, default None
        Default path from where to read this configuration. A function implies that the
        value will be computed using that function.
        You can only specify default or default_value.
    default_value : Union[str, Dict[str, Any], Callable[[ParameterContext, Union[str, Dict[str, Any]]], Any], optional, default None
        Default value for the parameter. A function
        implies that the value will be computed using that function.
        You can only specify default or default_value.
    help : str, optional, default None
        Help text to show in `run --help`.
    required : bool, optional, default None
        Require that the user specified a value for the configuration. Note that if
        a default is provided, the required flag is ignored. A value of None is
        equivalent to False.
    parser : Union[str, Callable[[str], Dict[Any, Any]]], optional, default None
        If a callable, it is a function that can parse the configuration string
        into an arbitrarily nested dictionary. If a string, the string should refer to
        a function (like "my_parser_package.my_parser.my_parser_function") which should
        be able to parse the configuration string into an arbitrarily nested dictionary.
        If the name starts with a ".", it is assumed to be relative to "metaflow".
    show_default : bool, default True
        If True, show the default value in the help text.
    """

    IS_CONFIG_PARAMETER = True

    def __init__(
        self,
        name: str,
        default: Optional[Union[str, Callable[[ParameterContext], str]]] = None,
        default_value: Optional[
            Union[
                str,
                Dict[str, Any],
                Callable[[ParameterContext], Union[str, Dict[str, Any]]],
            ]
        ] = None,
        help: Optional[str] = None,
        required: Optional[bool] = None,
        parser: Optional[Union[str, Callable[[str], Dict[Any, Any]]]] = None,
        **kwargs: Dict[str, str]
    ):

        if default and default_value:
            raise MetaflowException(
                "For config '%s', you can only specify default or default_value, not both"
                % name
            )
        self._default_is_file = default is not None
        kwargs["default"] = default or default_value
        super(Config, self).__init__(
            name, required=required, help=help, type=str, **kwargs
        )
        super(Config, self).init()

        if isinstance(kwargs.get("default", None), str):
            kwargs["default"] = json.dumps(kwargs["default"])
        self.parser = parser
        self._computed_value = None

    def load_parameter(self, v):
        return v

    def _store_value(self, v: Any) -> None:
        self._computed_value = v

    # Support <config>.<var> syntax
    def __getattr__(self, name):
        return DelayEvaluator(self.name.lower()).__getattr__(name)

    # Next three methods are to implement mapping to support **<config> syntax
    def __iter__(self):
        return iter(DelayEvaluator(self.name.lower()))

    def __len__(self):
        return len(DelayEvaluator(self.name.lower()))

    def __getitem__(self, key):
        return DelayEvaluator(self.name.lower())[key]


def resolve_delayed_evaluator(v: Any) -> Any:
    if isinstance(v, DelayEvaluator):
        return v()
    if isinstance(v, dict):
        return {
            resolve_delayed_evaluator(k): resolve_delayed_evaluator(v)
            for k, v in v.items()
        }
    if isinstance(v, list):
        return [resolve_delayed_evaluator(x) for x in v]
    if isinstance(v, tuple):
        return tuple(resolve_delayed_evaluator(x) for x in v)
    if isinstance(v, set):
        return {resolve_delayed_evaluator(x) for x in v}
    return v


def unpack_delayed_evaluator(to_unpack: Dict[str, Any]) -> Dict[str, Any]:
    result = {}
    for k, v in to_unpack.items():
        if not isinstance(k, str) or not k.startswith(UNPACK_KEY):
            result[k] = v
        else:
            # k.startswith(UNPACK_KEY)
            result.update(resolve_delayed_evaluator(v[k]))
    return result
