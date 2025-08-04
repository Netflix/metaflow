import inspect
import json
import collections.abc
import copy
import os
import re

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)


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

ID_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

UNPACK_KEY = "_unpacked_delayed_"


def dump_config_values(flow: "FlowSpec"):
    from ..flowspec import _FlowState  # Prevent circular import

    configs = flow._flow_state.get(_FlowState.CONFIGS)
    if configs:
        return {"user_configs": configs}
    return {}


class ConfigValue(collections.abc.Mapping, dict):
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

    # We inherit from dict to allow the isinstanceof check to work easily and also
    # to provide a simple json dumps functionality.

    def __init__(self, data: Union["ConfigValue", Dict[str, Any]]):
        self._data = {k: self._construct(v) for k, v in data.items()}

        # Enable json dumps
        dict.__init__(self, self._data)

    @classmethod
    def fromkeys(cls, iterable: Iterable, value: Any = None) -> "ConfigValue":
        """
        Creates a new ConfigValue object from the given iterable and value.

        Parameters
        ----------
        iterable : Iterable
            Iterable to create the ConfigValue from.
        value : Any, optional
            Value to set for each key in the iterable.

        Returns
        -------
        ConfigValue
            A new ConfigValue object.
        """
        return cls(dict.fromkeys(iterable, value))

    def to_dict(self) -> Dict[Any, Any]:
        """
        Returns a dictionary representation of this configuration object.

        Returns
        -------
        Dict[Any, Any]
            Dictionary equivalent of this configuration object.
        """
        return self._to_dict(self._data)

    def copy(self) -> "ConfigValue":
        return self.__copy__()

    def clear(self) -> None:
        # Prevent configuration modification
        raise TypeError("ConfigValue is immutable")

    def update(self, *args, **kwargs) -> None:
        # Prevent configuration modification
        raise TypeError("ConfigValue is immutable")

    def setdefault(self, key: Any, default: Any = None) -> Any:
        # Prevent configuration modification
        raise TypeError("ConfigValue is immutable")

    def pop(self, key: Any, default: Any = None) -> Any:
        # Prevent configuration modification
        raise TypeError("ConfigValue is immutable")

    def popitem(self) -> Tuple[Any, Any]:
        # Prevent configuration modification
        raise TypeError("ConfigValue is immutable")

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
        return self._data[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        # Prevent configuration modification
        raise TypeError("ConfigValue is immutable")

    def __delattr__(self, key) -> None:
        # Prevent configuration modification
        raise TypeError("ConfigValue is immutable")

    def __delitem__(self, key: Any) -> None:
        # Prevent configuration modification
        raise TypeError("ConfigValue is immutable")

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ConfigValue):
            return self._data == other._data
        if isinstance(other, dict):
            return self._data == other
        return False

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __copy__(self) -> "ConfigValue":
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update({k: copy.copy(v) for k, v in self.__dict__.items()})
        return result

    def __repr__(self) -> str:
        return repr(self._data)

    def __str__(self) -> str:
        return str(self._data)

    def __dir__(self) -> Iterable[str]:
        return dir(type(self)) + [k for k in self._data.keys() if ID_PATTERN.match(k)]

    def __contains__(self, key: Any) -> bool:
        try:
            self[key]
        except KeyError:
            return False
        return True

    def keys(self):
        """
        Returns the keys of this configuration object.

        Returns
        -------
        Any
            Keys of this configuration object.
        """
        return self._data.keys()

    @classmethod
    def _construct(cls, obj: Any) -> Any:
        # Internal method to construct a ConfigValue so that all mappings internally
        # are also converted to ConfigValue
        if isinstance(obj, ConfigValue):
            v = obj
        elif isinstance(obj, collections.abc.Mapping):
            v = ConfigValue({k: cls._construct(v) for k, v in obj.items()})
        elif isinstance(obj, (list, tuple, set)):
            v = type(obj)([cls._construct(x) for x in obj])
        else:
            v = obj
        return v

    @classmethod
    def _to_dict(cls, obj: Any) -> Any:
        # Internal method to convert all nested mappings to dicts
        if isinstance(obj, collections.abc.Mapping):
            v = {k: cls._to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple, set)):
            v = type(obj)([cls._to_dict(x) for x in obj])
        else:
            v = obj
        return v


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

    def __init__(self, ex: str, saved_globals: Optional[Dict[str, Any]] = None):
        self._config_expr = ex
        self._globals = saved_globals
        if ID_PATTERN.match(self._config_expr):
            # This is a variable only so allow things like config_expr("config").var
            self._is_var_only = True
            self._access = []
        else:
            self._is_var_only = False
            self._access = None
        self._cached_expr = None

    def __copy__(self):
        c = DelayEvaluator(self._config_expr)
        c._access = self._access.copy() if self._access is not None else None
        # Globals are not copied -- always kept as a reference
        return c

    def __deepcopy__(self, memo):
        c = DelayEvaluator(self._config_expr)
        c._access = (
            copy.deepcopy(self._access, memo) if self._access is not None else None
        )
        # Globals are not copied -- always kept as a reference
        return c

    def __iter__(self):
        yield "%s%d" % (UNPACK_KEY, id(self))

    def __getitem__(self, key):
        if isinstance(key, str) and key == "%s%d" % (UNPACK_KEY, id(self)):
            return self
        if self._access is None:
            raise KeyError(key)

        # Make a copy so that we can support something like
        # foo = delay_evaluator["blah"]
        # bar = delay_evaluator["baz"]
        # and don't end up with a access list that contains both "blah" and "baz"
        c = self.__copy__()
        c._access.append(key)
        c._cached_expr = None
        return c

    def __len__(self):
        return 1

    def __getattr__(self, name):
        if self._access is None:
            raise AttributeError(name)
        c = self.__copy__()
        c._access.append(name)
        c._cached_expr = None
        return c

    def __call__(self, ctx=None, deploy_time=False):
        from ..flowspec import _FlowState  # Prevent circular import

        # Two additional arguments are only used by DeployTimeField which will call
        # this function with those two additional arguments. They are ignored.
        flow_cls = getattr(current_flow, "flow_cls", None)
        if flow_cls is None:
            # We are not executing inside a flow (ie: not the CLI)
            raise MetaflowException(
                "Config object can only be used directly in the FlowSpec defining them "
                "(or their flow decorators)."
            )
        if self._cached_expr is not None:
            to_eval_expr = self._cached_expr
        elif self._access is not None:
            # Build the final expression by adding all the fields in access as . fields
            access_list = [self._config_expr]
            for a in self._access:
                if isinstance(a, str):
                    access_list.append(a)
                elif isinstance(a, DelayEvaluator):
                    # Supports things like config[other_config.selector].var
                    access_list.append(a())
                else:
                    raise MetaflowException(
                        "Field '%s' of type '%s' is not supported" % (str(a), type(a))
                    )
            to_eval_expr = self._cached_expr = ".".join(access_list)
        else:
            to_eval_expr = self._cached_expr = self._config_expr
        # Evaluate the expression setting the config values as local variables
        try:
            return eval(
                to_eval_expr,
                self._globals or globals(),
                {
                    k: ConfigValue(v)
                    for k, v in flow_cls._flow_state.get(_FlowState.CONFIGS, {}).items()
                },
            )
        except NameError as e:
            raise MetaflowException(
                "Config expression '%s' could not be evaluated: %s"
                % (to_eval_expr, str(e))
            ) from e


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
    # Get globals where the expression is defined so that the user can use
    # something like `config_expr("my_func()")` in the expression.
    parent_globals = inspect.currentframe().f_back.f_globals
    return DelayEvaluator(expr, saved_globals=parent_globals)


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
        You can only specify default or default_value, not both.
    default_value : Union[str, Dict[str, Any], Callable[[ParameterContext, Union[str, Dict[str, Any]]], Any], optional, default None
        Default value for the parameter. A function
        implies that the value will be computed using that function.
        You can only specify default or default_value, not both.
    help : str, optional, default None
        Help text to show in `run --help`.
    required : bool, optional, default None
        Require that the user specifies a value for the configuration. Note that if
        a default or default_value is provided, the required flag is ignored.
        A value of None is equivalent to False.
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

        if default is not None and default_value is not None:
            raise MetaflowException(
                "For config '%s', you can only specify default or default_value, not both"
                % name
            )
        self._default_is_file = default is not None
        kwargs["default"] = default if default is not None else default_value
        super(Config, self).__init__(
            name, required=required, help=help, type=str, **kwargs
        )
        super(Config, self).init()

        if isinstance(kwargs.get("default", None), str):
            kwargs["default"] = json.dumps(kwargs["default"])
        self.parser = parser
        self._computed_value = None

        self._delayed_evaluator = None

    def load_parameter(self, v):
        if v is None:
            return None
        return ConfigValue(v)

    def _store_value(self, v: Any) -> None:
        self._computed_value = v

    def _init_delayed_evaluator(self) -> None:
        if self._delayed_evaluator is None:
            self._delayed_evaluator = DelayEvaluator(self.name.lower())

    # Support <config>.<var> syntax
    def __getattr__(self, name):
        # Need to return a new DelayEvaluator everytime because the evaluator will
        # contain the "path" (ie: .name) and can be further accessed.
        return getattr(DelayEvaluator(self.name.lower()), name)

    # Next three methods are to implement mapping to support **<config> syntax. We
    # need to be careful, however, to also support a regular `config["key"]` syntax
    # which calls into `__getitem__` and therefore behaves like __getattr__ above.
    def __iter__(self):
        self._init_delayed_evaluator()
        yield from self._delayed_evaluator

    def __len__(self):
        self._init_delayed_evaluator()
        return len(self._delayed_evaluator)

    def __getitem__(self, key):
        self._init_delayed_evaluator()
        if isinstance(key, str) and key.startswith(UNPACK_KEY):
            return self._delayed_evaluator[key]
        return DelayEvaluator(self.name.lower())[key]


def resolve_delayed_evaluator(
    v: Any, ignore_errors: bool = False, to_dict: bool = False
) -> Any:
    # NOTE: We don't ignore errors in downstream calls because we want to have either
    # all or nothing for the top-level call by the user.
    try:
        if isinstance(v, DelayEvaluator):
            to_return = v()
            if to_dict and isinstance(to_return, ConfigValue):
                to_return = to_return.to_dict()
            return to_return
        if isinstance(v, dict):
            return {
                resolve_delayed_evaluator(
                    k, to_dict=to_dict
                ): resolve_delayed_evaluator(v, to_dict=to_dict)
                for k, v in v.items()
            }
        if isinstance(v, list):
            return [resolve_delayed_evaluator(x, to_dict=to_dict) for x in v]
        if isinstance(v, tuple):
            return tuple(resolve_delayed_evaluator(x, to_dict=to_dict) for x in v)
        if isinstance(v, set):
            return {resolve_delayed_evaluator(x, to_dict=to_dict) for x in v}
        return v
    except Exception as e:
        if ignore_errors:
            # Assumption is that default value of None is always allowed.
            # This code path is *only* used when evaluating Parameters AND they
            # use configs in their attributes AND the runner/deployer is being used
            # AND CLICK_API_PROCESS_CONFIG is False. In those cases, all attributes in
            # Parameter can be set to None except for required and show_default
            # and even in those cases, a wrong value will have very limited consequence.
            return None
        raise e


def unpack_delayed_evaluator(
    to_unpack: Dict[str, Any], ignore_errors: bool = False
) -> Tuple[Dict[str, Any], List[str]]:
    result = {}
    new_keys = []
    for k, v in to_unpack.items():
        if not isinstance(k, str) or not k.startswith(UNPACK_KEY):
            result[k] = v
        else:
            # k.startswith(UNPACK_KEY)
            try:
                new_vals = resolve_delayed_evaluator(v, to_dict=True)
                new_keys.extend(new_vals.keys())
                result.update(new_vals)
            except Exception as e:
                if ignore_errors:
                    continue
                raise e
    return result, new_keys
