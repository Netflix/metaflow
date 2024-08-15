import json
import os

from typing import Any, Dict, Optional, Union, TYPE_CHECKING

from metaflow import INFO_FILE
from metaflow._vendor import click

from .exception import MetaflowException
from .parameters import (
    DelayedEvaluationParameter,
    Parameter,
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


def dump_config_values(flow: FlowSpec):
    if hasattr(flow, "_user_configs"):
        return "user_configs", flow._user_configs
    return None, None


def load_config_values() -> Optional[Dict[str, Any]]:
    try:
        with open(INFO_FILE, encoding="utf-8") as contents:
            return json.load(contents).get("user_configs", {})
    except IOError:
        return None


class ConfigValue(object):
    # Thin wrapper to allow configuration values to be accessed using a "." notation
    # as well as a [] notation.

    def __init__(self, data: Dict[str, Any]):
        self._data = data

        for key, value in data.items():
            if isinstance(value, dict):
                value = ConfigValue(value)
            elif isinstance(value, list):
                value = [ConfigValue(v) for v in value]
            setattr(self, key, value)

    def __getitem__(self, key):
        value = self._data[key]
        if isinstance(value, dict):
            value = ConfigValue(value)
        elif isinstance(value, list):
            value = [ConfigValue(v) for v in value]
        return value

    def __repr__(self):
        return repr(self._data)


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

    def __init__(self):
        self._flow_cls = getattr(current_flow, "flow_cls", None)
        if self._flow_cls is None:
            raise MetaflowException("ConfigInput can only be used inside a flow")
        if not hasattr(self._flow_cls, "_user_configs"):
            self._flow_cls._user_configs = {}

    @staticmethod
    def _make_key_name(name: str) -> str:
        return "kv." + name.lower()

    @classmethod
    def get_config(cls, config_name: str) -> Optional[Dict[str, Any]]:
        if cls.loaded_configs is None:
            all_configs = load_config_values()
            if all_configs is None:
                raise MetaflowException(
                    "Could not load expected configuration values "
                    "the INFO file. This is a Metaflow bug. Please contact support."
                )
            cls.loaded_configs = all_configs
        return cls.loaded_configs.get(config_name, None)

    def convert(self, value, param, ctx):
        # Click can call convert multiple times, so we need to make sure to only
        # convert once.
        if isinstance(value, (ConfigValue, DelayedEvaluationParameter)):
            return value

        # There are two paths we need to worry about:
        #  - Scenario A: deploying to a scheduler
        #    A.1 In this case, when deploying (using `step-functions create` for example),
        #    the value passed to click (or the default value) will be converted and we
        #    will:
        #      - store the configuration in the flow object under _user_configs (so that it
        #        can later be dumped to the INFO file when packaging)
        #     - return a DelayedEvaluationParameter object so that when the scheduler
        #       evaluates it (with return_str set to True), it gets back the *string*
        #       kv.<name> which indicates that this
        #       configuration should be fetched from INFO
        #    A.2 When the scheduler runs the flow, the value returned in A.1 (the kv.<name>
        #    string) will be passed to convert again. This time, we directly return a
        #    ConfigValue after having fetched/loaded the configuration from INFO.
        #
        #  - Scenario B: running with the native Runtime
        #    The value passed in will be similarly stored under _user_configs. We also
        #    return a DelayedEvaluationParameter object but when the _set_constants in
        #    the runtime calls it, it calls it with return_str set to False and it will
        #    return a ConfigValue directly which can then be persisted in the artifact
        #    store.

        # The value we get in to convert can be:
        #  - a dictionary
        #  - a path to a YAML or JSON file
        #  - the string representation of a YAML or JSON file
        # In all cases, we also store the configuration in the flow object under _user_configs.
        # It will *not* be stored as an artifact but is a good place to store it so we
        # can access it when packaging to store it in the INFO file. The config itself
        # will be stored as regular artifacts (the ConfigValue object basically)

        def _delay_eval(name: str, value: ConfigValue, return_str=False):
            if return_str:
                # Scenario A.1 when deploy_time_eval is called by the scheduler
                # (or, in some cases, some schedulers directly identify the
                # DelayedEvaluationParameter value and call it directory with
                # return_str=True)
                return name
            # Scenario B
            return value

        if isinstance(value, dict):
            # Scenario A.1 or B.
            self._flow_cls._user_configs[self._make_key_name(param.name)] = value
            return DelayedEvaluationParameter(
                param.name, "value", functools.partial(_delay_eval, param.name, value)
            )
        elif not isinstance(value, str):
            raise MetaflowException(
                "Configuration value for '%s' must be a string or a dictionary"
                % param.name
            )

        # Here we are sure we have a string
        if value.startswith("kv."):
            # This is scenario A.2
            value = self.get_config(value)
            if value is None:
                raise MetaflowException(
                    "Could not find configuration '%s' in INFO file" % value
                )
            return ConfigValue(value)

        elif os.path.isfile(value):
            try:
                with open(value, "r") as f:
                    content = f.read()
            except OSError as e:
                raise MetaflowException(
                    "Could not read configuration file '%s'" % value
                ) from e
            try:
                value = json.loads(content)
            except json.JSONDecodeError as e:
                raise MetaflowException(
                    "Configuration file '%s' is not valid JSON" % value
                ) from e
            # TODO: Support YAML
            self._flow_cls._user_configs[self._make_key_name(param.name)] = value
        else:
            try:
                value = json.loads(value)
            except json.JSONDecodeError as e:
                raise MetaflowException(
                    "Configuration value for '%s' is not valid JSON" % param.name
                ) from e
            # TODO: Support YAML
            self._flow_cls._user_configs[self._make_key_name(param.name)] = value
        return DelayedEvaluationParameter(
            param.name, "value", functools.partial(_delay_eval, param.name, value)
        )

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "ConfigInput"


ConfigArgType = Union[str, Dict[str, Any]]


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
        User-visible parameter name.
    default : Union[ConfigArgType, Callable[[ParameterContext], ConfigArgType]]
        Default configuration either as a path to a file, the string representation of
        a YAML or JSON file or a dictionary. If specified as a function, the function
        will be evaluated to get the value to use.
    required : bool, default False
        Require that the user specified a value for the parameter.
        `required=True` implies that the `default` value is ignored.
    help : str, optional
        Help text to show in `run --help`.
    show_default : bool, default True
        If True, show the default value in the help text.
    """

    def __init__(
        self,
        name: str,
        required: bool = False,
        help: Optional[str] = None,
        **kwargs: Dict[str, str]
    ):
        super(Config, self).__init__(
            name,
            required=required,
            help=help,
            type=ConfigInput(),
            **kwargs,
        )

    def load_parameter(self, v):
        return v
