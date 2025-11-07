import os
import sys

_py_ver = sys.version_info[:2]

if _py_ver >= (3, 8):
    from metaflow._vendor.typeguard import TypeCheckError, check_type
elif _py_ver >= (3, 7):
    from metaflow._vendor.v3_7.typeguard import TypeCheckError, check_type
else:
    raise RuntimeError(
        """
        The Metaflow Programmatic API is not supported for versions of Python less than 3.7
    """
    )

import datetime
import functools
import importlib
import inspect
import itertools
import uuid
import json
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Type
from typing import OrderedDict as TOrderedDict
from typing import Tuple as TTuple
from typing import Union

from metaflow import FlowSpec, Parameter
from metaflow._vendor import click
from metaflow._vendor.click.types import (
    BoolParamType,
    Choice,
    DateTime,
    File,
    FloatParamType,
    IntParamType,
    Path,
    StringParamType,
    Tuple,
    UUIDParameterType,
)
from metaflow.decorators import add_decorator_options
from metaflow.exception import MetaflowException
from metaflow.flowspec import _FlowState
from metaflow.includefile import FilePathClass
from metaflow.metaflow_config import CLICK_API_PROCESS_CONFIG
from metaflow.parameters import JSONTypeClass, flow_context
from metaflow.user_configs.config_options import (
    ConfigValue,
    ConvertDictOrStr,
    ConvertPath,
    LocalFileInput,
    MultipleTuple,
    config_options_with_config_input,
)
from metaflow.user_decorators.user_flow_decorator import FlowMutator

# Define a recursive type alias for JSON
JSON = Union[Dict[str, "JSON"], List["JSON"], str, int, float, bool, None]

click_to_python_types = {
    StringParamType: str,
    IntParamType: int,
    FloatParamType: float,
    BoolParamType: bool,
    UUIDParameterType: uuid.UUID,
    Path: str,
    DateTime: datetime.datetime,
    Tuple: tuple,
    Choice: str,
    File: str,
    JSONTypeClass: JSON,
    FilePathClass: str,
    LocalFileInput: str,
    MultipleTuple: TTuple[str, Union[JSON, ConfigValue]],
}


def _method_sanity_check(
    possible_arg_params: TOrderedDict[str, click.Argument],
    possible_opt_params: TOrderedDict[str, click.Option],
    annotations: TOrderedDict[str, Any],
    defaults: TOrderedDict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    method_params = {"args": {}, "options": {}, "defaults": defaults}

    possible_params = OrderedDict()
    possible_params.update(possible_arg_params)
    possible_params.update(possible_opt_params)

    # supplied kwargs
    for supplied_k, supplied_v in kwargs.items():
        if supplied_k not in possible_params:
            raise ValueError(
                "Unknown argument: '%s', possible args are: %s"
                % (supplied_k, ", ".join(possible_params.keys()))
            )

        try:
            check_type(supplied_v, annotations[supplied_k])
        except TypeCheckError:
            raise TypeError(
                "Invalid type for '%s' (%s), expected: '%s', default is '%s' but found '%s'"
                % (
                    supplied_k,
                    type(supplied_k),
                    annotations[supplied_k],
                    defaults[supplied_k],
                    str(supplied_v),
                )
            )

        # Clean up values to make them into what click expects
        if annotations[supplied_k] == JSON:
            # JSON should be a string (json dumps)
            supplied_v = json.dumps(supplied_v)
        elif supplied_k == "config_value":
            # Special handling of config value because we need to go look in the tuple
            new_list = []
            for cfg_name, cfg_value in supplied_v:
                if isinstance(cfg_value, ConfigValue):
                    # ConfigValue should be JSONified and converted to a string
                    new_list.append((cfg_name, json.dumps(cfg_value.to_dict())))
                elif isinstance(cfg_value, dict):
                    # ConfigValue passed as a dictionary
                    new_list.append((cfg_name, json.dumps(cfg_value)))
                else:
                    raise TypeError(
                        "Invalid type for a config-value, expected a ConfigValue or "
                        "dict but got '%s'" % type(cfg_value)
                    )
            supplied_v = new_list

        if supplied_k in possible_arg_params:
            cli_name = possible_arg_params[supplied_k].opts[0].strip("-")
            method_params["args"][cli_name] = supplied_v
        elif supplied_k in possible_opt_params:
            if possible_opt_params[supplied_k].is_bool_flag:
                # it is a boolean flag..
                if supplied_v == True:
                    cli_name = possible_opt_params[supplied_k].opts[0].strip("-")
                elif supplied_v == False:
                    if possible_opt_params[supplied_k].secondary_opts:
                        cli_name = (
                            possible_opt_params[supplied_k].secondary_opts[0].strip("-")
                        )
                    else:
                        continue
                supplied_v = "flag"
            else:
                cli_name = possible_opt_params[supplied_k].opts[0].strip("-")
            method_params["options"][cli_name] = supplied_v

    # possible kwargs
    for _, possible_v in possible_params.items():
        cli_name = possible_v.opts[0].strip("-")
        if (
            (cli_name not in method_params["args"])
            and (cli_name not in method_params["options"])
        ) and possible_v.required:
            raise ValueError("Missing argument: %s is required." % cli_name)

    return method_params


def _cleanup_flow_parameters(cmd_obj: Union[click.Command, click.Group]):
    if hasattr(cmd_obj, "original_params"):
        cmd_obj.params = list(cmd_obj.original_params)

    if isinstance(cmd_obj, click.Group):
        for sub_cmd_name in cmd_obj.list_commands(None):
            sub_cmd = cmd_obj.get_command(None, sub_cmd_name)
            if sub_cmd:
                _cleanup_flow_parameters(sub_cmd)


def _lazy_load_command(
    cli_collection: click.Group,
    flow_parameters: Union[str, List[Parameter]],
    _self,
    name: str,
):
    # Context is not used in get_command so we can pass None. Since we pin click,
    # this won't change from under us.

    if isinstance(flow_parameters, str):
        # Resolve flow_parameters -- for start, this is a function which we
        # need to call to figure out the actual parameters (may be changed by configs)
        flow_parameters = getattr(_self, flow_parameters)()
    cmd_obj = cli_collection.get_command(None, name)
    if cmd_obj:
        _cleanup_flow_parameters(cmd_obj)
        if isinstance(cmd_obj, click.Group):
            # TODO: possibly check for fake groups with cmd_obj.name in ["cli", "main"]
            result = functools.partial(extract_group(cmd_obj, flow_parameters), _self)
        elif isinstance(cmd_obj, click.Command):
            result = functools.partial(extract_command(cmd_obj, flow_parameters), _self)
        else:
            raise RuntimeError(
                "Cannot handle %s of type %s" % (cmd_obj.name, type(cmd_obj))
            )
        setattr(_self, name, result)
        return result
    else:
        raise AttributeError()


def get_annotation(param: click.Parameter) -> TTuple[Type, bool]:
    py_type = click_to_python_types[type(param.type)]
    if param.nargs == -1:
        # This is the equivalent of *args effectively
        # so the type annotation should be the type of the
        # elements in the list
        return py_type, True
    if not param.required:
        if param.multiple or param.nargs > 1:
            return Optional[Union[List[py_type], TTuple[py_type]]], False
        else:
            return Optional[py_type], False
    else:
        if param.multiple or param.nargs > 1:
            return Union[List[py_type], TTuple[py_type]], False
        else:
            return py_type, False


def get_inspect_param_obj(p: Union[click.Argument, click.Option], kind: str):
    annotation, is_vararg = get_annotation(p)
    return (
        inspect.Parameter(
            name="args" if is_vararg else p.name,
            kind=inspect.Parameter.VAR_POSITIONAL if is_vararg else kind,
            default=inspect.Parameter.empty if is_vararg else p.default,
            annotation=annotation,
        ),
        (
            Optional[Union[TTuple[annotation], List[annotation]]]
            if is_vararg
            else annotation
        ),
    )


# Cache to store already loaded modules
loaded_modules = {}


def extract_flow_class_from_file(flow_file: str) -> FlowSpec:
    if not os.path.exists(flow_file):
        raise FileNotFoundError("Flow file not present at '%s'" % flow_file)

    flow_dir = os.path.dirname(os.path.abspath(flow_file))
    path_was_added = False

    # Only add to path if it's not already there
    if flow_dir not in sys.path:
        sys.path.insert(0, flow_dir)
        path_was_added = True

    try:
        # Get module name from the file path
        module_name = os.path.splitext(os.path.basename(flow_file))[0]

        # Check if the module has already been loaded
        if flow_file in loaded_modules:
            module = loaded_modules[flow_file]
        else:
            # Load the module if it's not already loaded
            spec = importlib.util.spec_from_file_location(module_name, flow_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            # Cache the loaded module
            loaded_modules[flow_file] = module

        classes = inspect.getmembers(
            module, lambda x: inspect.isclass(x) or isinstance(x, FlowMutator)
        )
        flow_cls = None

        for _, kls in classes:
            if isinstance(kls, FlowMutator):
                kls = kls._flow_cls
            if (
                kls is not FlowSpec
                and kls.__module__ == module_name
                and issubclass(kls, FlowSpec)
            ):
                if flow_cls is not None:
                    raise MetaflowException(
                        "Multiple FlowSpec classes found in %s" % flow_file
                    )
                flow_cls = kls

        if flow_cls is None:
            raise MetaflowException("No FlowSpec class found in %s" % flow_file)
        return flow_cls
    finally:
        # Only remove from path if we added it
        if path_was_added:
            try:
                sys.path.remove(flow_dir)
            except ValueError:
                # User's code might have removed it already
                pass


class MetaflowAPI(object):
    def __init__(self, parent=None, flow_cls=None, config_input=None, **kwargs):
        self._parent = parent
        self._chain = [{self._API_NAME: kwargs}]
        self._flow_cls = flow_cls
        self._config_input = config_input
        self._cached_computed_parameters = None

    @property
    def parent(self):
        if self._parent:
            return self._parent
        return None

    @property
    def chain(self):
        return self._chain

    @property
    def name(self):
        return self._API_NAME

    @classmethod
    def from_cli(cls, flow_file: str, cli_collection: Callable) -> Callable:
        flow_cls = extract_flow_class_from_file(flow_file)

        with flow_context(flow_cls) as _:
            cli_collection, config_input = config_options_with_config_input(
                cli_collection
            )
            cli_collection = add_decorator_options(cli_collection)

        def getattr_wrapper(_self, name):
            # Functools.partial do not automatically bind self (no __get__)
            with flow_context(flow_cls) as _:
                # We also wrap this in the proper flow context because since commands
                # are loaded lazily, we need the proper flow context to compute things
                # like parameters. If we do not do this, the outer flow's context will
                # be used.
                return _self._internal_getattr(_self, name)

        class_dict = {
            "__module__": "metaflow",
            "_API_NAME": flow_file,
            "_internal_getattr": staticmethod(
                functools.partial(
                    _lazy_load_command, cli_collection, "_compute_flow_parameters"
                )
            ),
            "__getattr__": getattr_wrapper,
        }

        to_return = type(flow_file, (MetaflowAPI,), class_dict)
        to_return.__name__ = flow_file

        (
            params_sigs,
            possible_arg_params,
            possible_opt_params,
            annotations,
            defaults,
        ) = extract_all_params(cli_collection)

        def _method(_self, *args, **kwargs):
            method_params = _method_sanity_check(
                possible_arg_params,
                possible_opt_params,
                annotations,
                defaults,
                **kwargs,
            )
            return to_return(
                parent=None,
                flow_cls=flow_cls,
                config_input=config_input,
                **method_params,
            )

        m = _method
        m.__name__ = cli_collection.name
        m.__doc__ = getattr(cli_collection, "help", None)
        m.__signature__ = inspect.signature(_method).replace(
            parameters=params_sigs.values()
        )
        m.__annotations__ = annotations
        m.__defaults__ = tuple(defaults.values())

        return m

    def execute(self) -> List[str]:
        parents = []
        current = self
        while current.parent:
            parents.append(current.parent)
            current = current.parent

        parents.reverse()

        final_chain = list(itertools.chain.from_iterable([p.chain for p in parents]))
        final_chain.extend(self.chain)

        components = []
        for each_cmd in final_chain:
            for cmd, params in each_cmd.items():
                components.append(cmd)
                args = params.pop("args", {})
                options = params.pop("options", {})

                for _, v in args.items():
                    if v is None:
                        continue
                    if isinstance(v, (list, tuple)):
                        for i in v:
                            components.append(i)
                    else:
                        components.append(v)
                for k, v in options.items():
                    if v is None:
                        continue
                    if isinstance(v, list):
                        for i in v:
                            if isinstance(i, tuple):
                                components.append("--%s" % k)
                                components.extend(map(str, i))
                            else:
                                components.append("--%s" % k)
                                components.append(str(i))
                    elif v is None:
                        continue  # Skip None values -- they are defaults and converting
                        # them to string will not be what the user wants
                    else:
                        components.append("--%s" % k)
                        if v != "flag":
                            components.append(str(v))

        return components

    def _compute_flow_parameters(self):
        if (
            self._flow_cls is None
            or self._config_input is None
            or self._parent is not None
        ):
            raise RuntimeError(
                "Computing flow-level parameters for a non start API. "
                "Please report to the Metaflow team."
            )

        if self._cached_computed_parameters is not None:
            return self._cached_computed_parameters
        self._cached_computed_parameters = []

        config_options = None
        if CLICK_API_PROCESS_CONFIG:
            with flow_context(self._flow_cls) as _:
                # We are going to resolve the configs first and then get the parameters.
                # Note that configs may update/add parameters so the order is important
                # Since part of the processing of configs happens by click, we need to
                # "fake" it.

                # Extract any config options as well as datastore and quiet options
                method_params = self._chain[0][self._API_NAME]
                opts = method_params["options"]
                defaults = method_params["defaults"]

                ds = opts.get("datastore", defaults["datastore"])
                quiet = opts.get("quiet", defaults["quiet"])

                # Order to find config or config_value:
                # 1. Passed directly to the Click API
                # 2. If not found, check if passed through an environment variable
                # 3. If not found, use the default value
                is_default = False
                config_file = opts.get("config")
                if config_file is None:
                    # Check if it was set through an environment variable -- we
                    # don't have click process them here so we need to "fake" it.
                    env_config_file = os.environ.get("METAFLOW_FLOW_CONFIG")
                    if env_config_file:
                        # Convert dict items to list of tuples
                        config_file = list(json.loads(env_config_file).items())
                        is_default = False
                    else:
                        is_default = True
                        config_file = defaults.get("config")

                if config_file:
                    config_file = dict(
                        map(
                            lambda x: (
                                x[0],
                                ConvertPath.convert_value(x[1], is_default),
                            ),
                            config_file,
                        )
                    )

                is_default = False
                config_value = opts.get("config-value")
                if config_value is None:
                    env_config_value = os.environ.get("METAFLOW_FLOW_CONFIG_VALUE")
                    if env_config_value:
                        # Parse environment variable using MultipleTuple logic
                        loaded = json.loads(env_config_value)
                        # Convert dict items to list of tuples with JSON-serialized values
                        config_value = [
                            (k, json.dumps(v) if not isinstance(v, str) else v)
                            for k, v in loaded.items()
                        ]
                        is_default = False
                    else:
                        is_default = True
                        config_value = defaults.get("config_value")

                if config_value:
                    config_value = dict(
                        map(
                            lambda x: (
                                x[0],
                                ConvertDictOrStr.convert_value(x[1], is_default),
                            ),
                            config_value,
                        )
                    )

                if (config_file is None) ^ (config_value is None):
                    # If we have one, we should have the other
                    raise MetaflowException(
                        "Options were not properly set -- this is an internal error."
                    )

                if config_file:
                    # Process both configurations; the second one will return all the merged
                    # configuration options properly processed.
                    self._config_input.process_configs(
                        self._flow_cls.__name__, "config", config_file, quiet, ds
                    )
                    config_options = self._config_input.process_configs(
                        self._flow_cls.__name__, "config_value", config_value, quiet, ds
                    )

        # At this point, we are like in start() in cli.py -- we obtained the
        # properly processed config_options which we can now use to process
        # the config decorators (including StepMutator/FlowMutator)
        # Note that if CLICK_API_PROCESS_CONFIG is False, we still do this because
        # it will init all parameters (config_options will be None)
        # We ignore any errors if we don't check the configs in the click API.

        # Init all values in the flow mutators and then process them
        for decorator in self._flow_cls._flow_state.get(_FlowState.FLOW_MUTATORS, []):
            decorator.external_init()

        new_cls = self._flow_cls._process_config_decorators(
            config_options, process_configs=CLICK_API_PROCESS_CONFIG
        )
        if new_cls:
            self._flow_cls = new_cls

        for _, param in self._flow_cls._get_parameters():
            if param.IS_CONFIG_PARAMETER:
                continue
            self._cached_computed_parameters.append(param)
        return self._cached_computed_parameters


def extract_all_params(cmd_obj: Union[click.Command, click.Group]):
    arg_params_sigs = OrderedDict()
    opt_params_sigs = OrderedDict()
    params_sigs = OrderedDict()

    arg_parameters = OrderedDict()
    opt_parameters = OrderedDict()
    annotations = OrderedDict()
    defaults = OrderedDict()

    for each_param in cmd_obj.params:
        if isinstance(each_param, click.Argument):
            (
                arg_params_sigs[each_param.name],
                annotations[each_param.name],
            ) = get_inspect_param_obj(each_param, inspect.Parameter.POSITIONAL_ONLY)
            arg_parameters[each_param.name] = each_param
        elif isinstance(each_param, click.Option):
            (
                opt_params_sigs[each_param.name],
                annotations[each_param.name],
            ) = get_inspect_param_obj(each_param, inspect.Parameter.KEYWORD_ONLY)
            opt_parameters[each_param.name] = each_param

        defaults[each_param.name] = each_param.default

    # first, fill in positional arguments
    for name, each_arg_param in arg_params_sigs.items():
        params_sigs[name] = each_arg_param
    # then, fill in keyword arguments
    for name, each_opt_param in opt_params_sigs.items():
        params_sigs[name] = each_opt_param

    return params_sigs, arg_parameters, opt_parameters, annotations, defaults


def extract_group(cmd_obj: click.Group, flow_parameters: List[Parameter]) -> Callable:
    class_dict = {"__module__": "metaflow", "_API_NAME": cmd_obj.name}
    for _, sub_cmd_obj in cmd_obj.commands.items():
        if isinstance(sub_cmd_obj, click.Group):
            # recursion
            class_dict[sub_cmd_obj.name] = extract_group(sub_cmd_obj, flow_parameters)
        elif isinstance(sub_cmd_obj, click.Command):
            class_dict[sub_cmd_obj.name] = extract_command(sub_cmd_obj, flow_parameters)
        else:
            raise RuntimeError(
                "Cannot handle %s of type %s" % (sub_cmd_obj.name, type(sub_cmd_obj))
            )

    resulting_class = type(cmd_obj.name, (MetaflowAPI,), class_dict)
    resulting_class.__name__ = cmd_obj.name

    (
        params_sigs,
        possible_arg_params,
        possible_opt_params,
        annotations,
        defaults,
    ) = extract_all_params(cmd_obj)

    def _method(_self, *args, **kwargs):
        method_params = _method_sanity_check(
            possible_arg_params, possible_opt_params, annotations, defaults, **kwargs
        )
        return resulting_class(parent=_self, flow_cls=None, **method_params)

    m = _method
    m.__name__ = cmd_obj.name
    m.__doc__ = getattr(cmd_obj, "help", None)
    m.__signature__ = inspect.signature(_method).replace(
        parameters=params_sigs.values()
    )
    m.__annotations__ = annotations
    m.__defaults__ = tuple(defaults.values())

    return m


def extract_command(
    cmd_obj: click.Command, flow_parameters: List[Parameter]
) -> Callable:
    if getattr(cmd_obj, "has_flow_params", False):
        for p in flow_parameters[::-1]:
            cmd_obj.params.insert(0, click.Option(("--" + p.name,), **p.kwargs))

    (
        params_sigs,
        possible_arg_params,
        possible_opt_params,
        annotations,
        defaults,
    ) = extract_all_params(cmd_obj)

    def _method(_self, *args, **kwargs):
        method_params = _method_sanity_check(
            possible_arg_params, possible_opt_params, annotations, defaults, **kwargs
        )
        _self._chain.append({cmd_obj.name: method_params})
        return _self.execute()

    m = _method
    m.__name__ = cmd_obj.name
    m.__doc__ = getattr(cmd_obj, "help", None)
    m.__signature__ = inspect.signature(_method).replace(
        parameters=params_sigs.values()
    )
    m.__annotations__ = annotations
    m.__defaults__ = tuple(defaults.values())

    return m


if __name__ == "__main__":
    from metaflow.cli import start

    api = MetaflowAPI.from_cli("../try.py", start)

    command = api(metadata="local").run(
        tags=["abc", "def"],
        decospecs=["kubernetes"],
        max_workers=5,
        alpha=3,
        myfile="path/to/file",
    )
    print(" ".join(command))

    command = (
        api(metadata="local")
        .kubernetes()
        .step(
            step_name="process",
            code_package_metadata="some_version",
            code_package_sha="some_sha",
            code_package_url="some_url",
        )
    )
    print(" ".join(command))

    command = api().tag().add(tags=["abc", "def"])
    print(" ".join(command))

    command = getattr(api(decospecs=["retry"]), "argo-workflows")().create()
    print(" ".join(command))
