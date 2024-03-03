import inspect
import importlib
import itertools
from collections import OrderedDict
from typeguard import check_type, TypeCheckError
import uuid, datetime
from typing import Optional, List
from metaflow import FlowSpec, Parameter
from metaflow.cli import start
from metaflow._vendor import click
from metaflow._vendor.click import Command, Group, Argument, Option
from metaflow.parameters import JSONTypeClass
from metaflow.includefile import FilePathClass
from metaflow._vendor.click.types import (
    StringParamType,
    IntParamType,
    FloatParamType,
    BoolParamType,
    UUIDParameterType,
    Path,
    DateTime,
    Tuple,
    Choice,
    File,
)

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
    JSONTypeClass: str,
    FilePathClass: str,
}


def _method_sanity_check(
    possible_arg_params, possible_opt_params, annotations, defaults, **kwargs
):
    method_params = {"args": {}, "options": {}}

    possible_params = OrderedDict()
    possible_params.update(possible_arg_params)
    possible_params.update(possible_opt_params)

    # supplied kwargs
    for supplied_k, supplied_v in kwargs.items():
        if supplied_k not in possible_params:
            raise ValueError(
                f"Unknown argument: '{supplied_k}', "
                f"possible args are: {list(possible_params.keys())}"
            )

        try:
            check_type(supplied_v, annotations[supplied_k])
        except TypeCheckError:
            raise TypeError(
                f"Invalid type for '{supplied_k}', "
                f"expected: '{annotations[supplied_k]}', "
                f"default is '{defaults[supplied_k]}'"
            )

        if supplied_k in possible_arg_params:
            cli_name = possible_arg_params[supplied_k].opts[0].strip("-")
            method_params["args"][cli_name] = supplied_v
        elif supplied_k in possible_opt_params:
            cli_name = possible_opt_params[supplied_k].opts[0].strip("-")
            method_params["options"][cli_name] = supplied_v

    # possible kwargs
    for _, possible_v in possible_params.items():
        cli_name = possible_v.opts[0].strip("-")
        if (
            (cli_name not in method_params["args"])
            and (cli_name not in method_params["options"])
        ) and possible_v.required:
            raise ValueError(f"Missing argument: {cli_name} is required.")

    return method_params


def get_annotation(param):
    py_type = click_to_python_types[type(param.type)]
    if not param.required:
        if param.multiple or param.nargs == -1:
            return Optional[List[py_type]]
        else:
            return Optional[py_type]
    else:
        if param.multiple or param.nargs == -1:
            return List[py_type]
        else:
            return py_type


def get_inspect_param_obj(p, kind):
    return inspect.Parameter(
        name=p.name,
        kind=kind,
        default=p.default,
        annotation=get_annotation(p),
    )


def extract_flowspec_params(flow_file):
    spec = importlib.util.spec_from_file_location("module", flow_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    classes = inspect.getmembers(module, inspect.isclass)

    parameters = []
    for _, kls in classes:
        if kls != FlowSpec and issubclass(kls, FlowSpec):
            for _, obj in inspect.getmembers(kls):
                if isinstance(obj, Parameter):
                    parameters.append(obj)

    return parameters


class MetaflowAPI(object):
    def __init__(self, parent=None, **kwargs):
        self._parent = parent
        self._chain = [{self._API_NAME: kwargs}]

    @property
    def parent(self):
        if self._parent:
            return self._parent
        return None

    @property
    def chain(self):
        return self._chain

    @classmethod
    def from_cli(cls, flow_file, cli_collection):
        flow_parameters = extract_flowspec_params(flow_file)
        class_dict = {"__module__": "metaflow", "_API_NAME": flow_file}
        command_groups = cli_collection.sources
        for each_group in command_groups:
            for _, cmd_obj in each_group.commands.items():
                if isinstance(cmd_obj, Group):
                    # TODO: possibly check for fake groups with cmd_obj.name in ["cli", "main"]
                    class_dict[cmd_obj.name] = extract_group(cmd_obj, flow_parameters)
                elif isinstance(cmd_obj, Command):
                    class_dict[cmd_obj.name] = extract_command(cmd_obj, flow_parameters)
                else:
                    raise RuntimeError(
                        f"Cannot handle {cmd_obj.name} of type {type(cmd_obj)}"
                    )

        to_return = type(flow_file, (MetaflowAPI,), class_dict)
        to_return.__name__ = flow_file

        (
            params_sigs,
            possible_arg_params,
            possible_opt_params,
            annotations,
            defaults,
        ) = extract_all_params(cli_collection)

        def _method(_self, **kwargs):
            method_params = _method_sanity_check(
                possible_arg_params,
                possible_opt_params,
                annotations,
                defaults,
                **kwargs,
            )
            return to_return(parent=None, **method_params)

        m = _method
        m.__name__ = cmd_obj.name
        m.__doc__ = getattr(cmd_obj, "help", None)
        m.__signature__ = inspect.signature(_method).replace(
            parameters=params_sigs.values()
        )
        m.__annotations__ = annotations
        m.__defaults__ = tuple(defaults.values())

        return m

    def execute(self):
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
                assert len(params) == 0
                for _, v in args.items():
                    if isinstance(v, list):
                        for i in v:
                            components.append(i)
                    else:
                        components.append(v)
                for k, v in options.items():
                    if isinstance(v, list):
                        for i in v:
                            components.append(f"--{k} {i}")
                    else:
                        components.append(f"--{k} {v}")

        return " ".join(components)


def extract_all_params(cmd_obj):
    arg_params_sigs = OrderedDict()
    opt_params_sigs = OrderedDict()
    params_sigs = OrderedDict()

    arg_parameters = OrderedDict()
    opt_parameters = OrderedDict()
    annotations = OrderedDict()
    defaults = OrderedDict()

    for each_param in cmd_obj.params:
        if isinstance(each_param, Argument):
            arg_params_sigs[each_param.name] = get_inspect_param_obj(
                each_param, inspect.Parameter.POSITIONAL_ONLY
            )
            arg_parameters[each_param.name] = each_param
        elif isinstance(each_param, Option):
            opt_params_sigs[each_param.name] = get_inspect_param_obj(
                each_param, inspect.Parameter.KEYWORD_ONLY
            )
            opt_parameters[each_param.name] = each_param

        annotations[each_param.name] = get_annotation(each_param)
        defaults[each_param.name] = each_param.default

    # first, fill in positional arguments
    for name, each_arg_param in arg_params_sigs.items():
        params_sigs[name] = each_arg_param
    # then, fill in keyword arguments
    for name, each_opt_param in opt_params_sigs.items():
        params_sigs[name] = each_opt_param

    return params_sigs, arg_parameters, opt_parameters, annotations, defaults


def extract_group(cmd_obj, flow_parameters):
    class_dict = {"__module__": "metaflow", "_API_NAME": cmd_obj.name}
    for _, sub_cmd_obj in cmd_obj.commands.items():
        if isinstance(sub_cmd_obj, Group):
            # recursion
            class_dict[sub_cmd_obj.name] = extract_group(sub_cmd_obj, flow_parameters)
        elif isinstance(sub_cmd_obj, Command):
            class_dict[sub_cmd_obj.name] = extract_command(sub_cmd_obj, flow_parameters)
        else:
            raise RuntimeError(
                f"Cannot handle {sub_cmd_obj.name} of type {type(sub_cmd_obj)}"
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

    def _method(_self, **kwargs):
        method_params = _method_sanity_check(
            possible_arg_params, possible_opt_params, annotations, defaults, **kwargs
        )
        return resulting_class(parent=_self, **method_params)

    m = _method
    m.__name__ = cmd_obj.name
    m.__doc__ = getattr(cmd_obj, "help", None)
    m.__signature__ = inspect.signature(_method).replace(
        parameters=params_sigs.values()
    )
    m.__annotations__ = annotations
    m.__defaults__ = tuple(defaults.values())

    return m


def extract_command(cmd_obj, flow_parameters):
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

    def _method(_self, **kwargs):
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
    api = MetaflowAPI.from_cli("../try.py", start)

    command = api(metadata="local").run(
        tags=["abc", "def"],
        decospecs=["kubernetes"],
        max_workers=5,
        alpha=3,
        myfile="path/to/file",
    )
    print(command)

    command = (
        api(metadata="local")
        .kubernetes()
        .step(
            step_name="process",
            code_package_sha="some_sha",
            code_package_url="some_url",
        )
    )
    print(command)

    command = api().tag().add(tags=["abc", "def"])
    print(command)
