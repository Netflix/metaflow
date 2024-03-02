import inspect
from collections import OrderedDict
from typeguard import check_type, TypeCheckError
import uuid, datetime
from typing import Optional, List
from metaflow.cli import start
from metaflow._vendor.click import Command, Group, Argument, Option
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
}


def get_annotation(param):
    py_type = click_to_python_types[type(param.type)]
    if not param.required:
        if param.multiple:
            return Optional[List[py_type]]
        else:
            return Optional[py_type]
    else:
        if param.multiple:
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


def extract_commands(cli_collection):
    commands_to_methods = {}
    command_groups = cli_collection.sources
    for each_group in command_groups:
        for _, cmd_obj in each_group.commands.items():
            if isinstance(cmd_obj, Group):
                # TODO: possibly check for fake groups with cmd_obj.name in ["cli", "main"]
                cmd = extract_group(cmd_obj)
            elif isinstance(cmd_obj, Command):
                m = extract_command_to_method(cmd_obj)
                commands_to_methods[cmd_obj.name] = m
            else:
                raise RuntimeError(
                    f"Cannot handle {cmd_obj.name} of type {type(cmd_obj)}"
                )

    return commands_to_methods


def extract_group(cmd_obj):
    for _, sub_cmd_obj in cmd_obj.commands.items():
        if isinstance(sub_cmd_obj, Group):
            # TODO: recursion / nesting to be done here
            ...
            # print("aaa", cmd_obj.name, sub_cmd_obj.name, sub_cmd_obj)
        elif isinstance(sub_cmd_obj, Command):
            # TODO: call extract_command_to_method here
            ...
            # print("bbb", cmd_obj.name, sub_cmd_obj.name, sub_cmd_obj)
        else:
            raise RuntimeError(
                f"Cannot handle {sub_cmd_obj.name} of type {type(sub_cmd_obj)}"
            )


def extract_command_to_method(cmd_obj):
    def extract_all_params():
        arg_params = OrderedDict()
        opt_params = OrderedDict()

        params_sigs = OrderedDict()
        parameters = OrderedDict()
        annotations = OrderedDict()
        defaults = OrderedDict()

        for each_param in cmd_obj.params:
            if isinstance(each_param, Argument):
                arg_params[each_param.name] = get_inspect_param_obj(
                    each_param, inspect.Parameter.POSITIONAL_ONLY
                )
            elif isinstance(each_param, Option):
                opt_params[each_param.name] = get_inspect_param_obj(
                    each_param, inspect.Parameter.KEYWORD_ONLY
                )

            parameters[each_param.name] = each_param
            annotations[each_param.name] = get_annotation(each_param)
            defaults[each_param.name] = each_param.default

        # first, fill in positional arguments
        for name, each_arg_param in arg_params.items():
            params_sigs[name] = each_arg_param
        # then, fill in keyword arguments
        for name, each_opt_param in opt_params.items():
            params_sigs[name] = each_opt_param

        return params_sigs, parameters, annotations, defaults

    def _method(_self, **kwargs):
        _, possible_params, annotations, defaults = extract_all_params()
        method_params = {}

        # supplied kwargs
        for supplied_k, supplied_v in kwargs.items():
            if supplied_k not in possible_params:
                raise ValueError(
                    f"Unknown argument: '{supplied_k}', possible args are: {list(possible_params.keys())}"
                )

            try:
                check_type(supplied_v, annotations[supplied_k])
            except TypeCheckError:
                raise TypeError(
                    f"Invalid type for '{supplied_k}', expected: '{annotations[supplied_k]}', default is '{defaults[supplied_k]}'"
                )

            method_params[supplied_k] = supplied_v

        # possible kwargs
        for possible_k, possible_v in possible_params.items():
            if possible_k not in method_params and possible_v.required:
                raise ValueError(f"Missing argument: {possible_k} is required.")

        # romain's idea, do something about it...
        _self._chain.append({cmd_obj.name: method_params})
        return _self.execute()

    params_sigs, _, annotations, defaults = extract_all_params()

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
    commands_to_methods = extract_commands(start)
    resume_fn = commands_to_methods["resume"]
    print(resume_fn(tags=["pp"]))
