import functools
import importlib
import inspect
import logging
import math
import os
import pathlib
import re
import time
import typing

from datetime import datetime
from io import StringIO
from types import ModuleType
from typing import (
    Any,
    Callable,
    Dict,
    ForwardRef,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from metaflow.decorators import Decorator
from metaflow.graph import deindent_docstring
from metaflow.metaflow_version import get_version

TAB = "    "
METAFLOW_CURRENT_MODULE_NAME = "metaflow.metaflow_current"

param_section_header = re.compile(r"Parameters\s*\n----------\s*\n", flags=re.M)
add_to_current_header = re.compile(
    r"MF Add To Current\s*\n-----------------\s*\n", flags=re.M
)
non_indented_line = re.compile(r"^\S+.*$")
param_name_type = re.compile(r"^(?P<name>\S+)(?:\s*:\s*(?P<type>.*))?$")
type_annotations = re.compile(
    r"(?P<type>.*?)(?P<optional>, optional|\(optional\))?(?:, [Dd]efault(?: is | = |: |s to |)\s*(?P<default>.*))?$"
)

logger = logging.getLogger("StubGenerator")
logger.setLevel(logging.INFO)


def descend_object(object: str, options: Iterable[str]):
    # Returns true if:
    #  - options contains a prefix of object
    #  - the component after the prefix does not start with _
    for opt in options:
        new_object = object.removeprefix(opt)
        if len(new_object) == len(object):
            # There was no prefix, so we continue
            continue
        # Using [1] to skip the inevitable "."
        if len(new_object) == 0 or new_object[1] != "_":
            return True
    return False


class StubGenerator:
    """
    This class takes the name of a library as input and a directory as output.

    It will then generate the corresponding stub files for each defined type
    (generic variables, functions, classes, etc.) at run time.
    This means that the code for the library is not statically parsed, but it is
    executed and then the types are dynamically created and analyzed to produce the stub
    files.

    The only items analyzes are those that belong to the library (ie: anything in
    the library or below it but not any external imports)
    """

    def __init__(self, output_dir: str):
        """
        Initializes the StubGenerator.
        :param file_path: the file path
        :type file_path: str
        :param members_from_other_modules: the names of the members defined in other module to be analyzed
        :type members_from_other_modules: List[str]
        """

        self._pending_modules = ["metaflow"]  # type: List[str]
        self._root_module = "metaflow."
        self._safe_modules = ["metaflow.", "metaflow_extensions."]

        # We exclude some modules to not create a bunch of random non-user facing
        # .pyi files. For now, it is definitely crucial to exclude metadata as it
        # conflicts with the metadata() function defined in metaflow as well and
        # griffe legitimately has a problem with it.
        self._exclude_modules = set(
            [
                "metaflow.cli_args",
                "metaflow.cmd_with_io",
                "metaflow.datastore",
                "metaflow.debug",
                "metaflow.decorators",
                "metaflow.event_logger",
                "metaflow.extension_support",
                "metaflow.graph",
                "metaflow.metadata",
                "metaflow.metaflow_config_funcs",
                "metaflow.metaflow_environment",
                "metaflow.mflog",
                "metaflow.monitor",
                "metaflow.package",
                "metaflow.R",
                "metaflow.sidecar",
                "metaflow.unbounded_foreach",
                "metaflow.util",
            ]
        )
        self._done_modules = set()  # type: Set[str]
        self._output_dir = output_dir
        self._mf_version = get_version()
        self._addl_current = (
            dict()
        )  # type: Dict[str, Dict[str, Tuple[inspect.Signature, str]]]

        self._reset()

    def _reset(self):
        # "Globals" that are used throughout processing. This is not the cleanest
        # but simplifies code quite a bit.

        # Imports that are needed at the top of the file
        self._imports = set()  # type: Set[str]
        # Typing imports (behind if TYPE_CHECKING) that are needed at the top of the file
        self._typing_imports = set()  # type: Set[str]
        # Current objects in the file being processed
        self._current_objects = {}  # type: Dict[str, Any]
        # Current stubs in the file being processed
        self._stubs = []  # type: List[str]

        # These have a shorter "scope"
        # Current parent module of the object being processed -- used to determine
        # the "globals()"
        self._current_parent_module = None  # type: Optional[ModuleType]

    def _get_module(self, name):
        logger.info("Analyzing module %s ...", name)
        self._current_module = importlib.import_module(name)
        self._current_module_name = name
        for objname, obj in self._current_module.__dict__.items():
            if objname.startswith("_"):
                logger.debug("Skipping object because it starts with _ %s", objname)
                continue
            if inspect.ismodule(obj):
                # Only consider modules that are part of the root module
                if (
                    obj.__name__.startswith(self._root_module)
                    and not obj.__name__ in self._exclude_modules
                ):
                    logger.debug("Adding child module %s to process", obj.__name__)
                    self._pending_modules.append(obj.__name__)
                else:
                    logger.debug("Skipping child module %s", obj.__name__)
            else:
                parent_module = inspect.getmodule(obj)
                # For objects we include:
                #  - stuff that is a functools.partial (these are all the decorators;
                #    we could be more specific but good enough for now) for root module
                #  - otherwise, anything that is in safe_modules. Note this may include
                #    a bit much (all the imports)
                if (
                    parent_module is None
                    or (
                        name + "." == self._root_module
                        and (parent_module.__name__.startswith("functools"))
                    )
                    or (
                        not any(
                            [
                                parent_module.__name__.startswith(p)
                                for p in self._exclude_modules
                            ]
                        )
                        and any(
                            [
                                parent_module.__name__.startswith(p)
                                for p in self._safe_modules
                            ]
                        )
                    )
                ):
                    logger.debug("Adding object %s to process", objname)
                    self._current_objects[objname] = obj
                else:
                    logger.debug("Skipping object %s", objname)
                # We also include the module to process if it is part of root_module
                if (
                    parent_module is not None
                    and not any(
                        [
                            parent_module.__name__.startswith(d)
                            for d in self._exclude_modules
                        ]
                    )
                    and parent_module.__name__.startswith(self._root_module)
                ):
                    logger.debug(
                        "Adding module of child object %s to process",
                        parent_module.__name__,
                    )
                    self._pending_modules.append(parent_module.__name__)

    def _get_element_name_with_module(self, element: Union[TypeVar, type, Any]) -> str:
        # The element can be a string, for example "def f() -> 'SameClass':..."
        def _add_to_import(name):
            if name != self._current_module_name:
                self._imports.add(name)

        def _add_to_typing_check(name):
            splits = name.rsplit(".", 1)
            if len(splits) == 2:
                # We don't add things that are just one name -- probably things within
                # the current file
                if splits[0] != self._current_module_name:
                    self._typing_imports.add(splits[0])

        if isinstance(element, str):
            # We first try to eval the annotation because with the annotations future
            # it is always a string
            try:
                potential_element = eval(
                    element,
                    self._current_parent_module.__dict__
                    if self._current_parent_module
                    else None,
                )
                if potential_element:
                    element = potential_element
            except:
                pass

        if isinstance(element, str):
            _add_to_typing_check(element)
            return '"%s"' % element
        elif isinstance(element, TypeVar):
            return "{0}.{1}".format(
                element.__module__, element.__bound__.__forward_arg__
            )
        elif isinstance(element, type):
            module = inspect.getmodule(element)
            if (
                module is None
                or module.__name__ == "builtins"
                or module.__name__ == "__main__"
            ):
                # Special case for "NoneType" -- return None as NoneType is only 3.10+
                if element.__name__ == "NoneType":
                    return "None"
                return element.__name__

            _add_to_import(module.__name__)
            if module.__name__ != self._current_module_name:
                return "{0}.{1}".format(module.__name__, element.__name__)
            else:
                return element.__name__
        elif isinstance(element, type(Ellipsis)):
            return "..."
        elif isinstance(element, typing._GenericAlias):
            # We need to check things recursively in __args__ if it exists
            args_str = []
            for arg in getattr(element, "__args__", []):
                args_str.append(self._get_element_name_with_module(arg))

            _add_to_import("typing")
            if element._name:
                if element._name == "Optional":
                    # We don't want to include NoneType in the string -- it breaks things
                    args_str = args_str[:1]
                elif element._name == "Callable":
                    # We need to make this a list of everything except the end one
                    # except if it is an ellipsis
                    if args_str[0] != "...":
                        call_args = "[" + ", ".join(args_str[:-1]) + "]"
                        args_str = [call_args, args_str[-1]]
                return "typing.%s[%s]" % (element._name, ", ".join(args_str))
            else:
                return "%s[%s]" % (element.__origin__, ", ".join(args_str))
        elif isinstance(element, ForwardRef):
            f_arg = element.__forward_arg__
            # if f_arg in ("Run", "Task"):  # HACK -- forward references in current.py
            #    _add_to_import("metaflow")
            #    f_arg = "metaflow.%s" % f_arg
            _add_to_typing_check(f_arg)
            return '"%s"' % f_arg
        elif inspect.getmodule(element) == inspect.getmodule(typing):
            _add_to_import("typing")
            # Special handling for NamedTuple which is a function
            if element.__name__ == "NamedTuple":
                return "typing.NamedTuple"
            return str(element)
        else:
            raise RuntimeError(
                "Does not handle element %s of type %s" % (str(element), type(element))
            )

    def _generate_class_stub(self, name: str, clazz: type) -> str:
        buff = StringIO()
        # Class prototype
        buff.write("class " + name.split(".")[-1] + "(")

        # Add super classes
        for c in clazz.__bases__:
            name_with_module = self._get_element_name_with_module(c)
            buff.write(name_with_module + ", ")

        # Add metaclass
        name_with_module = self._get_element_name_with_module(clazz.__class__)
        buff.write("metaclass=" + name_with_module + "):\n")

        # For NamedTuple, we have __annotations__ but no __init__. In that case,
        # we are going to "create" a __init__ function with the annotations
        # to show what the class takes.
        annotation_dict = None
        init_func = None
        for key, element in clazz.__dict__.items():
            func_deco = None
            if isinstance(element, staticmethod):
                func_deco = "@staticmethod"
                element = element.__func__
            elif isinstance(element, classmethod):
                func_deco = "@classmethod"
                element = element.__func__
            if key == "__init__":
                init_func = element
            elif key == "__annotations__":
                annotation_dict = element
            if inspect.isfunction(element):
                if not element.__name__.startswith("_") or element.__name__.startswith(
                    "__"
                ):
                    buff.write(
                        self._generate_function_stub(
                            key, element, indentation=TAB, deco=func_deco
                        )
                    )
            elif isinstance(element, property):
                if element.fget:
                    buff.write(
                        self._generate_function_stub(
                            key, element.fget, indentation=TAB, deco="@property"
                        )
                    )
                if element.fset:
                    buff.write(
                        self._generate_function_stub(
                            key, element.fset, indentation=TAB, deco="@%s.setter" % key
                        )
                    )

        # Special handling for the current module
        if (
            self._current_module_name == METAFLOW_CURRENT_MODULE_NAME
            and name == "Current"
        ):
            # Multiple decorators can add the same object (trigger and trigger_on_finish)
            # as examples so we sort it out.
            resulting_dict = (
                dict()
            )  # type Dict[str, List[inspect.Signature, str, List[str]]]
            for project_name, addl_current in self._addl_current.items():
                for name, (sign, doc) in addl_current.items():
                    r = resulting_dict.setdefault(name, [sign, doc, []])
                    r[2].append("@%s" % project_name)
            for name, (sign, doc, decos) in resulting_dict.items():
                buff.write(
                    self._generate_function_stub(
                        name,
                        sign=sign,
                        indentation=TAB,
                        doc="(only in the presence of the %s decorator%s)\n\n"
                        % (", or ".join(decos), "" if len(decos) == 1 else "s")
                        + doc,
                        deco="@property",
                    )
                )
        if init_func is None and annotation_dict:
            buff.write(
                self._generate_function_stub(
                    "__init__",
                    func=None,
                    sign=inspect.Signature(
                        parameters=[
                            inspect.Parameter(
                                name="self",
                                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            )
                        ]
                        + [
                            inspect.Parameter(
                                name=name,
                                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                annotation=annotation,
                            )
                            for name, annotation in annotation_dict.items()
                        ]
                    ),
                    indentation=TAB,
                )
            )
        buff.write("%s...\n" % TAB)

        return buff.getvalue()

    def _extract_parameters_from_doc(
        self, name: str, raw_doc: Optional[str]
    ) -> Optional[Tuple[inspect.Signature, str]]:
        # TODO: This only handles the `Parameters` section for now; we are
        # using it only to parse the documentation for step/flow decorators so
        # this is enough for now but it could be extended more.
        # Inspired from:
        # https://github.com/rr-/docstring_parser/blob/master/docstring_parser/numpydoc.py
        if raw_doc is None:
            return None
        raw_doc = inspect.cleandoc(raw_doc)
        has_parameters = param_section_header.search(raw_doc)
        has_add_to_current = add_to_current_header.search(raw_doc)
        if not has_parameters:
            return None
        if has_add_to_current:
            doc = raw_doc[has_parameters.end() : has_add_to_current.start()]
            add_to_current_doc = raw_doc[has_add_to_current.end() :]
            raw_doc = raw_doc[: has_add_to_current.start()]
        else:
            doc = raw_doc[has_parameters.end() :]
            add_to_current_doc = None
        parameters = []
        for line in doc.splitlines():
            if non_indented_line.match(line):
                match = param_name_type.match(line)
                arg_name = type_name = is_optional = default = None
                default_set = False
                if match is not None:
                    arg_name = match.group("name")
                    type_name = match.group("type")
                    if type_name is not None:
                        type_detail = type_annotations.match(type_name)
                        if type_detail is not None:
                            type_name = type_detail.group("type")
                            is_optional = type_detail.group("optional") is not None
                            default = type_detail.group("default")
                            if default:
                                default_set = True
                            try:
                                default = eval(default)
                            except:
                                pass
                            try:
                                type_name = eval(type_name)
                            except:
                                pass
                    parameters.append(
                        inspect.Parameter(
                            name=arg_name,
                            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            default=default
                            if default_set
                            else None
                            if is_optional
                            else inspect.Parameter.empty,
                            annotation=Optional[type_name]
                            if is_optional
                            else type_name,
                        )
                    )
        if not add_to_current_doc:
            return inspect.Signature(parameters=parameters), raw_doc

        current_property = None
        current_return_type = None
        current_property_indent = None
        current_doc = []
        add_to_current = dict()  # type: Dict[str, Tuple[inspect.Signature, str]]

        def _add():
            if current_property:
                add_to_current[current_property] = (
                    inspect.Signature(
                        [
                            inspect.Parameter(
                                "self", inspect.Parameter.POSITIONAL_OR_KEYWORD
                            )
                        ],
                        return_annotation=current_return_type,
                    ),
                    "\n".join(current_doc),
                )

        for line in add_to_current_doc.splitlines():
            # Parse stanzas that look like the following:
            # <property-name> -> type
            # indented doc string
            if current_property_indent is not None and (
                line.startswith(current_property_indent + " ") or line.strip() == ""
            ):
                current_doc.append(line[len(current_property_indent) :].rstrip())
            else:
                if line.strip() == 0:
                    continue
                if current_property:
                    # Ends a property stanza
                    _add()
                # Now start a new one
                line = line.rstrip()
                current_property_indent = line[: len(line) - len(line.lstrip())]
                # This is a line so we split it using "->"
                current_property, current_return_type = line.split("->")
                current_property = current_property.strip()
                current_return_type = current_return_type.strip()
                current_doc = []
        _add()

        self._addl_current[name] = add_to_current
        return inspect.Signature(parameters=parameters), raw_doc

    def _generate_function_stub(
        self,
        name: str,
        func: Optional[Union[Callable, classmethod]] = None,
        sign: Optional[inspect.Signature] = None,
        indentation: Optional[str] = None,
        doc: Optional[str] = None,
        deco: Optional[str] = None,
    ) -> str:
        def exploit_annotation(annotation: Any, starting: str = ": ") -> str:
            annotation_string = ""
            if annotation and annotation != inspect.Parameter.empty:
                annotation_string += starting + self._get_element_name_with_module(
                    annotation
                )
            return annotation_string

        def exploit_default(default_value: Any) -> Optional[str]:
            if (
                default_value != inspect.Parameter.empty
                and type(default_value).__module__ == "builtins"
            ):
                if isinstance(default_value, list):
                    return (
                        "["
                        + ", ".join(
                            [cast(str, exploit_default(v)) for v in default_value]
                        )
                        + "]"
                    )
                elif isinstance(default_value, tuple):
                    return (
                        "("
                        + ", ".join(
                            [cast(str, exploit_default(v)) for v in default_value]
                        )
                        + ")"
                    )
                elif isinstance(default_value, dict):
                    return (
                        "{"
                        + ", ".join(
                            [
                                cast(str, exploit_default(k))
                                + ": "
                                + cast(str, exploit_default(v))
                                for k, v in default_value.items()
                            ]
                        )
                        + "}"
                    )
                elif str(default_value).startswith("<"):
                    if default_value.__module__ == "builtins":
                        return default_value.__name__
                    else:
                        self._imports.add(default_value.__module__)
                        return ".".join(
                            [default_value.__module__, default_value.__name__]
                        )
                else:
                    return (
                        str(default_value)
                        if not isinstance(default_value, str)
                        else '"' + default_value + '"'
                    )
            else:
                return None

        buff = StringIO()
        if sign is None and func is None:
            raise RuntimeError(
                "Cannot generate stub for function %s with either a function or signature"
                % name
            )
        sign = sign or inspect.signature(cast(Callable, func))
        doc = doc or func.__doc__
        indentation = indentation or ""

        if deco:
            buff.write(indentation + deco + "\n")
        buff.write(indentation + "def " + name + "(")
        for i, (par_name, parameter) in enumerate(sign.parameters.items()):
            annotation = exploit_annotation(parameter.annotation)
            default = exploit_default(parameter.default)

            if parameter.kind == inspect.Parameter.VAR_KEYWORD:
                par_name = "**%s" % par_name
            elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
                par_name = "*%s" % par_name

            if default:
                buff.write(par_name + annotation + " = " + default)
            else:
                buff.write(par_name + annotation)

            if i < len(sign.parameters) - 1:
                buff.write(", ")
        ret_annotation = exploit_annotation(sign.return_annotation, starting=" -> ")
        buff.write(")" + ret_annotation + ":\n")

        if doc is not None:
            buff.write('%s%s"""\n' % (indentation, TAB))
            doc = cast(str, deindent_docstring(doc))
            init_blank = True
            for line in doc.split("\n"):
                if init_blank and len(line.strip()) == 0:
                    continue
                init_blank = False
                buff.write("%s%s%s\n" % (indentation, TAB, line.rstrip()))
            buff.write('%s%s"""\n' % (indentation, TAB))
        buff.write("%s%s...\n" % (indentation, TAB))
        return buff.getvalue()

    def _generate_generic_stub(self, element_name: str, element: Any) -> str:
        return "{0}: {1}\n".format(
            element_name, self._get_element_name_with_module(type(element))
        )

    def _generate_stubs(self):
        for name, attr in self._current_objects.items():
            self._current_parent_module = inspect.getmodule(attr)
            if inspect.isclass(attr):
                self._stubs.append(self._generate_class_stub(name, attr))
            elif inspect.isfunction(attr):
                self._stubs.append(self._generate_function_stub(name, attr))
            elif isinstance(attr, functools.partial):
                if issubclass(attr.args[0], Decorator):
                    # Special case where we are going to extract the parameters from
                    # the docstring to make the decorator look nicer
                    res = self._extract_parameters_from_doc(name, attr.args[0].__doc__)
                    if res:
                        self._stubs.append(
                            self._generate_function_stub(
                                name,
                                func=attr.func,
                                sign=res[0],
                                doc=res[1],
                            )
                        )
                else:
                    self._stubs.append(
                        self._generate_function_stub(
                            name, attr.func, doc=attr.args[0].__doc__
                        )
                    )
            elif not inspect.ismodule(attr):
                self._stubs.append(self._generate_generic_stub(name, attr))

    def write_out(self):
        out_dir = self._output_dir
        os.makedirs(out_dir, exist_ok=True)
        # Write out py.typed (pylance seems to require it even though it is not
        # required in PEP 561) as well as a file we will use to check the "version"
        # of the stubs -- this helps to inform the user if the stubs were generated
        # for another version of Metaflow.
        pathlib.Path(os.path.join(out_dir, "py.typed")).touch()
        pathlib.Path(os.path.join(out_dir, "generated_for.txt")).write_text(
            "%s %s"
            % (self._mf_version, datetime.fromtimestamp(time.time()).isoformat())
        )
        while len(self._pending_modules) != 0:
            module_name = self._pending_modules.pop(0)
            # Skip vendored stuff
            if module_name.startswith("metaflow._vendor"):
                continue
            # We delay current module
            if (
                module_name == METAFLOW_CURRENT_MODULE_NAME
                and len(set(self._pending_modules)) > 1
            ):
                self._pending_modules.append(module_name)
                continue
            if module_name in self._done_modules:
                continue
            self._done_modules.add(module_name)
            # If not, we process the module
            self._reset()
            self._get_module(module_name)
            self._generate_stubs()

            # We don't include "metaflow" in the out directory
            out_dir = os.path.join(
                self._output_dir, *self._current_module.__name__.split(".")[1:]
            )
            os.makedirs(out_dir, exist_ok=True)

            width = 55
            title_line = "Auto-generated Metaflow stub file"
            title_white_space = (width - len(title_line)) / 2
            title_line = "#%s%s%s#\n" % (
                " " * math.floor(title_white_space),
                title_line,
                " " * math.ceil(title_white_space),
            )
            with open(
                os.path.join(out_dir, "__init__.pyi"), mode="w", encoding="utf-8"
            ) as f:
                f.write(
                    "#" * (width + 2)
                    + "\n"
                    + title_line
                    + "# MF version: %s%s#\n"
                    % (self._mf_version, " " * (width - 13 - len(self._mf_version)))
                    + "# Generated on %s%s#\n"
                    % (
                        datetime.fromtimestamp(time.time()).isoformat(),
                        " " * (width - 14 - 26),
                    )
                    + "#" * (width + 2)
                    + "\n\n"
                )
                f.write("from __future__ import annotations\n\n")
                imported_typing = False
                for module in self._imports:
                    f.write("import " + module + "\n")
                    if module == "typing":
                        imported_typing = True
                if self._typing_imports:
                    if not imported_typing:
                        f.write("import typing\n")
                    f.write("if typing.TYPE_CHECKING:\n")
                    for module in self._typing_imports:
                        f.write(TAB + "import " + module + "\n")
                f.write("\n")
                for stub in self._stubs:
                    f.write(stub + "\n")


if __name__ == "__main__":
    gen = StubGenerator("./stubs")
    gen.write_out()
