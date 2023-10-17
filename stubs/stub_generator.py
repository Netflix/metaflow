import functools
import importlib
import inspect
import math
import os
import re
import time
import typing

from datetime import datetime
from io import StringIO
from itertools import chain
from typing import *

import metaflow
from metaflow.decorators import Decorator
from metaflow.graph import deindent_docstring
from metaflow.metaflow_version import get_version

TAB = "    "

param_section_header = re.compile(r"Parameters\s*\n----------\s*\n", flags=re.M)
add_to_current_header = re.compile(
    r"MF Add To Current\s*\n-----------------\s*\n", flags=re.M
)
non_indented_line = re.compile(r"^\S+.*$")
param_name_type = re.compile(r"^(?P<name>\S+)(?:\s*:\s*(?P<type>.*))?$")
type_annotations = re.compile(
    r"(?P<type>.*?)(?P<optional>, optional|\(optional\))?(?:, [Dd]efault(?: is | = |: |s to |)\s*(?P<default>.*))?$"
)


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
        self._done_modules = set()  # type: Set[str]
        self._output_dir = output_dir
        self._mf_version = get_version()
        self._addl_current = (
            dict()
        )  # type: Dict[str, Dict[str, Tuple[inspect.Signature, str]]]

        self._reset()

    def _reset(self):
        self._imports = set()  # type: Set[str]
        self._typing_imports = set()  # type: Set[str]
        self._current_objects = {}  # type: Dict[str, Any]
        self._stubs = []  # type: List[str]

    def _get_module(self, name):
        print("Getting module %s ..." % name)
        self._current_module = importlib.import_module(name)
        self._current_module_name = name
        for objname, obj in self._current_module.__dict__.items():
            if objname.startswith("_"):
                continue
            if inspect.ismodule(obj):
                # Only consider modules that are part of the root module
                if obj.__name__.startswith(self._root_module):
                    self._pending_modules.append(obj.__name__)
            else:
                parent_module = inspect.getmodule(obj)
                # For objects we include:
                #  - stuff that is a functools.partial (these are all the decorators;
                #    we could be more specific but good enough for now)
                #  - if this is for the root_module, anything in safe_modules
                #  - if this is not for the root_module, only stuff in this module
                if parent_module is None:
                    print("Object %s has no parent module" % objname)
                if parent_module is None or (
                    name + "." == self._root_module
                    and (
                        parent_module.__name__.startswith("functools")
                        or any(
                            [
                                parent_module.__name__.startswith(p)
                                for p in self._safe_modules
                            ]
                        )
                    )
                    or parent_module.__name__ == name
                ):
                    print("Adding object %s for module %s" % (objname, name))
                    self._current_objects[objname] = obj
                else:
                    print(
                        "Skipping object %s with parent module %s"
                        % (objname, parent_module.__name__)
                    )
                # We also include the module to process if it is part of root_module
                if parent_module is not None and parent_module.__name__.startswith(
                    self._root_module
                ):
                    self._pending_modules.append(parent_module.__name__)

    def _get_element_name_with_module(self, element: Union[TypeVar, type, Any]) -> str:
        # The element can be a string, for example "def f() -> 'SameClass':..."
        def _add_to_import(name):
            if name != self._current_module_name:
                self._imports.add(name)

        def _add_to_typing_check(name):
            if name != self._current_module_name:
                parent_module = name.split(".", 1)[0]
                self._typing_imports.add(parent_module)

        if isinstance(element, str):
            return element
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
        elif isinstance(element, typing.ForwardRef):
            f_arg = element.__forward_arg__
            if f_arg in ("Run", "Task"):  # HACK -- forward references in current.py
                _add_to_import("metaflow")
                f_arg = "metaflow.%s" % f_arg
            return '"%s"' % f_arg
        elif inspect.getmodule(element) == inspect.getmodule(typing):
            _add_to_import("typing")
            return str(element)
        else:
            raise RuntimeError(
                "Does not handle element %s of type %s" % (str(element), type(element))
            )

    def _generate_class_stub(self, name: str, clazz: type) -> str:
        buff = StringIO()
        print("Generating documentation for class %s" % name)
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
            if key == "__init__":
                init_func = element
            elif key == "__annotations__":
                annotation_dict = element
            if inspect.isfunction(element):
                if not element.__name__.startswith("_") or element.__name__.startswith(
                    "__"
                ):
                    buff.write(
                        self._generate_function_stub(key, element, indentation=TAB)
                    )
            elif isinstance(element, property):
                if element.fget:
                    buff.write(
                        self._generate_function_stub(
                            key, element.fget, indentation=TAB, is_getter=True
                        )
                    )
                if element.fset:
                    buff.write(
                        self._generate_function_stub(
                            key, element.fset, indentation=TAB, is_setter=True
                        )
                    )

        # Special handling for the current module
        if self._current_module_name == "metaflow.current" and name == "Current":
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
                        is_getter=True,
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
                            annotation=typing.Optional[type_name]
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
                current_return_type = eval(current_return_type.strip())
                current_doc = []
        _add()

        self._addl_current[name] = add_to_current
        return inspect.Signature(parameters=parameters), raw_doc

    def _generate_function_stub(
        self,
        name: str,
        func: Optional[Callable] = None,
        sign: Optional[inspect.Signature] = None,
        indentation: Optional[str] = None,
        doc: Optional[str] = None,
        is_setter=False,
        is_getter=False,
    ) -> str:
        def exploit_annotation(annotation: Any, starting: str = ": ") -> str:
            annotation_string = ""
            if annotation and annotation != inspect.Parameter.empty:
                annotation_string += starting + self._get_element_name_with_module(
                    annotation
                )
            return annotation_string

        buff = StringIO()
        if sign is None and func is None:
            raise RuntimeError(
                "Cannot generate stub for function %s with either a function or signature"
                % name
            )
        sign = sign or inspect.signature(cast(Callable, func))
        doc = doc or func.__doc__
        indentation = indentation or ""

        if is_setter:
            buff.write(indentation + "@" + name + ".setter\n")
        elif is_getter:
            buff.write(indentation + "@property\n")
        buff.write(indentation + "def " + name + "(")
        for i, (par_name, parameter) in enumerate(sign.parameters.items()):
            annotation = exploit_annotation(parameter.annotation)
            default = ""
            if (
                parameter.default != parameter.empty
                and type(parameter.default).__module__ == "builtins"
                and not str(parameter.default).startswith("<")
            ):
                default = (
                    " = " + str(parameter.default)
                    if not isinstance(parameter.default, str)
                    else " = '" + parameter.default + "'"
                )
            if parameter.kind == inspect.Parameter.VAR_KEYWORD:
                par_name = "**%s" % par_name
            elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
                par_name = "*%s" % par_name
            buff.write(par_name + annotation + default)

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
            if inspect.isclass(attr):
                self._stubs.append(self._generate_class_stub(name, attr))
            elif inspect.isfunction(attr):
                self._stubs.append(self._generate_function_stub(name, attr))
            elif isinstance(attr, functools.partial):
                if issubclass(attr.args[0], Decorator):
                    # Special case where we are going to extract the parameters from
                    # the docstring to make the decorator look nicer
                    print("Generating decorator documentation for %s..." % name)
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
        while len(self._pending_modules) != 0:
            module_name = self._pending_modules.pop(0)
            # Skip vendored stuff
            if module_name == "metaflow._vendor":
                continue
            # We delay current module
            if (
                module_name == "metaflow.current"
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
                for module in self._imports:
                    f.write("import " + module + "\n")
                if self._typing_imports:
                    f.write("if typing.TYPE_CHECKING:\n")
                    for module in self._typing_imports:
                        f.write(TAB + "import " + module + "\n")
                f.write("\n")
                for stub in self._stubs:
                    f.write(stub + "\n")


if __name__ == "__main__":
    gen = StubGenerator("./stubs")
    gen.write_out()
