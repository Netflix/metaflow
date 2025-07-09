import functools
import importlib
import inspect
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
    NewType,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from metaflow import FlowSpec, step
from metaflow.debug import debug
from metaflow.decorators import Decorator, FlowDecorator
from metaflow.extension_support import get_aliased_modules
from metaflow.metaflow_current import Current
from metaflow.metaflow_version import get_version
from metaflow.runner.deployer import DeployedFlow, Deployer, TriggeredRun
from metaflow.runner.deployer_impl import DeployerImpl

TAB = "    "
METAFLOW_CURRENT_MODULE_NAME = "metaflow.metaflow_current"
METAFLOW_DEPLOYER_MODULE_NAME = "metaflow.runner.deployer"

param_section_header = re.compile(r"Parameters\s*\n----------\s*\n", flags=re.M)
return_section_header = re.compile(r"Returns\s*\n-------\s*\n", flags=re.M)
add_to_current_header = re.compile(
    r"MF Add To Current\s*\n-----------------\s*\n", flags=re.M
)
non_indented_line = re.compile(r"^\S+.*$")
param_name_type = re.compile(r"^(?P<name>\S+)(?:\s*:\s*(?P<type>.*))?$")
type_annotations = re.compile(
    r"(?P<type>.*?)(?P<optional>, optional|\(optional\))?(?:, [Dd]efault(?: is | = |: |s to |)\s*(?P<default>.*))?$"
)

FlowSpecDerived = TypeVar("FlowSpecDerived", bound=FlowSpec)

StepFlag = NewType("StepFlag", bool)

MetaflowStepFunction = Union[
    Callable[[FlowSpecDerived, StepFlag], None],
    Callable[[FlowSpecDerived, Any, StepFlag], None],
]


# Object that has start() and end() like a Match object to make the code simpler when
# we are parsing different sections of doc
class StartEnd:
    def __init__(self, start: int, end: int):
        self._start = start
        self._end = end

    def start(self):
        return self._start

    def end(self):
        return self._end


def type_var_to_str(t: TypeVar) -> str:
    bound_name = None
    if t.__bound__ is not None:
        if isinstance(t.__bound__, typing.ForwardRef):
            bound_name = t.__bound__.__forward_arg__
        else:
            bound_name = t.__bound__.__name__
    return 'typing.TypeVar("%s", %scontravariant=%s, covariant=%s%s)' % (
        t.__name__,
        'bound="%s", ' % bound_name if t.__bound__ else "",
        t.__contravariant__,
        t.__covariant__,
        ", ".join([""] + [c.__name__ for c in t.__constraints__]),
    )


def new_type_to_str(t: typing.NewType) -> str:
    return 'typing.NewType("%s", %s)' % (t.__name__, t.__supertype__.__name__)


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


def parse_params_from_doc(doc: str) -> Tuple[List[inspect.Parameter], bool]:
    parameters = []
    no_arg_version = True
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
                        kind=inspect.Parameter.KEYWORD_ONLY,
                        default=(
                            default
                            if default_set
                            else None if is_optional else inspect.Parameter.empty
                        ),
                        annotation=(Optional[type_name] if is_optional else type_name),
                    )
                )
                if not default_set:
                    # If we don't have a default set for any parameter, we can't
                    # have a no-arg version since the function would be incomplete
                    no_arg_version = False
    return parameters, no_arg_version


def split_docs(
    raw_doc: str, boundaries: List[Tuple[str, Union[StartEnd, re.Match]]]
) -> Dict[str, str]:
    docs = dict()
    boundaries.sort(key=lambda x: x[1].start())

    section_start = 0
    for idx in range(1, len(boundaries)):
        docs[boundaries[idx - 1][0]] = raw_doc[
            section_start : boundaries[idx][1].start()
        ]
        section_start = boundaries[idx][1].end()
    docs[boundaries[-1][0]] = raw_doc[section_start:]
    return docs


def parse_add_to_docs(
    raw_doc: str,
) -> Dict[str, Union[Tuple[inspect.Signature, str], str]]:
    prop = None
    return_type = None
    property_indent = None
    doc = []
    add_to_docs = dict()  # type: Dict[str, Union[str, Tuple[inspect.Signature, str]]]

    def _add():
        if prop:
            add_to_docs[prop] = (
                inspect.Signature(
                    [
                        inspect.Parameter(
                            "self", inspect.Parameter.POSITIONAL_OR_KEYWORD
                        )
                    ],
                    return_annotation=return_type,
                ),
                "\n".join(doc),
            )

    for line in raw_doc.splitlines():
        # Parse stanzas that look like the following:
        # <property-name> -> type
        # indented doc string
        if property_indent is not None and (
            line.startswith(property_indent + " ") or line.strip() == ""
        ):
            offset = len(property_indent)
            if line.lstrip().startswith("@@ "):
                line = line.replace("@@ ", "")
            doc.append(line[offset:].rstrip())
        else:
            if line.strip() == 0:
                continue
            if prop:
                # Ends a property stanza
                _add()
            # Now start a new one
            line = line.rstrip()
            property_indent = line[: len(line) - len(line.lstrip())]
            # Either this has a -> to denote a property or it is a pure name
            # to denote a reference to a function (starting with #)
            line = line.lstrip()
            if line.startswith("#"):
                # The name of the function is the last part like metaflow.deployer.run
                add_to_docs[line.split(".")[-1]] = line[1:]
                continue
            # This is a line so we split it using "->"
            prop, return_type = line.split("->")
            prop = prop.strip()
            return_type = return_type.strip()
            doc = []
    _add()
    return add_to_docs


def add_indent(indentation: str, text: str) -> str:
    return "\n".join([indentation + line for line in text.splitlines()])


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

    def __init__(self, output_dir: str, include_generated_for: bool = True):
        """
        Initializes the StubGenerator.
        :param file_path: the file path
        :type file_path: str
        :param members_from_other_modules: the names of the members defined in other module to be analyzed
        :type members_from_other_modules: List[str]
        """

        # Let metaflow know we are in stubgen mode. This is sometimes useful to skip
        # some processing like loading libraries, etc. It is used in Metaflow extensions
        # so do not remove even if you do not see a use for it directly in the code.
        os.environ["METAFLOW_STUBGEN"] = "1"

        self._write_generated_for = include_generated_for
        # First element is the name it should be installed in (alias) and second is the
        # actual module name
        self._pending_modules = [
            ("metaflow", "metaflow")
        ]  # type: List[Tuple[str, str]]
        self._root_module = "metaflow."
        self._safe_modules = ["metaflow.", "metaflow_extensions."]

        self._pending_modules.extend(
            (self._get_module_name_alias(x), x) for x in get_aliased_modules()
        )

        # We exclude some modules to not create a bunch of random non-user facing
        # .pyi files.
        self._exclude_modules = set(
            [
                "metaflow.cli_args",
                "metaflow.cmd",
                "metaflow.cmd_with_io",
                "metaflow.datastore",
                "metaflow.debug",
                "metaflow.decorators",
                "metaflow.event_logger",
                "metaflow.extension_support",
                "metaflow.graph",
                "metaflow.integrations",
                "metaflow.lint",
                "metaflow.metaflow_metadata",
                "metaflow.metaflow_config_funcs",
                "metaflow.metaflow_environment",
                "metaflow.metaflow_profile",
                "metaflow.metaflow_version",
                "metaflow.mflog",
                "metaflow.monitor",
                "metaflow.package",
                "metaflow.plugins.datastores",
                "metaflow.plugins.env_escape",
                "metaflow.plugins.metadata_providers",
                "metaflow.procpoll.py",
                "metaflow.R",
                "metaflow.runtime",
                "metaflow.sidecar",
                "metaflow.task",
                "metaflow.tracing",
                "metaflow.unbounded_foreach",
                "metaflow.util",
                "metaflow._vendor",
            ]
        )

        self._done_modules = set()  # type: Set[str]
        self._output_dir = output_dir
        self._mf_version = get_version()

        # Contains the names of the methods that are injected in Deployer
        self._deployer_injected_methods = (
            {}
        )  # type: Dict[str, Dict[str, Union[Tuple[str, str], str]]]
        # Contains information to add to the Current object (injected by decorators)
        self._addl_current = (
            dict()
        )  # type: Dict[str, Dict[str, Tuple[inspect.Signature, str]]]

        self._reset()

    def _reset(self):
        # "Globals" that are used throughout processing. This is not the cleanest
        # but simplifies code quite a bit.

        # Imports that are needed at the top of the file
        self._imports = set()  # type: Set[str]

        self._sub_module_imports = set()  # type: Set[Tuple[str, str]]``
        # Typing imports (behind if TYPE_CHECKING) that are needed at the top of the file
        self._typing_imports = set()  # type: Set[str]
        # Typevars that are defined
        self._typevars = dict()  # type: Dict[str, Union[TypeVar, type]]
        # Current objects in the file being processed
        self._current_objects = {}  # type: Dict[str, Any]
        self._current_references = []  # type: List[str]
        # Current stubs in the file being processed
        self._stubs = []  # type: List[str]

        # These have a shorter "scope"
        # Current parent module of the object being processed -- used to determine
        # the "globals()"
        self._current_parent_module = None  # type: Optional[ModuleType]

    def _get_module_name_alias(self, module_name):
        if any(
            module_name.startswith(x) for x in self._safe_modules
        ) and not module_name.startswith(self._root_module):
            return self._root_module + ".".join(
                ["mf_extensions", *module_name.split(".")[1:]]
            )
        return module_name

    def _get_relative_import(
        self, new_module_name, cur_module_name, is_init_module=False
    ):
        new_components = new_module_name.split(".")
        cur_components = cur_module_name.split(".")
        init_module_count = 1 if is_init_module else 0
        common_idx = 0
        max_idx = min(len(new_components), len(cur_components))
        while (
            common_idx < max_idx
            and new_components[common_idx] == cur_components[common_idx]
        ):
            common_idx += 1
        # current: a.b and parent: a.b.e.d -> from .e.d import <name>
        # current: a.b.c.d and parent: a.b.e.f -> from ...e.f import <name>
        return "." * (len(cur_components) - common_idx + init_module_count) + ".".join(
            new_components[common_idx:]
        )

    def _get_module(self, alias, name):
        debug.stubgen_exec("Analyzing module %s (aliased at %s)..." % (name, alias))
        self._current_module = importlib.import_module(name)
        self._current_module_name = alias
        for objname, obj in self._current_module.__dict__.items():
            if objname == "_addl_stubgen_modules":
                debug.stubgen_exec(
                    "Adding modules %s from _addl_stubgen_modules" % str(obj)
                )
                self._pending_modules.extend(
                    (self._get_module_name_alias(m), m) for m in obj
                )
                continue
            if objname.startswith("_"):
                debug.stubgen_exec(
                    "Skipping object because it starts with _ %s" % objname
                )
                continue
            if inspect.ismodule(obj):
                # Only consider modules that are safe modules
                if (
                    any(obj.__name__.startswith(m) for m in self._safe_modules)
                    and not obj.__name__ in self._exclude_modules
                ):
                    debug.stubgen_exec(
                        "Adding child module %s to process" % obj.__name__
                    )

                    new_module_alias = self._get_module_name_alias(obj.__name__)
                    self._pending_modules.append((new_module_alias, obj.__name__))

                    new_parent, new_name = new_module_alias.rsplit(".", 1)
                    self._current_references.append(
                        "from %s import %s as %s"
                        % (
                            self._get_relative_import(
                                new_parent,
                                alias,
                                hasattr(self._current_module, "__path__"),
                            ),
                            new_name,
                            objname,
                        )
                    )
                else:
                    debug.stubgen_exec("Skipping child module %s" % obj.__name__)
            else:
                parent_module = inspect.getmodule(obj)
                # For objects we include:
                #  - stuff that is a functools.partial (these are all the decorators;
                #    we could be more specific but good enough for now) for root module.
                #    We also include the step decorator (it's from metaflow.decorators
                #    which is typically excluded)
                #  - Stuff that is defined in this module itself
                #  - a reference to anything in the modules we will process later
                #    (so we don't duplicate a ton of times)

                if (
                    parent_module is None
                    or (
                        name + "." == self._root_module
                        and (
                            (parent_module.__name__.startswith("functools"))
                            or obj == step
                        )
                    )
                    or parent_module.__name__ == name
                ):
                    debug.stubgen_exec("Adding object %s to process" % objname)
                    self._current_objects[objname] = obj

                elif not any(
                    [
                        parent_module.__name__.startswith(p)
                        for p in self._exclude_modules
                    ]
                ) and any(
                    [parent_module.__name__.startswith(p) for p in self._safe_modules]
                ):
                    parent_alias = self._get_module_name_alias(parent_module.__name__)

                    relative_import = self._get_relative_import(
                        parent_alias, alias, hasattr(self._current_module, "__path__")
                    )

                    debug.stubgen_exec(
                        "Adding reference %s and adding module %s as %s"
                        % (objname, parent_module.__name__, parent_alias)
                    )
                    obj_import_name = getattr(obj, "__name__", objname)
                    if obj_import_name == "<lambda>":
                        # We have one case of this
                        obj_import_name = objname
                    self._current_references.append(
                        "from %s import %s as %s"
                        % (relative_import, obj_import_name, objname)
                    )
                    self._pending_modules.append((parent_alias, parent_module.__name__))
                else:
                    debug.stubgen_exec("Skipping object %s" % objname)

    def _get_element_name_with_module(
        self, element: Union[TypeVar, type, Any], force_import=False
    ) -> str:
        # The element can be a string, for example "def f() -> 'SameClass':..."
        def _add_to_import(name):
            if name != self._current_module_name:
                self._imports.add(name)

        def _add_to_typing_check(name, is_module=False):
            if name == "None":
                return
            if is_module:
                self._typing_imports.add(name)
            else:
                splits = name.rsplit(".", 1)
                if len(splits) > 1 and not (
                    len(splits) == 2 and splits[0] == self._current_module_name
                ):
                    # We don't add things that are just one name -- probably things within
                    # the current file
                    self._typing_imports.add(splits[0])

        def _format_qualified_class_name(cls: type) -> str:
            """Helper to format a class with its qualified module name"""
            # Special case for NoneType - return None
            if cls.__name__ == "NoneType":
                return "None"

            module = inspect.getmodule(cls)
            if (
                module
                and module.__name__ != "builtins"
                and module.__name__ != "__main__"
            ):
                module_name = self._get_module_name_alias(module.__name__)
                _add_to_typing_check(module_name, is_module=True)
                return f"{module_name}.{cls.__name__}"
            else:
                return cls.__name__

        if isinstance(element, str):
            # Special case for self referential things (particularly in a class)
            if element == self._current_name:
                return '"%s"' % element
            # We first try to eval the annotation because with the annotations future
            # it is always a string
            try:
                potential_element = eval(
                    element,
                    (
                        self._current_parent_module.__dict__
                        if self._current_parent_module
                        else None
                    ),
                )
                if potential_element:
                    element = potential_element
            except:
                pass

        if isinstance(element, str):
            # If we are in our "safe" modules, make sure we alias properly
            if any(element.startswith(x) for x in self._safe_modules):
                element = self._get_module_name_alias(element)
            _add_to_typing_check(element)
            return '"%s"' % element
        # 3.10+ has NewType as a class but not before so hack around to check for NewType
        elif isinstance(element, TypeVar) or hasattr(element, "__supertype__"):
            if not element.__name__ in self._typevars:
                self._typevars[element.__name__] = element
            return element.__name__
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

            module_name = self._get_module_name_alias(module.__name__)
            if force_import:
                _add_to_import(module_name.split(".")[0])
            _add_to_typing_check(module_name, is_module=True)
            if module_name != self._current_module_name:
                return "{0}.{1}".format(module_name, element.__name__)
            else:
                return element.__name__
        elif isinstance(element, type(Ellipsis)):
            return "..."
        elif isinstance(element, typing._GenericAlias):
            # We need to check things recursively in __args__ if it exists
            args_str = []
            for arg in getattr(element, "__args__", []):
                # Special handling for class objects in type arguments
                if isinstance(arg, type):
                    args_str.append(_format_qualified_class_name(arg))
                else:
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
                # Handle the case where we have a generic type without a _name
                origin = element.__origin__
                if isinstance(origin, type):
                    origin_str = _format_qualified_class_name(origin)
                else:
                    origin_str = str(origin)
                return "%s[%s]" % (origin_str, ", ".join(args_str))
        elif isinstance(element, ForwardRef):
            f_arg = self._get_module_name_alias(element.__forward_arg__)
            _add_to_typing_check(f_arg)
            return '"%s"' % f_arg
        elif inspect.getmodule(element) == inspect.getmodule(typing):
            _add_to_import("typing")
            # Special handling for NamedTuple which is a function
            if hasattr(element, "__name__") and element.__name__ == "NamedTuple":
                return "typing.NamedTuple"
            return str(element)
        else:
            if hasattr(element, "__module__"):
                elem_module = self._get_module_name_alias(element.__module__)
                if elem_module == "builtins":
                    return getattr(element, "__name__", str(element))
                _add_to_typing_check(elem_module, is_module=True)
                return "{0}.{1}".format(
                    elem_module, getattr(element, "__name__", element)
                )
            else:
                # A constant
                return str(element)

    def _exploit_annotation(self, annotation: Any, starting: str = ": ") -> str:
        annotation_string = ""
        if annotation and annotation != inspect.Parameter.empty:
            annotation_string += starting + self._get_element_name_with_module(
                annotation
            )
        return annotation_string

    def _generate_class_stub(self, name: str, clazz: type) -> str:
        debug.stubgen_exec("Generating class stub for %s" % name)
        skip_init = issubclass(clazz, (TriggeredRun, DeployedFlow))
        if issubclass(clazz, DeployerImpl):
            if clazz.TYPE is not None:
                clazz_type = clazz.TYPE.replace("-", "_")
                self._deployer_injected_methods.setdefault(clazz_type, {})[
                    "deployer"
                ] = (self._current_module_name + "." + name)

        # Handle TypedDict gracefully for Python 3.7 compatibility
        # _TypedDictMeta is not available in Python 3.7
        typed_dict_meta = getattr(typing, "_TypedDictMeta", None)
        if typed_dict_meta is not None and isinstance(clazz, typed_dict_meta):
            self._sub_module_imports.add(("typing", "TypedDict"))
            total_flag = getattr(clazz, "__total__", False)
            buff = StringIO()
            # Emit the TypedDict base and total flag
            buff.write(f"class {name}(TypedDict, total={total_flag}):\n")
            # Write out each field from __annotations__
            for field_name, field_type in clazz.__annotations__.items():
                ann = self._get_element_name_with_module(field_type)
                buff.write(f"{TAB}{field_name}: {ann}\n")
            return buff.getvalue()

        buff = StringIO()
        # Class prototype
        buff.write("class " + name.split(".")[-1] + "(")

        # Add super classes
        for c in clazz.__bases__:
            name_with_module = self._get_element_name_with_module(c, force_import=True)
            buff.write(name_with_module + ", ")

        # Add metaclass
        name_with_module = self._get_element_name_with_module(
            clazz.__class__, force_import=True
        )
        buff.write("metaclass=" + name_with_module + "):\n")

        # Add class docstring
        if clazz.__doc__:
            buff.write('%s"""\n' % TAB)
            my_doc = inspect.cleandoc(clazz.__doc__)
            init_blank = True
            for line in my_doc.split("\n"):
                if init_blank and len(line.strip()) == 0:
                    continue
                init_blank = False
                buff.write("%s%s\n" % (TAB, line.rstrip()))
            buff.write('%s"""\n' % TAB)

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
                if skip_init:
                    continue
                init_func = element
            elif key == "__annotations__":
                annotation_dict = element
            if inspect.isfunction(element):
                if not element.__name__.startswith("_") or element.__name__.startswith(
                    "__"
                ):
                    if (
                        clazz == Deployer
                        and element.__name__ in self._deployer_injected_methods
                    ):
                        # This is a method that was injected. It has docs but we need
                        # to parse it to generate the proper signature
                        func_doc = inspect.cleandoc(element.__doc__)
                        docs = split_docs(
                            func_doc,
                            [
                                ("func_doc", StartEnd(0, 0)),
                                (
                                    "param_doc",
                                    param_section_header.search(func_doc)
                                    or StartEnd(len(func_doc), len(func_doc)),
                                ),
                                (
                                    "return_doc",
                                    return_section_header.search(func_doc)
                                    or StartEnd(len(func_doc), len(func_doc)),
                                ),
                            ],
                        )

                        parameters, _ = parse_params_from_doc(docs["param_doc"])
                        return_type = self._deployer_injected_methods[element.__name__][
                            "deployer"
                        ]

                        buff.write(
                            self._generate_function_stub(
                                key,
                                element,
                                sign=[
                                    inspect.Signature(
                                        parameters=[
                                            inspect.Parameter(
                                                "self",
                                                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                            )
                                        ]
                                        + parameters,
                                        return_annotation=return_type,
                                    )
                                ],
                                indentation=TAB,
                                deco=func_deco,
                            )
                        )
                    elif (
                        clazz == DeployedFlow and element.__name__ == "from_deployment"
                    ):
                        # We simply update the signature to list the return
                        # type as a union of all possible deployers
                        func_doc = inspect.cleandoc(element.__doc__)
                        docs = split_docs(
                            func_doc,
                            [
                                ("func_doc", StartEnd(0, 0)),
                                (
                                    "param_doc",
                                    param_section_header.search(func_doc)
                                    or StartEnd(len(func_doc), len(func_doc)),
                                ),
                                (
                                    "return_doc",
                                    return_section_header.search(func_doc)
                                    or StartEnd(len(func_doc), len(func_doc)),
                                ),
                            ],
                        )

                        parameters, _ = parse_params_from_doc(docs["param_doc"])

                        def _create_multi_type(*l):
                            return typing.Union[l]

                        all_types = [
                            v["from_deployment"][0]
                            for v in self._deployer_injected_methods.values()
                        ]

                        if len(all_types) > 1:
                            return_type = _create_multi_type(*all_types)
                        else:
                            return_type = all_types[0] if len(all_types) else None

                        buff.write(
                            self._generate_function_stub(
                                key,
                                element,
                                sign=[
                                    inspect.Signature(
                                        parameters=[
                                            inspect.Parameter(
                                                "cls",
                                                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                            )
                                        ]
                                        + parameters,
                                        return_annotation=return_type,
                                    )
                                ],
                                indentation=TAB,
                                doc=docs["func_doc"]
                                + "\n\nParameters\n----------\n"
                                + docs["param_doc"]
                                + "\n\nReturns\n-------\n"
                                + "%s\nA `DeployedFlow` object" % str(return_type),
                                deco=func_deco,
                            )
                        )
                    elif (
                        clazz == DeployedFlow
                        and element.__name__.startswith("from_")
                        and element.__name__[5:] in self._deployer_injected_methods
                    ):
                        # Get the doc from the from_deployment method stored in
                        # self._deployer_injected_methods
                        func_doc = inspect.cleandoc(
                            self._deployer_injected_methods[element.__name__[5:]][
                                "from_deployment"
                            ][1]
                            or ""
                        )
                        docs = split_docs(
                            func_doc,
                            [
                                ("func_doc", StartEnd(0, 0)),
                                (
                                    "param_doc",
                                    param_section_header.search(func_doc)
                                    or StartEnd(len(func_doc), len(func_doc)),
                                ),
                                (
                                    "return_doc",
                                    return_section_header.search(func_doc)
                                    or StartEnd(len(func_doc), len(func_doc)),
                                ),
                            ],
                        )

                        parameters, _ = parse_params_from_doc(docs["param_doc"])
                        return_type = self._deployer_injected_methods[
                            element.__name__[5:]
                        ]["from_deployment"][0]

                        buff.write(
                            self._generate_function_stub(
                                key,
                                element,
                                sign=[
                                    inspect.Signature(
                                        parameters=[
                                            inspect.Parameter(
                                                "cls",
                                                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                            )
                                        ]
                                        + parameters,
                                        return_annotation=return_type,
                                    )
                                ],
                                indentation=TAB,
                                doc=docs["func_doc"]
                                + "\n\nParameters\n----------\n"
                                + docs["param_doc"]
                                + "\n\nReturns\n-------\n"
                                + docs["return_doc"],
                                deco=func_deco,
                            )
                        )
                    else:
                        if (
                            issubclass(clazz, DeployedFlow)
                            and clazz.TYPE is not None
                            and key == "from_deployment"
                        ):
                            clazz_type = clazz.TYPE.replace("-", "_")
                            # Record docstring for this function
                            self._deployer_injected_methods.setdefault(clazz_type, {})[
                                "from_deployment"
                            ] = (
                                self._current_module_name + "." + name,
                                element.__doc__,
                            )
                        buff.write(
                            self._generate_function_stub(
                                key,
                                element,
                                indentation=TAB,
                                deco=func_deco,
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

        # Special handling of classes that have injected methods
        if clazz == Current:
            # Multiple decorators can add the same object (trigger and trigger_on_finish)
            # as examples so we sort it out.
            resulting_dict = (
                dict()
            )  # type Dict[str, List[inspect.Signature, str, List[str]]]
            for deco_name, addl_current in self._addl_current.items():
                for name, (sign, doc) in addl_current.items():
                    r = resulting_dict.setdefault(name, [sign, doc, []])
                    r[2].append("@%s" % deco_name)
            for name, (sign, doc, decos) in resulting_dict.items():
                buff.write(
                    self._generate_function_stub(
                        name,
                        sign=[sign],
                        indentation=TAB,
                        doc="(only in the presence of the %s decorator%s)\n\n"
                        % (", or ".join(decos), "" if len(decos) == 1 else "s")
                        + doc,
                        deco="@property",
                    )
                )

        if not skip_init and init_func is None and annotation_dict:
            buff.write(
                self._generate_function_stub(
                    "__init__",
                    func=None,
                    sign=[
                        inspect.Signature(
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
                        )
                    ],
                    indentation=TAB,
                )
            )
        buff.write("%s...\n" % TAB)

        return buff.getvalue()

    def _extract_signature_from_decorator(
        self, name: str, raw_doc: Optional[str], is_flow_decorator: bool = False
    ) -> Optional[List[Tuple[inspect.Signature, str]]]:
        # TODO: This only handles the `Parameters` section for now; we are
        # using it only to parse the documentation for step/flow decorators so
        # this is enough for now but it could be extended more.
        # Inspired from:
        # https://github.com/rr-/docstring_parser/blob/master/docstring_parser/numpydoc.py
        if raw_doc is None:
            return None

        if not "FlowSpecDerived" in self._typevars:
            self._typevars["FlowSpecDerived"] = FlowSpecDerived
            self._typevars["StepFlag"] = StepFlag

        raw_doc = inspect.cleandoc(raw_doc)
        section_boundaries = [
            ("func_doc", StartEnd(0, 0)),
            (
                "param_doc",
                param_section_header.search(raw_doc)
                or StartEnd(len(raw_doc), len(raw_doc)),
            ),
            (
                "add_to_current_doc",
                add_to_current_header.search(raw_doc)
                or StartEnd(len(raw_doc), len(raw_doc)),
            ),
        ]

        docs = split_docs(raw_doc, section_boundaries)
        parameters, no_arg_version = parse_params_from_doc(docs["param_doc"])

        if docs["add_to_current_doc"]:
            self._addl_current[name] = parse_add_to_docs(docs["add_to_current_doc"])

        result = []
        if no_arg_version:
            if is_flow_decorator:
                if docs["param_doc"]:
                    result.append(
                        (
                            inspect.Signature(
                                parameters=parameters,
                                return_annotation=Callable[
                                    [typing.Type[FlowSpecDerived]],
                                    typing.Type[FlowSpecDerived],
                                ],
                            ),
                            "",
                        )
                    )
                result.append(
                    (
                        inspect.Signature(
                            parameters=[
                                inspect.Parameter(
                                    name="f",
                                    kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                    annotation=typing.Type[FlowSpecDerived],
                                )
                            ],
                            return_annotation=typing.Type[FlowSpecDerived],
                        ),
                        "",
                    ),
                )
            else:
                if docs["param_doc"]:
                    result.append(
                        (
                            inspect.Signature(
                                parameters=parameters,
                                return_annotation=typing.Callable[
                                    [MetaflowStepFunction], MetaflowStepFunction
                                ],
                            ),
                            "",
                        )
                    )
                result.extend(
                    [
                        (
                            inspect.Signature(
                                parameters=[
                                    inspect.Parameter(
                                        name="f",
                                        kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                        annotation=Callable[
                                            [FlowSpecDerived, StepFlag], None
                                        ],
                                    )
                                ],
                                return_annotation=Callable[
                                    [FlowSpecDerived, StepFlag], None
                                ],
                            ),
                            "",
                        ),
                        (
                            inspect.Signature(
                                parameters=[
                                    inspect.Parameter(
                                        name="f",
                                        kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                        annotation=Callable[
                                            [FlowSpecDerived, Any, StepFlag], None
                                        ],
                                    )
                                ],
                                return_annotation=Callable[
                                    [FlowSpecDerived, Any, StepFlag], None
                                ],
                            ),
                            "",
                        ),
                    ]
                )

        if is_flow_decorator:
            result = result + [
                (
                    inspect.Signature(
                        parameters=(
                            [
                                inspect.Parameter(
                                    name="f",
                                    kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                    annotation=Optional[typing.Type[FlowSpecDerived]],
                                    default=(
                                        None
                                        if no_arg_version
                                        else inspect.Parameter.empty
                                    ),
                                )
                            ]
                            + parameters
                            if no_arg_version
                            else [] + parameters
                        ),
                        return_annotation=(
                            inspect.Signature.empty
                            if no_arg_version
                            else Callable[
                                [typing.Type[FlowSpecDerived]],
                                typing.Type[FlowSpecDerived],
                            ]
                        ),
                    ),
                    "",
                ),
            ]
        else:
            result = result + [
                (
                    inspect.Signature(
                        parameters=(
                            [
                                inspect.Parameter(
                                    name="f",
                                    kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                    annotation=Optional[MetaflowStepFunction],
                                    default=(
                                        None
                                        if no_arg_version
                                        else inspect.Parameter.empty
                                    ),
                                )
                            ]
                            + parameters
                            if no_arg_version
                            else [] + parameters
                        ),
                        return_annotation=(
                            inspect.Signature.empty
                            if no_arg_version
                            else typing.Callable[
                                [MetaflowStepFunction], MetaflowStepFunction
                            ]
                        ),
                    ),
                    "",
                ),
            ]
        if len(result) == 2:
            # If we only have one overload -- we don't need it at all. Happens for
            # flow-level decorators that don't take any arguments
            result = result[1:]
        # Add doc to first and last overloads. Jedi uses the last one and pycharm
        # the first one. Go figure.
        result_docstring = docs["func_doc"]
        if docs["param_doc"]:
            result_docstring += "\nParameters\n----------\n" + docs["param_doc"]
        result[0] = (
            result[0][0],
            result_docstring,
        )
        result[-1] = (
            result[-1][0],
            result_docstring,
        )
        return result

    def _generate_function_stub(
        self,
        name: str,
        func: Optional[Union[Callable, classmethod]] = None,
        sign: Optional[List[inspect.Signature]] = None,
        indentation: Optional[str] = None,
        doc: Optional[str] = None,
        deco: Optional[str] = None,
    ) -> str:
        debug.stubgen_exec("Generating function stub for %s" % name)

        def exploit_default(default_value: Any) -> Optional[str]:
            if default_value == inspect.Parameter.empty:
                return None
            if type(default_value).__module__ == "builtins":
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
                elif isinstance(default_value, str):
                    return repr(default_value)  # Use repr() for proper escaping
                elif isinstance(default_value, (int, float, bool)):
                    return str(default_value)
                elif default_value is None:
                    return "None"
                else:
                    return "..."  # For other built-in types not explicitly handled
            elif inspect.isclass(default_value) or inspect.isfunction(default_value):
                if default_value.__module__ == "builtins":
                    return default_value.__name__
                else:
                    self._typing_imports.add(default_value.__module__)
                    return ".".join([default_value.__module__, default_value.__name__])
            else:
                return "..."  # For complex objects like class instances

        buff = StringIO()
        if sign is None and func is None:
            raise RuntimeError(
                "Cannot generate stub for function %s with either a function or signature"
                % name
            )
        try:
            sign = sign or [inspect.signature(cast(Callable, func))]
        except ValueError:
            # In 3.7, NamedTuples have properties that then give an operator.itemgetter
            # which doesn't have a signature. We ignore for now. It doesn't have much
            # value
            return ""
        doc = doc or func.__doc__
        if doc == "STUBGEN_IGNORE":
            # Ignore methods that have STUBGEN_IGNORE. Used to ignore certain
            # methods for the Deployer
            return ""
        indentation = indentation or ""

        # Deal with overload annotations -- the last one will be non overloaded and
        # will be the one that shows up as the type hint (for Jedi and PyCharm which
        # don't handle overloads as well)
        do_overload = False
        if sign and len(sign) > 1:
            do_overload = True
        for count, my_sign in enumerate(sign):
            if count > 0:
                buff.write("\n")

            if do_overload and count < len(sign) - 1:
                # According to mypy, we should have this on all variants but
                # some IDEs seem to prefer if there is one non-overloaded
                # This also changes our checks so if changing, modify tests
                buff.write(indentation + "@typing.overload\n")
            if deco:
                buff.write(indentation + deco + "\n")
            buff.write(indentation + "def " + name + "(")
            kw_only_param = False
            has_var_args = False
            for i, (par_name, parameter) in enumerate(my_sign.parameters.items()):
                annotation = self._exploit_annotation(parameter.annotation)
                default = exploit_default(parameter.default)

                if (
                    kw_only_param
                    and not has_var_args
                    and parameter.kind != inspect.Parameter.KEYWORD_ONLY
                ):
                    raise RuntimeError(
                        "In function '%s': cannot have a positional parameter after a "
                        "keyword only parameter" % name
                    )

                if (
                    parameter.kind == inspect.Parameter.KEYWORD_ONLY
                    and not kw_only_param
                    and not has_var_args
                ):
                    kw_only_param = True
                    buff.write("*, ")
                if parameter.kind == inspect.Parameter.VAR_KEYWORD:
                    par_name = "**%s" % par_name
                elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
                    has_var_args = True
                    par_name = "*%s" % par_name

                if default:
                    buff.write(par_name + annotation + " = " + default)
                else:
                    buff.write(par_name + annotation)

                if i < len(my_sign.parameters) - 1:
                    buff.write(", ")
            ret_annotation = self._exploit_annotation(
                my_sign.return_annotation, starting=" -> "
            )
            buff.write(")" + ret_annotation + ":\n")

            if (count == 0 or count == len(sign) - 1) and doc is not None:
                buff.write('%s%s"""\n' % (indentation, TAB))
                my_doc = inspect.cleandoc(doc)
                init_blank = True
                for line in my_doc.split("\n"):
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
            self._current_name = name
            if inspect.isclass(attr):
                self._stubs.append(self._generate_class_stub(name, attr))
            elif inspect.isfunction(attr):
                # Special handling of the `step` function where we want to add an
                # overload. This is just a single case so we don't make it general.
                # Unfortunately, when iterating, it doesn't see the @overload
                if (
                    name == "step"
                    and self._current_module_name == self._root_module[:-1]
                ):
                    self._stubs.append(
                        self._generate_function_stub(
                            name,
                            func=attr,
                            sign=[
                                inspect.Signature(
                                    parameters=[
                                        inspect.Parameter(
                                            name="f",
                                            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                            annotation=Callable[
                                                [FlowSpecDerived], None
                                            ],
                                        )
                                    ],
                                    return_annotation=Callable[
                                        [FlowSpecDerived, StepFlag], None
                                    ],
                                ),
                                inspect.Signature(
                                    parameters=[
                                        inspect.Parameter(
                                            name="f",
                                            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                            annotation=Callable[
                                                [FlowSpecDerived, Any], None
                                            ],
                                        )
                                    ],
                                    return_annotation=Callable[
                                        [FlowSpecDerived, Any, StepFlag], None
                                    ],
                                ),
                                inspect.signature(attr),
                            ],
                        )
                    )
                else:
                    self._stubs.append(self._generate_function_stub(name, attr))
            elif isinstance(attr, functools.partial):
                if issubclass(attr.args[0], Decorator):
                    # Special case where we are going to extract the parameters from
                    # the docstring to make the decorator look nicer
                    res = self._extract_signature_from_decorator(
                        name,
                        attr.args[0].__doc__,
                        is_flow_decorator=issubclass(attr.args[0], FlowDecorator),
                    )
                    if res:
                        self._stubs.append(
                            self._generate_function_stub(
                                name,
                                func=attr.func,
                                sign=[r[0] for r in res],
                                doc=res[-1][1],
                            )
                        )
                    else:
                        # print(
                        #    "WARNING: Could not extract decorator signature for %s"
                        #    % name
                        # )
                        pass
                else:
                    self._stubs.append(
                        self._generate_function_stub(
                            name, attr.func, doc=attr.args[0].__doc__
                        )
                    )
            elif not inspect.ismodule(attr):
                self._stubs.append(self._generate_generic_stub(name, attr))

    def _write_header(self, f, width):
        title_line = "Auto-generated Metaflow stub file"
        title_white_space = (width - len(title_line)) / 2
        title_line = "#%s%s%s#\n" % (
            " " * math.floor(title_white_space),
            title_line,
            " " * math.ceil(title_white_space),
        )
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

    def write_out(self):
        out_dir = self._output_dir
        os.makedirs(out_dir, exist_ok=True)
        # Write out py.typed (pylance seems to require it even though it is not
        # required in PEP 561) as well as a file we will use to check the "version"
        # of the stubs -- this helps to inform the user if the stubs were generated
        # for another version of Metaflow.
        pathlib.Path(os.path.join(out_dir, "py.typed")).touch()
        if self._write_generated_for:
            pathlib.Path(os.path.join(out_dir, "generated_for.txt")).write_text(
                "%s %s"
                % (self._mf_version, datetime.fromtimestamp(time.time()).isoformat())
            )
        post_process_modules = []
        is_post_processing = False
        while len(self._pending_modules) != 0 or len(post_process_modules) != 0:
            if is_post_processing or len(self._pending_modules) == 0:
                is_post_processing = True
                module_alias, module_name = post_process_modules.pop(0)
            else:
                module_alias, module_name = self._pending_modules.pop(0)
            # Skip vendored stuff
            if module_alias.startswith("metaflow._vendor") or module_name.startswith(
                "metaflow._vendor"
            ):
                continue
            # We delay current module and deployer module to the end since they
            # depend on info we gather elsewhere
            if (
                module_alias
                in (
                    METAFLOW_CURRENT_MODULE_NAME,
                    METAFLOW_DEPLOYER_MODULE_NAME,
                )
                and len(self._pending_modules) != 0
            ):
                post_process_modules.append((module_alias, module_name))
                continue
            if module_alias in self._done_modules:
                continue
            self._done_modules.add(module_alias)
            # If not, we process the module
            self._reset()
            self._get_module(module_alias, module_name)
            if module_name == "metaflow" and not is_post_processing:
                # We will want to regenerate this at the end to take into account
                # any changes to the Deployer
                post_process_modules.append((module_name, module_name))
                self._done_modules.remove(module_name)
                continue
            self._generate_stubs()

            if hasattr(self._current_module, "__path__"):
                # This is a package (so a directory) and we are dealing with
                # a __init__.pyi type of case
                dir_path = os.path.join(self._output_dir, *module_alias.split(".")[1:])
            else:
                # This is NOT a package so the original source file is not a __init__.py
                dir_path = os.path.join(
                    self._output_dir, *module_alias.split(".")[1:-1]
                )
            out_file = os.path.join(
                dir_path, os.path.basename(self._current_module.__file__) + "i"
            )

            width = 100

            os.makedirs(os.path.dirname(out_file), exist_ok=True)
            # We want to make sure we always have a __init__.pyi in the directories
            # we are creating
            parts = dir_path.split(os.sep)[len(self._output_dir.split(os.sep)) :]
            for i in range(1, len(parts) + 1):
                init_file_path = os.path.join(
                    self._output_dir, *parts[:i], "__init__.pyi"
                )
                if not os.path.exists(init_file_path):
                    with open(init_file_path, mode="w", encoding="utf-8") as f:
                        self._write_header(f, width)

            with open(out_file, mode="w", encoding="utf-8") as f:
                self._write_header(f, width)

                f.write("from __future__ import annotations\n\n")
                imported_typing = False
                for module in self._imports:
                    f.write("import " + module + "\n")
                    if module == "typing":
                        imported_typing = True
                for module, sub_module in self._sub_module_imports:
                    f.write(f"from {module} import {sub_module}\n")
                if self._typing_imports:
                    if not imported_typing:
                        f.write("import typing\n")
                        imported_typing = True
                    f.write("if typing.TYPE_CHECKING:\n")
                    for module in self._typing_imports:
                        f.write(TAB + "import " + module + "\n")
                if self._typevars:
                    if not imported_typing:
                        f.write("import typing\n")
                        imported_typing = True
                    for type_name, type_var in self._typevars.items():
                        if isinstance(type_var, TypeVar):
                            f.write(
                                "%s = %s\n" % (type_name, type_var_to_str(type_var))
                            )
                        else:
                            f.write(
                                "%s = %s\n" % (type_name, new_type_to_str(type_var))
                            )
                f.write("\n")
                for import_line in self._current_references:
                    f.write(import_line + "\n")
                f.write("\n")
                for stub in self._stubs:
                    f.write(stub + "\n")
            if is_post_processing:
                # Don't consider any pending modules if we are post processing
                self._pending_modules.clear()


if __name__ == "__main__":
    gen = StubGenerator("./stubs")
    gen.write_out()
