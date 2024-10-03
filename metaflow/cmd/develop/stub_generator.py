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
from metaflow.runner.deployer import DeployedFlow, Deployer, DeployerImpl, TriggeredRun

TAB = "    "
METAFLOW_CURRENT_MODULE_NAME = "metaflow.metaflow_current"
METAFLOW_DEPLOYER_MODULE_NAME = "metaflow.runner.deployer"

param_section_header = re.compile(r"Parameters\s*\n----------\s*\n", flags=re.M)
add_to_current_header = re.compile(
    r"MF Add To Current\s*\n-----------------\s*\n", flags=re.M
)
add_to_deployed_flow_header = re.compile(
    r"MF Add To DeployedFlow\s*\n----------------------\s*\n", flags=re.M
)
add_to_triggered_run_header = re.compile(
    r"MF Add To TriggeredRun\s*\n----------------------\s*\n", flags=re.M
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
        self._pending_modules = ["metaflow"]  # type: List[str]
        self._pending_modules.extend(get_aliased_modules())
        self._root_module = "metaflow."
        self._safe_modules = ["metaflow.", "metaflow_extensions."]

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
                "metaflow.plugins.metadata",
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

        # NOTE: the : at the beginning of the string is normal -- we will match
        # this string with the annotation string for the type. See _generate_function_stub
        self._injected_types = set(
            [
                ": metaflow.runner.deployer.TriggeredRun",
                ": metaflow.runner.deployer.DeployedFlow",
                ": metaflow.metaflow_current.Current",
            ]
        )
        self._done_modules = set()  # type: Set[str]
        self._output_dir = output_dir
        self._mf_version = get_version()

        # Contains information to add to the Current object (injected by decorators)
        self._addl_current = (
            dict()
        )  # type: Dict[str, Dict[str, Union[Tuple[inspect.Signature, str], str]]]

        # Contains methods to inject in the Deployer object (injected by DeployerImpl)
        self._addl_deployer = (
            dict()
        )  # type: Dict[str, Union[Tuple[inspect.Signature, str], str]]

        # Same for methods injected to the DeployedFlow object
        self._addl_deployed_flow = (
            dict()
        )  # type: Dict[Tuple[str, str], Union[Tuple[inspect.Signature, str], str]]

        # And finally, same for TriggeredRun
        self._addl_triggered_run = (
            dict()
        )  # type: Dict[Tuple[str, str], Union[Tuple[inspect.Signature, str], str]]

        # Contains relevant generated stubs so we can reference to them from
        # the DeployedFlow or TriggeredRun classes. This will effectively contain
        # stubs where the first argument is one of self._injected_types
        # The key is the FQN of the function and the value is the stub that can
        # be used as is (first argument is already a `self`)
        self._injected_stubs = {}  # type: Dict[str, str]

        self._reset()

    def _reset(self):
        # "Globals" that are used throughout processing. This is not the cleanest
        # but simplifies code quite a bit.

        # Imports that are needed at the top of the file
        self._imports = set()  # type: Set[str]
        # Typing imports (behind if TYPE_CHECKING) that are needed at the top of the file
        self._typing_imports = set()  # type: Set[str]
        # Typevars that are defined
        self._typevars = dict()  # type: Dict[str, Union[TypeVar, type]]
        # Current objects in the file being processed
        self._current_objects = {}  # type: Dict[str, Any]
        # Current stubs in the file being processed
        self._stubs = []  # type: List[str]

        # These have a shorter "scope"
        # Current parent module of the object being processed -- used to determine
        # the "globals()"
        self._current_parent_module = None  # type: Optional[ModuleType]

    def _get_module(self, name):
        debug.stubgen_exec("Analyzing module %s ..." % name)
        self._current_module = importlib.import_module(name)
        self._current_module_name = name
        for objname, obj in self._current_module.__dict__.items():
            if objname.startswith("_"):
                debug.stubgen_exec(
                    "Skipping object because it starts with _ %s" % objname
                )
                continue
            if inspect.ismodule(obj):
                # Only consider modules that are part of the root module
                if (
                    obj.__name__.startswith(self._root_module)
                    and not obj.__name__ in self._exclude_modules
                ):
                    debug.stubgen_exec(
                        "Adding child module %s to process" % obj.__name__
                    )
                    self._pending_modules.append(obj.__name__)
                else:
                    debug.stubgen_exec("Skipping child module %s" % obj.__name__)
            else:
                parent_module = inspect.getmodule(obj)
                # For objects we include:
                #  - stuff that is a functools.partial (these are all the decorators;
                #    we could be more specific but good enough for now) for root module.
                #    We also include the step decorator (it's from metaflow.decorators
                #    which is typically excluded)
                #  - otherwise, anything that is in safe_modules. Note this may include
                #    a bit much (all the imports)
                if (
                    parent_module is None
                    or (
                        name + "." == self._root_module
                        and (
                            (parent_module.__name__.startswith("functools"))
                            or obj == step
                        )
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
                    debug.stubgen_exec("Adding object %s to process" % objname)
                    self._current_objects[objname] = obj
                else:
                    debug.stubgen_exec("Skipping object %s" % objname)
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
                    debug.stubgen_exec(
                        "Adding module of child object %s to process"
                        % parent_module.__name__,
                    )
                    self._pending_modules.append(parent_module.__name__)

    def _get_element_name_with_module(self, element: Union[TypeVar, type, Any]) -> str:
        # The element can be a string, for example "def f() -> 'SameClass':..."
        def _add_to_import(name):
            if name != self._current_module_name:
                self._imports.add(name)

        def _add_to_typing_check(name, is_module=False):
            # if name != self._current_module_name:
            #    self._typing_imports.add(name)
            #
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

            _add_to_typing_check(module.__name__, is_module=True)
            if module.__name__ != self._current_module_name:
                return "{0}.{1}".format(module.__name__, element.__name__)
            else:
                return element.__name__
        elif isinstance(element, type(Ellipsis)):
            return "..."
        # elif (
        #    isinstance(element, typing._GenericAlias)
        #    and hasattr(element, "_name")
        #    and element._name in ("List", "Tuple", "Dict", "Set")
        # ):
        #    # 3.7 has these as _GenericAlias but they don't behave like the ones in 3.10
        #    _add_to_import("typing")
        #    return str(element)
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
            if hasattr(element, "__name__") and element.__name__ == "NamedTuple":
                return "typing.NamedTuple"
            return str(element)
        else:
            raise RuntimeError(
                "Does not handle element %s of type %s" % (str(element), type(element))
            )

    def _exploit_annotation(self, annotation: Any, starting: str = ": ") -> str:
        annotation_string = ""
        if annotation and annotation != inspect.Parameter.empty:
            annotation_string += starting + self._get_element_name_with_module(
                annotation
            )
        return annotation_string

    def _generate_class_stub(self, name: str, clazz: type) -> str:
        if issubclass(clazz, DeployerImpl):
            # This is a special case -- the init method will be injected in the
            # Deployer class as from_TYPE
            # We will only parse the class docstring which will contain the parameters
            # as well as anything added to the triggeredrun and deployedflow objects
            clazz_doc = clazz.__doc__
            print("In Class %s with doc %s" % (name, clazz_doc))
            if clazz.TYPE is None:
                # Base DeployerImpl class -- we skip
                return ""
            clazz_type = clazz.TYPE.replace("-", "_")
            if not clazz_doc:
                # There is no doc -- this is probably a problem but can't do much
                return ""
            clazz_doc = inspect.cleandoc(clazz_doc)
            docs = split_docs(
                clazz_doc,
                [
                    ("func_doc", StartEnd(0, 0)),
                    (
                        "param_doc",
                        param_section_header.search(clazz_doc)
                        or StartEnd(len(clazz_doc), len(clazz_doc)),
                    ),
                    (
                        "add_to_deployed_flow_doc",
                        add_to_deployed_flow_header.search(clazz_doc)
                        or StartEnd(len(clazz_doc), len(clazz_doc)),
                    ),
                    (
                        "add_to_triggered_run_doc",
                        add_to_triggered_run_header.search(clazz_doc)
                        or StartEnd(len(clazz_doc), len(clazz_doc)),
                    ),
                ],
            )

            # We are expecting the doc to follow the following format:
            # <doc for the function>
            # Parameters
            # ----------
            # <Parameters for the function>
            # MF Add To DeployedFlow
            # ----------------------
            # <Things to add to the DeployedFlow object>
            # MF Add To TriggeredRun
            # ----------------------
            # <Things to add to the TriggeredRun object>
            #
            # For both of the DeployedFlow or TriggeredRun part, it can either be the
            # name of a function (which we will have extracted or will extract in
            # self._injected_stubs) or a property (regular values can still be
            # documented as properties).
            #
            parameters, _ = parse_params_from_doc(docs["param_doc"])
            self._addl_deployer[clazz_type] = (
                inspect.Signature(
                    parameters=parameters,
                    return_annotation="Deployer",
                ),
                docs["func_doc"] + "\n\n" + docs["param_doc"],
            )
            for method_name, method_info in parse_add_to_docs(
                docs["add_to_deployed_flow_doc"]
            ).items():
                self._addl_deployed_flow[(clazz_type, method_name)] = method_info
            for method_name, method_info in parse_add_to_docs(
                docs["add_to_triggered_run_doc"]
            ).items():
                self._addl_triggered_run[(clazz_type, method_name)] = method_info
            return ""

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
                        )[0]
                    )
            elif isinstance(element, property):
                if element.fget:
                    buff.write(
                        self._generate_function_stub(
                            key, element.fget, indentation=TAB, deco="@property"
                        )[0]
                    )
                if element.fset:
                    buff.write(
                        self._generate_function_stub(
                            key, element.fset, indentation=TAB, deco="@%s.setter" % key
                        )[0]
                    )

        # Special handling of classes that have injected methods
        if clazz == Current:
            # Multiple decorators can add the same object (trigger and trigger_on_finish)
            # as examples so we sort it out.
            resulting_dict = (
                dict()
            )  # type Dict[str, List[inspect.Signature, str, List[str]]]
            for deco_name, addl_current in self._addl_current.items():
                # TODO: For now, we assume all current functions are explicit (no references)
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
                    )[0]
                )
        # Special handling of the Deployer, DeployedFlow and TriggeredRun classes
        elif clazz == Deployer:
            for impl_name, func_info in self._addl_deployer.items():
                if isinstance(func_info, str):
                    buff.write(add_indent(TAB, self._injected_stubs[func_info]))
                else:
                    sign, doc = func_info
                    buff.write(
                        self._generate_function_stub(
                            impl_name, sign=[sign], indentation=TAB, doc=doc
                        )[0]
                    )
        elif clazz == DeployedFlow:
            for (impl_name, func_name), func_info in self._addl_deployed_flow.items():
                if isinstance(func_info, str):
                    buff.write(self._injected_stubs[func_info])
                else:
                    sign, doc = func_info
                    buff.write(
                        self._generate_function_stub(
                            "%s_%s" % (impl_name, func_name),
                            sign=[sign],
                            indentation=TAB,
                            doc="(for a DeployedFlow returned with `Deployer().%s()` -- "
                            "accessed as %s)\n\n" % (impl_name, func_name) + doc,
                        )[0]
                    )
        elif clazz == TriggeredRun:
            for (impl_name, func_name), func_info in self._addl_triggered_run.items():
                if isinstance(func_info, str):
                    buff.write(self._injected_stubs[func_info])
                else:
                    sign, doc = func_info
                    buff.write(
                        self._generate_function_stub(
                            "%s_%s" % (impl_name, func_name),
                            sign=[sign],
                            indentation=TAB,
                            doc="(for a TriggeredRun returned from a DeployedFlow returned "
                            "through `Deployer().%s()` -- accessed as %s)\n\n"
                            % (impl_name, func_name)
                            + doc,
                        )[0]
                    )

        if init_func is None and annotation_dict:
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
                )[0]
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
        result[0] = (result[0][0], docs["func_doc"])
        result[-1] = (result[-1][0], docs["func_doc"])
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
                        self._typing_imports.add(default_value.__module__)
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
        try:
            sign = sign or [inspect.signature(cast(Callable, func))]
        except ValueError:
            # In 3.7, NamedTuples have properties that then give an operator.itemgetter
            # which doesn't have a signature. We ignore for now. It doesn't have much
            # value
            return "", False
        doc = doc or func.__doc__
        is_injected_function = False
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
                buff.write(indentation + "@typing.overload\n")
            if deco:
                buff.write(indentation + deco + "\n")
            buff.write(indentation + "def " + name + "(")
            kw_only_param = False
            for i, (par_name, parameter) in enumerate(my_sign.parameters.items()):
                annotation = self._exploit_annotation(parameter.annotation)
                if annotation in self._injected_types:
                    # We are going to write "self" since this is meant to be injected
                    # in another class. We will also add this to the injected_stubs
                    is_injected_function = True
                    buff.write("self")
                    if i < len(my_sign.parameters) - 1:
                        buff.write(", ")
                    continue

                default = exploit_default(parameter.default)

                if kw_only_param and parameter.kind != inspect.Parameter.KEYWORD_ONLY:
                    raise RuntimeError(
                        "In function '%s': cannot have a positional parameter after a "
                        "keyword only parameter" % name
                    )
                if (
                    parameter.kind == inspect.Parameter.KEYWORD_ONLY
                    and not kw_only_param
                ):
                    kw_only_param = True
                    buff.write("*, ")
                if parameter.kind == inspect.Parameter.VAR_KEYWORD:
                    par_name = "**%s" % par_name
                elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
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
        return buff.getvalue(), is_injected_function

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
                        )[0]
                    )
                else:
                    func_stub, is_injected = self._generate_function_stub(name, attr)
                    if is_injected:
                        self._injected_stubs[
                            "%s.%s" % (self._current_module_name, name)
                        ] = func_stub
                    else:
                        self._stubs.append(func_stub)
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
                            )[0]
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
                        )[0]
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
        if self._write_generated_for:
            pathlib.Path(os.path.join(out_dir, "generated_for.txt")).write_text(
                "%s %s"
                % (self._mf_version, datetime.fromtimestamp(time.time()).isoformat())
            )
        post_process_modules = []
        while len(self._pending_modules) != 0 or len(post_process_modules) != 0:
            if len(self._pending_modules) == 0:
                module_name = post_process_modules.pop(0)
            else:
                module_name = self._pending_modules.pop(0)
            # Skip vendored stuff
            if module_name.startswith("metaflow._vendor"):
                continue
            # We delay current module and deployer module to the end since they
            # depend on info we gather elsewhere
            if (
                module_name
                in (
                    METAFLOW_CURRENT_MODULE_NAME,
                    METAFLOW_DEPLOYER_MODULE_NAME,
                )
                and len(self._pending_modules) != 0
            ):
                post_process_modules.append(module_name)
                continue
            if module_name in self._done_modules:
                continue
            self._done_modules.add(module_name)
            # If not, we process the module
            self._reset()
            self._get_module(module_name)
            self._generate_stubs()

            if hasattr(self._current_module, "__path__"):
                # This is a package (so a directory) and we are dealing with
                # a __init__.pyi type of case
                dir_path = os.path.join(
                    self._output_dir, *self._current_module.__name__.split(".")[1:]
                )
            else:
                # This is NOT a package so the original source file is not a __init__.py
                dir_path = os.path.join(
                    self._output_dir, *self._current_module.__name__.split(".")[1:-1]
                )
            out_file = os.path.join(
                dir_path, os.path.basename(self._current_module.__file__) + "i"
            )

            os.makedirs(os.path.dirname(out_file), exist_ok=True)

            width = 80
            title_line = "Auto-generated Metaflow stub file"
            title_white_space = (width - len(title_line)) / 2
            title_line = "#%s%s%s#\n" % (
                " " * math.floor(title_white_space),
                title_line,
                " " * math.ceil(title_white_space),
            )
            with open(out_file, mode="w", encoding="utf-8") as f:
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
                for stub in self._stubs:
                    f.write(stub + "\n")


if __name__ == "__main__":
    gen = StubGenerator("./stubs")
    gen.write_out()
