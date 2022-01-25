# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE
"""
Astroid hook for the dataclasses library

Support built-in dataclasses, pydantic.dataclasses, and marshmallow_dataclass-annotated
dataclasses. References:
- https://docs.python.org/3/library/dataclasses.html
- https://pydantic-docs.helpmanual.io/usage/dataclasses/
- https://lovasoa.github.io/marshmallow_dataclass/

"""
import sys
from typing import FrozenSet, Generator, List, Optional, Tuple, Union

from metaflow._vendor.astroid import context, inference_tip
from metaflow._vendor.astroid.builder import parse
from metaflow._vendor.astroid.const import PY37_PLUS, PY39_PLUS
from metaflow._vendor.astroid.exceptions import (
    AstroidSyntaxError,
    InferenceError,
    MroError,
    UseInferenceDefault,
)
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes.node_classes import (
    AnnAssign,
    Assign,
    AssignName,
    Attribute,
    Call,
    Name,
    NodeNG,
    Subscript,
    Unknown,
)
from metaflow._vendor.astroid.nodes.scoped_nodes import ClassDef, FunctionDef
from metaflow._vendor.astroid.util import Uninferable

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from metaflow._vendor.typing_extensions import Literal

_FieldDefaultReturn = Union[
    None, Tuple[Literal["default"], NodeNG], Tuple[Literal["default_factory"], Call]
]

DATACLASSES_DECORATORS = frozenset(("dataclass",))
FIELD_NAME = "field"
DATACLASS_MODULES = frozenset(
    ("dataclasses", "marshmallow_dataclass", "pydantic.dataclasses")
)
DEFAULT_FACTORY = "_HAS_DEFAULT_FACTORY"  # based on typing.py


def is_decorated_with_dataclass(node, decorator_names=DATACLASSES_DECORATORS):
    """Return True if a decorated node has a `dataclass` decorator applied."""
    if not isinstance(node, ClassDef) or not node.decorators:
        return False

    return any(
        _looks_like_dataclass_decorator(decorator_attribute, decorator_names)
        for decorator_attribute in node.decorators.nodes
    )


def dataclass_transform(node: ClassDef) -> None:
    """Rewrite a dataclass to be easily understood by pylint"""

    for assign_node in _get_dataclass_attributes(node):
        name = assign_node.target.name

        rhs_node = Unknown(
            lineno=assign_node.lineno,
            col_offset=assign_node.col_offset,
            parent=assign_node,
        )
        rhs_node = AstroidManager().visit_transforms(rhs_node)
        node.instance_attrs[name] = [rhs_node]

    if not _check_generate_dataclass_init(node):
        return

    try:
        reversed_mro = list(reversed(node.mro()))
    except MroError:
        reversed_mro = [node]

    field_assigns = {}
    field_order = []
    for klass in (k for k in reversed_mro if is_decorated_with_dataclass(k)):
        for assign_node in _get_dataclass_attributes(klass, init=True):
            name = assign_node.target.name
            if name not in field_assigns:
                field_order.append(name)
            field_assigns[name] = assign_node

    init_str = _generate_dataclass_init([field_assigns[name] for name in field_order])
    try:
        init_node = parse(init_str)["__init__"]
    except AstroidSyntaxError:
        pass
    else:
        init_node.parent = node
        init_node.lineno, init_node.col_offset = None, None
        node.locals["__init__"] = [init_node]

        root = node.root()
        if DEFAULT_FACTORY not in root.locals:
            new_assign = parse(f"{DEFAULT_FACTORY} = object()").body[0]
            new_assign.parent = root
            root.locals[DEFAULT_FACTORY] = [new_assign.targets[0]]


def _get_dataclass_attributes(node: ClassDef, init: bool = False) -> Generator:
    """Yield the AnnAssign nodes of dataclass attributes for the node.

    If init is True, also include InitVars, but exclude attributes from calls to
    field where init=False.
    """
    for assign_node in node.body:
        if not isinstance(assign_node, AnnAssign) or not isinstance(
            assign_node.target, AssignName
        ):
            continue

        if _is_class_var(assign_node.annotation):  # type: ignore[arg-type] # annotation is never None
            continue

        if init:
            value = assign_node.value
            if (
                isinstance(value, Call)
                and _looks_like_dataclass_field_call(value, check_scope=False)
                and any(
                    keyword.arg == "init"
                    and not keyword.value.bool_value()  # type: ignore[union-attr] # value is never None
                    for keyword in value.keywords
                )
            ):
                continue
        elif _is_init_var(assign_node.annotation):  # type: ignore[arg-type] # annotation is never None
            continue

        yield assign_node


def _check_generate_dataclass_init(node: ClassDef) -> bool:
    """Return True if we should generate an __init__ method for node.

    This is True when:
        - node doesn't define its own __init__ method
        - the dataclass decorator was called *without* the keyword argument init=False
    """
    if "__init__" in node.locals:
        return False

    found = None

    for decorator_attribute in node.decorators.nodes:
        if not isinstance(decorator_attribute, Call):
            continue

        if _looks_like_dataclass_decorator(decorator_attribute):
            found = decorator_attribute

    if found is None:
        return True

    # Check for keyword arguments of the form init=False
    return all(
        keyword.arg != "init"
        and keyword.value.bool_value()  # type: ignore[union-attr] # value is never None
        for keyword in found.keywords
    )


def _generate_dataclass_init(assigns: List[AnnAssign]) -> str:
    """Return an init method for a dataclass given the targets."""
    target_names = []
    params = []
    assignments = []

    for assign in assigns:
        name, annotation, value = assign.target.name, assign.annotation, assign.value
        target_names.append(name)

        if _is_init_var(annotation):  # type: ignore[arg-type] # annotation is never None
            init_var = True
            if isinstance(annotation, Subscript):
                annotation = annotation.slice
            else:
                # Cannot determine type annotation for parameter from InitVar
                annotation = None
            assignment_str = ""
        else:
            init_var = False
            assignment_str = f"self.{name} = {name}"

        if annotation:
            param_str = f"{name}: {annotation.as_string()}"
        else:
            param_str = name

        if value:
            if isinstance(value, Call) and _looks_like_dataclass_field_call(
                value, check_scope=False
            ):
                result = _get_field_default(value)
                if result:
                    default_type, default_node = result
                    if default_type == "default":
                        param_str += f" = {default_node.as_string()}"
                    elif default_type == "default_factory":
                        param_str += f" = {DEFAULT_FACTORY}"
                        assignment_str = (
                            f"self.{name} = {default_node.as_string()} "
                            f"if {name} is {DEFAULT_FACTORY} else {name}"
                        )
            else:
                param_str += f" = {value.as_string()}"

        params.append(param_str)
        if not init_var:
            assignments.append(assignment_str)

    params_string = ", ".join(["self"] + params)
    assignments_string = "\n    ".join(assignments) if assignments else "pass"
    return f"def __init__({params_string}) -> None:\n    {assignments_string}"


def infer_dataclass_attribute(
    node: Unknown, ctx: Optional[context.InferenceContext] = None
) -> Generator:
    """Inference tip for an Unknown node that was dynamically generated to
    represent a dataclass attribute.

    In the case that a default value is provided, that is inferred first.
    Then, an Instance of the annotated class is yielded.
    """
    assign = node.parent
    if not isinstance(assign, AnnAssign):
        yield Uninferable
        return

    annotation, value = assign.annotation, assign.value
    if value is not None:
        yield from value.infer(context=ctx)
    if annotation is not None:
        yield from _infer_instance_from_annotation(annotation, ctx=ctx)
    else:
        yield Uninferable


def infer_dataclass_field_call(
    node: Call, ctx: Optional[context.InferenceContext] = None
) -> Generator:
    """Inference tip for dataclass field calls."""
    if not isinstance(node.parent, (AnnAssign, Assign)):
        raise UseInferenceDefault
    result = _get_field_default(node)
    if not result:
        yield Uninferable
    else:
        default_type, default = result
        if default_type == "default":
            yield from default.infer(context=ctx)
        else:
            new_call = parse(default.as_string()).body[0].value
            new_call.parent = node.parent
            yield from new_call.infer(context=ctx)


def _looks_like_dataclass_decorator(
    node: NodeNG, decorator_names: FrozenSet[str] = DATACLASSES_DECORATORS
) -> bool:
    """Return True if node looks like a dataclass decorator.

    Uses inference to lookup the value of the node, and if that fails,
    matches against specific names.
    """
    if isinstance(node, Call):  # decorator with arguments
        node = node.func
    try:
        inferred = next(node.infer())
    except (InferenceError, StopIteration):
        inferred = Uninferable

    if inferred is Uninferable:
        if isinstance(node, Name):
            return node.name in decorator_names
        if isinstance(node, Attribute):
            return node.attrname in decorator_names

        return False

    return (
        isinstance(inferred, FunctionDef)
        and inferred.name in decorator_names
        and inferred.root().name in DATACLASS_MODULES
    )


def _looks_like_dataclass_attribute(node: Unknown) -> bool:
    """Return True if node was dynamically generated as the child of an AnnAssign
    statement.
    """
    parent = node.parent
    if not parent:
        return False

    scope = parent.scope()
    return (
        isinstance(parent, AnnAssign)
        and isinstance(scope, ClassDef)
        and is_decorated_with_dataclass(scope)
    )


def _looks_like_dataclass_field_call(node: Call, check_scope: bool = True) -> bool:
    """Return True if node is calling dataclasses field or Field
    from an AnnAssign statement directly in the body of a ClassDef.

    If check_scope is False, skips checking the statement and body.
    """
    if check_scope:
        stmt = node.statement(future=True)
        scope = stmt.scope()
        if not (
            isinstance(stmt, AnnAssign)
            and stmt.value is not None
            and isinstance(scope, ClassDef)
            and is_decorated_with_dataclass(scope)
        ):
            return False

    try:
        inferred = next(node.func.infer())
    except (InferenceError, StopIteration):
        return False

    if not isinstance(inferred, FunctionDef):
        return False

    return inferred.name == FIELD_NAME and inferred.root().name in DATACLASS_MODULES


def _get_field_default(field_call: Call) -> _FieldDefaultReturn:
    """Return a the default value of a field call, and the corresponding keyword argument name.

    field(default=...) results in the ... node
    field(default_factory=...) results in a Call node with func ... and no arguments

    If neither or both arguments are present, return ("", None) instead,
    indicating that there is not a valid default value.
    """
    default, default_factory = None, None
    for keyword in field_call.keywords:
        if keyword.arg == "default":
            default = keyword.value
        elif keyword.arg == "default_factory":
            default_factory = keyword.value

    if default is not None and default_factory is None:
        return "default", default

    if default is None and default_factory is not None:
        new_call = Call(
            lineno=field_call.lineno,
            col_offset=field_call.col_offset,
            parent=field_call.parent,
        )
        new_call.postinit(func=default_factory)
        return "default_factory", new_call

    return None


def _is_class_var(node: NodeNG) -> bool:
    """Return True if node is a ClassVar, with or without subscripting."""
    if PY39_PLUS:
        try:
            inferred = next(node.infer())
        except (InferenceError, StopIteration):
            return False

        return getattr(inferred, "name", "") == "ClassVar"

    # Before Python 3.9, inference returns typing._SpecialForm instead of ClassVar.
    # Our backup is to inspect the node's structure.
    return isinstance(node, Subscript) and (
        isinstance(node.value, Name)
        and node.value.name == "ClassVar"
        or isinstance(node.value, Attribute)
        and node.value.attrname == "ClassVar"
    )


def _is_init_var(node: NodeNG) -> bool:
    """Return True if node is an InitVar, with or without subscripting."""
    try:
        inferred = next(node.infer())
    except (InferenceError, StopIteration):
        return False

    return getattr(inferred, "name", "") == "InitVar"


# Allowed typing classes for which we support inferring instances
_INFERABLE_TYPING_TYPES = frozenset(
    (
        "Dict",
        "FrozenSet",
        "List",
        "Set",
        "Tuple",
    )
)


def _infer_instance_from_annotation(
    node: NodeNG, ctx: Optional[context.InferenceContext] = None
) -> Generator:
    """Infer an instance corresponding to the type annotation represented by node.

    Currently has limited support for the typing module.
    """
    klass = None
    try:
        klass = next(node.infer(context=ctx))
    except (InferenceError, StopIteration):
        yield Uninferable
    if not isinstance(klass, ClassDef):
        yield Uninferable
    elif klass.root().name in {
        "typing",
        "_collections_abc",
        "",
    }:  # "" because of synthetic nodes in brain_typing.py
        if klass.name in _INFERABLE_TYPING_TYPES:
            yield klass.instantiate_class()
        else:
            yield Uninferable
    else:
        yield klass.instantiate_class()


if PY37_PLUS:
    AstroidManager().register_transform(
        ClassDef, dataclass_transform, is_decorated_with_dataclass
    )

    AstroidManager().register_transform(
        Call,
        inference_tip(infer_dataclass_field_call, raise_on_overwrite=True),
        _looks_like_dataclass_field_call,
    )

    AstroidManager().register_transform(
        Unknown,
        inference_tip(infer_dataclass_attribute, raise_on_overwrite=True),
        _looks_like_dataclass_attribute,
    )
