# Copyright (c) 2012-2015 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2013-2014 Google, Inc.
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Eevee (Alex Munroe) <amunroe@yelp.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2015 Dmitry Pribysh <dmand@yandex.ru>
# Copyright (c) 2015 David Shea <dshea@redhat.com>
# Copyright (c) 2015 Philip Lorenz <philip@bithub.de>
# Copyright (c) 2016 Jakub Wilk <jwilk@jwilk.net>
# Copyright (c) 2016 Mateusz Bysiek <mb@mbdev.pl>
# Copyright (c) 2017 Hugo <hugovk@users.noreply.github.com>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2019 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Ram Rachum <ram@rachum.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Dimitri Prybysh <dmand@yandex.ru>
# Copyright (c) 2021 David Liu <david@cs.toronto.edu>
# Copyright (c) 2021 pre-commit-ci[bot] <bot@noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Andrew Haigh <hello@nelf.in>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Astroid hooks for the Python standard library."""

import functools
import keyword
from textwrap import dedent

from metaflow._vendor import astroid
from metaflow._vendor.astroid import arguments, inference_tip, nodes, util
from metaflow._vendor.astroid.builder import AstroidBuilder, extract_node
from metaflow._vendor.astroid.exceptions import (
    AstroidTypeError,
    AstroidValueError,
    InferenceError,
    MroError,
    UseInferenceDefault,
)
from metaflow._vendor.astroid.manager import AstroidManager

TYPING_NAMEDTUPLE_BASENAMES = {"NamedTuple", "typing.NamedTuple"}
ENUM_BASE_NAMES = {
    "Enum",
    "IntEnum",
    "enum.Enum",
    "enum.IntEnum",
    "IntFlag",
    "enum.IntFlag",
}


def _infer_first(node, context):
    if node is util.Uninferable:
        raise UseInferenceDefault
    try:
        value = next(node.infer(context=context))
    except StopIteration as exc:
        raise InferenceError from exc
    if value is util.Uninferable:
        raise UseInferenceDefault()
    return value


def _find_func_form_arguments(node, context):
    def _extract_namedtuple_arg_or_keyword(  # pylint: disable=inconsistent-return-statements
        position, key_name=None
    ):
        if len(args) > position:
            return _infer_first(args[position], context)
        if key_name and key_name in found_keywords:
            return _infer_first(found_keywords[key_name], context)

    args = node.args
    keywords = node.keywords
    found_keywords = (
        {keyword.arg: keyword.value for keyword in keywords} if keywords else {}
    )

    name = _extract_namedtuple_arg_or_keyword(position=0, key_name="typename")
    names = _extract_namedtuple_arg_or_keyword(position=1, key_name="field_names")
    if name and names:
        return name.value, names

    raise UseInferenceDefault()


def infer_func_form(node, base_type, context=None, enum=False):
    """Specific inference function for namedtuple or Python 3 enum."""
    # node is a Call node, class name as first argument and generated class
    # attributes as second argument

    # namedtuple or enums list of attributes can be a list of strings or a
    # whitespace-separate string
    try:
        name, names = _find_func_form_arguments(node, context)
        try:
            attributes = names.value.replace(",", " ").split()
        except AttributeError as exc:
            if not enum:
                attributes = [
                    _infer_first(const, context).value for const in names.elts
                ]
            else:
                # Enums supports either iterator of (name, value) pairs
                # or mappings.
                if hasattr(names, "items") and isinstance(names.items, list):
                    attributes = [
                        _infer_first(const[0], context).value
                        for const in names.items
                        if isinstance(const[0], nodes.Const)
                    ]
                elif hasattr(names, "elts"):
                    # Enums can support either ["a", "b", "c"]
                    # or [("a", 1), ("b", 2), ...], but they can't
                    # be mixed.
                    if all(isinstance(const, nodes.Tuple) for const in names.elts):
                        attributes = [
                            _infer_first(const.elts[0], context).value
                            for const in names.elts
                            if isinstance(const, nodes.Tuple)
                        ]
                    else:
                        attributes = [
                            _infer_first(const, context).value for const in names.elts
                        ]
                else:
                    raise AttributeError from exc
                if not attributes:
                    raise AttributeError from exc
    except (AttributeError, InferenceError) as exc:
        raise UseInferenceDefault from exc

    if not enum:
        # namedtuple maps sys.intern(str()) over over field_names
        attributes = [str(attr) for attr in attributes]
        # XXX this should succeed *unless* __str__/__repr__ is incorrect or throws
        # in which case we should not have inferred these values and raised earlier
    attributes = [attr for attr in attributes if " " not in attr]

    # If we can't infer the name of the class, don't crash, up to this point
    # we know it is a namedtuple anyway.
    name = name or "Uninferable"
    # we want to return a Class node instance with proper attributes set
    class_node = nodes.ClassDef(name, "docstring")
    class_node.parent = node.parent
    # set base class=tuple
    class_node.bases.append(base_type)
    # XXX add __init__(*attributes) method
    for attr in attributes:
        fake_node = nodes.EmptyNode()
        fake_node.parent = class_node
        fake_node.attrname = attr
        class_node.instance_attrs[attr] = [fake_node]
    return class_node, name, attributes


def _has_namedtuple_base(node):
    """Predicate for class inference tip

    :type node: ClassDef
    :rtype: bool
    """
    return set(node.basenames) & TYPING_NAMEDTUPLE_BASENAMES


def _looks_like(node, name):
    func = node.func
    if isinstance(func, nodes.Attribute):
        return func.attrname == name
    if isinstance(func, nodes.Name):
        return func.name == name
    return False


_looks_like_namedtuple = functools.partial(_looks_like, name="namedtuple")
_looks_like_enum = functools.partial(_looks_like, name="Enum")
_looks_like_typing_namedtuple = functools.partial(_looks_like, name="NamedTuple")


def infer_named_tuple(node, context=None):
    """Specific inference function for namedtuple Call node"""
    tuple_base_name = nodes.Name(name="tuple", parent=node.root())
    class_node, name, attributes = infer_func_form(
        node, tuple_base_name, context=context
    )
    call_site = arguments.CallSite.from_call(node, context=context)
    node = extract_node("import collections; collections.namedtuple")
    try:

        func = next(node.infer())
    except StopIteration as e:
        raise InferenceError(node=node) from e
    try:
        rename = next(call_site.infer_argument(func, "rename", context)).bool_value()
    except (InferenceError, StopIteration):
        rename = False

    try:
        attributes = _check_namedtuple_attributes(name, attributes, rename)
    except AstroidTypeError as exc:
        raise UseInferenceDefault("TypeError: " + str(exc)) from exc
    except AstroidValueError as exc:
        raise UseInferenceDefault("ValueError: " + str(exc)) from exc

    replace_args = ", ".join(f"{arg}=None" for arg in attributes)
    field_def = (
        "    {name} = property(lambda self: self[{index:d}], "
        "doc='Alias for field number {index:d}')"
    )
    field_defs = "\n".join(
        field_def.format(name=name, index=index)
        for index, name in enumerate(attributes)
    )
    fake = AstroidBuilder(AstroidManager()).string_build(
        f"""
class {name}(tuple):
    __slots__ = ()
    _fields = {attributes!r}
    def _asdict(self):
        return self.__dict__
    @classmethod
    def _make(cls, iterable, new=tuple.__new__, len=len):
        return new(cls, iterable)
    def _replace(self, {replace_args}):
        return self
    def __getnewargs__(self):
        return tuple(self)
{field_defs}
    """
    )
    class_node.locals["_asdict"] = fake.body[0].locals["_asdict"]
    class_node.locals["_make"] = fake.body[0].locals["_make"]
    class_node.locals["_replace"] = fake.body[0].locals["_replace"]
    class_node.locals["_fields"] = fake.body[0].locals["_fields"]
    for attr in attributes:
        class_node.locals[attr] = fake.body[0].locals[attr]
    # we use UseInferenceDefault, we can't be a generator so return an iterator
    return iter([class_node])


def _get_renamed_namedtuple_attributes(field_names):
    names = list(field_names)
    seen = set()
    for i, name in enumerate(field_names):
        if (
            not all(c.isalnum() or c == "_" for c in name)
            or keyword.iskeyword(name)
            or not name
            or name[0].isdigit()
            or name.startswith("_")
            or name in seen
        ):
            names[i] = "_%d" % i
        seen.add(name)
    return tuple(names)


def _check_namedtuple_attributes(typename, attributes, rename=False):
    attributes = tuple(attributes)
    if rename:
        attributes = _get_renamed_namedtuple_attributes(attributes)

    # The following snippet is derived from the CPython Lib/collections/__init__.py sources
    # <snippet>
    for name in (typename,) + attributes:
        if not isinstance(name, str):
            raise AstroidTypeError("Type names and field names must be strings")
        if not name.isidentifier():
            raise AstroidValueError(
                "Type names and field names must be valid" + f"identifiers: {name!r}"
            )
        if keyword.iskeyword(name):
            raise AstroidValueError(
                f"Type names and field names cannot be a keyword: {name!r}"
            )

    seen = set()
    for name in attributes:
        if name.startswith("_") and not rename:
            raise AstroidValueError(
                f"Field names cannot start with an underscore: {name!r}"
            )
        if name in seen:
            raise AstroidValueError(f"Encountered duplicate field name: {name!r}")
        seen.add(name)
    # </snippet>

    return attributes


def infer_enum(node, context=None):
    """Specific inference function for enum Call node."""
    enum_meta = extract_node(
        """
    class EnumMeta(object):
        'docstring'
        def __call__(self, node):
            class EnumAttribute(object):
                name = ''
                value = 0
            return EnumAttribute()
        def __iter__(self):
            class EnumAttribute(object):
                name = ''
                value = 0
            return [EnumAttribute()]
        def __reversed__(self):
            class EnumAttribute(object):
                name = ''
                value = 0
            return (EnumAttribute, )
        def __next__(self):
            return next(iter(self))
        def __getitem__(self, attr):
            class Value(object):
                @property
                def name(self):
                    return ''
                @property
                def value(self):
                    return attr

            return Value()
        __members__ = ['']
    """
    )
    class_node = infer_func_form(node, enum_meta, context=context, enum=True)[0]
    return iter([class_node.instantiate_class()])


INT_FLAG_ADDITION_METHODS = """
    def __or__(self, other):
        return {name}(self.value | other.value)
    def __and__(self, other):
        return {name}(self.value & other.value)
    def __xor__(self, other):
        return {name}(self.value ^ other.value)
    def __add__(self, other):
        return {name}(self.value + other.value)
    def __div__(self, other):
        return {name}(self.value / other.value)
    def __invert__(self):
        return {name}(~self.value)
    def __mul__(self, other):
        return {name}(self.value * other.value)
"""


def infer_enum_class(node):
    """Specific inference for enums."""
    for basename in (b for cls in node.mro() for b in cls.basenames):
        if basename not in ENUM_BASE_NAMES:
            continue
        if node.root().name == "enum":
            # Skip if the class is directly from enum module.
            break
        dunder_members = {}
        target_names = set()
        for local, values in node.locals.items():
            if any(not isinstance(value, nodes.AssignName) for value in values):
                continue

            stmt = values[0].statement(future=True)
            if isinstance(stmt, nodes.Assign):
                if isinstance(stmt.targets[0], nodes.Tuple):
                    targets = stmt.targets[0].itered()
                else:
                    targets = stmt.targets
            elif isinstance(stmt, nodes.AnnAssign):
                targets = [stmt.target]
            else:
                continue

            inferred_return_value = None
            if isinstance(stmt, nodes.Assign):
                if isinstance(stmt.value, nodes.Const):
                    if isinstance(stmt.value.value, str):
                        inferred_return_value = repr(stmt.value.value)
                    else:
                        inferred_return_value = stmt.value.value
                else:
                    inferred_return_value = stmt.value.as_string()

            new_targets = []
            for target in targets:
                if isinstance(target, nodes.Starred):
                    continue
                target_names.add(target.name)
                # Replace all the assignments with our mocked class.
                classdef = dedent(
                    """
                class {name}({types}):
                    @property
                    def value(self):
                        return {return_value}
                    @property
                    def name(self):
                        return "{name}"
                """.format(
                        name=target.name,
                        types=", ".join(node.basenames),
                        return_value=inferred_return_value,
                    )
                )
                if "IntFlag" in basename:
                    # Alright, we need to add some additional methods.
                    # Unfortunately we still can't infer the resulting objects as
                    # Enum members, but once we'll be able to do that, the following
                    # should result in some nice symbolic execution
                    classdef += INT_FLAG_ADDITION_METHODS.format(name=target.name)

                fake = AstroidBuilder(
                    AstroidManager(), apply_transforms=False
                ).string_build(classdef)[target.name]
                fake.parent = target.parent
                for method in node.mymethods():
                    fake.locals[method.name] = [method]
                new_targets.append(fake.instantiate_class())
                dunder_members[local] = fake
            node.locals[local] = new_targets
        members = nodes.Dict(parent=node)
        members.postinit(
            [
                (nodes.Const(k, parent=members), nodes.Name(v.name, parent=members))
                for k, v in dunder_members.items()
            ]
        )
        node.locals["__members__"] = [members]
        # The enum.Enum class itself defines two @DynamicClassAttribute data-descriptors
        # "name" and "value" (which we override in the mocked class for each enum member
        # above). When dealing with inference of an arbitrary instance of the enum
        # class, e.g. in a method defined in the class body like:
        #     class SomeEnum(enum.Enum):
        #         def method(self):
        #             self.name  # <- here
        # In the absence of an enum member called "name" or "value", these attributes
        # should resolve to the descriptor on that particular instance, i.e. enum member.
        # For "value", we have no idea what that should be, but for "name", we at least
        # know that it should be a string, so infer that as a guess.
        if "name" not in target_names:
            code = dedent(
                """
            @property
            def name(self):
                return ''
            """
            )
            name_dynamicclassattr = AstroidBuilder(AstroidManager()).string_build(code)[
                "name"
            ]
            node.locals["name"] = [name_dynamicclassattr]
        break
    return node


def infer_typing_namedtuple_class(class_node, context=None):
    """Infer a subclass of typing.NamedTuple"""
    # Check if it has the corresponding bases
    annassigns_fields = [
        annassign.target.name
        for annassign in class_node.body
        if isinstance(annassign, nodes.AnnAssign)
    ]
    code = dedent(
        """
    from collections import namedtuple
    namedtuple({typename!r}, {fields!r})
    """
    ).format(typename=class_node.name, fields=",".join(annassigns_fields))
    node = extract_node(code)
    try:
        generated_class_node = next(infer_named_tuple(node, context))
    except StopIteration as e:
        raise InferenceError(node=node, context=context) from e
    for method in class_node.mymethods():
        generated_class_node.locals[method.name] = [method]

    for body_node in class_node.body:
        if isinstance(body_node, nodes.Assign):
            for target in body_node.targets:
                attr = target.name
                generated_class_node.locals[attr] = class_node.locals[attr]
        elif isinstance(body_node, nodes.ClassDef):
            generated_class_node.locals[body_node.name] = [body_node]

    return iter((generated_class_node,))


def infer_typing_namedtuple_function(node, context=None):
    """
    Starting with python3.9, NamedTuple is a function of the typing module.
    The class NamedTuple is build dynamically through a call to `type` during
    initialization of the `_NamedTuple` variable.
    """
    klass = extract_node(
        """
        from typing import _NamedTuple
        _NamedTuple
        """
    )
    return klass.infer(context)


def infer_typing_namedtuple(node, context=None):
    """Infer a typing.NamedTuple(...) call."""
    # This is essentially a namedtuple with different arguments
    # so we extract the args and infer a named tuple.
    try:
        func = next(node.func.infer())
    except (InferenceError, StopIteration) as exc:
        raise UseInferenceDefault from exc

    if func.qname() != "typing.NamedTuple":
        raise UseInferenceDefault

    if len(node.args) != 2:
        raise UseInferenceDefault

    if not isinstance(node.args[1], (nodes.List, nodes.Tuple)):
        raise UseInferenceDefault

    names = []
    for elt in node.args[1].elts:
        if not isinstance(elt, (nodes.List, nodes.Tuple)):
            raise UseInferenceDefault
        if len(elt.elts) != 2:
            raise UseInferenceDefault
        names.append(elt.elts[0].as_string())

    typename = node.args[0].as_string()
    if names:
        field_names = f"({','.join(names)},)"
    else:
        field_names = "''"
    node = extract_node(f"namedtuple({typename}, {field_names})")
    return infer_named_tuple(node, context)


def _is_enum_subclass(cls: astroid.ClassDef) -> bool:
    """Return whether cls is a subclass of an Enum."""
    try:
        return any(
            klass.name in ENUM_BASE_NAMES
            and getattr(klass.root(), "name", None) == "enum"
            for klass in cls.mro()
        )
    except MroError:
        return False


AstroidManager().register_transform(
    nodes.Call, inference_tip(infer_named_tuple), _looks_like_namedtuple
)
AstroidManager().register_transform(
    nodes.Call, inference_tip(infer_enum), _looks_like_enum
)
AstroidManager().register_transform(
    nodes.ClassDef, infer_enum_class, predicate=_is_enum_subclass
)
AstroidManager().register_transform(
    nodes.ClassDef, inference_tip(infer_typing_namedtuple_class), _has_namedtuple_base
)
AstroidManager().register_transform(
    nodes.FunctionDef,
    inference_tip(infer_typing_namedtuple_function),
    lambda node: node.name == "NamedTuple"
    and getattr(node.root(), "name", None) == "typing",
)
AstroidManager().register_transform(
    nodes.Call, inference_tip(infer_typing_namedtuple), _looks_like_typing_namedtuple
)
