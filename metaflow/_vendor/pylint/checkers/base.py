# Copyright (c) 2006-2016 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2010 Daniel Harding <dharding@gmail.com>
# Copyright (c) 2012-2014 Google, Inc.
# Copyright (c) 2013-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Nick Bastin <nick.bastin@gmail.com>
# Copyright (c) 2015 Michael Kefeder <oss@multiwave.ch>
# Copyright (c) 2015 Dmitry Pribysh <dmand@yandex.ru>
# Copyright (c) 2015 Stephane Wirtel <stephane@wirtel.be>
# Copyright (c) 2015 Cosmin Poieana <cmin@ropython.org>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2015 Radu Ciorba <radu@devrandom.ro>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016, 2019 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2016, 2018 Jakub Wilk <jwilk@jwilk.net>
# Copyright (c) 2016-2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2016 Glenn Matthews <glenn@e-dad.net>
# Copyright (c) 2016 Elias Dorneles <eliasdorneles@gmail.com>
# Copyright (c) 2016 Yannack <yannack@users.noreply.github.com>
# Copyright (c) 2016 Alex Jurkiewicz <alex@jurkiewi.cz>
# Copyright (c) 2017, 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2017, 2019-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017 danields <danields761@gmail.com>
# Copyright (c) 2017 Jacques Kvam <jwkvam@gmail.com>
# Copyright (c) 2017 ttenhoeve-aa <ttenhoeve@appannie.com>
# Copyright (c) 2018-2019, 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018-2019, 2021 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2018 Sergei Lebedev <185856+superbobry@users.noreply.github.com>
# Copyright (c) 2018 Lucas Cimon <lucas.cimon@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Natalie Serebryakova <natalie.serebryakova@Natalies-MacBook-Pro.local>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 SergeyKosarchuk <sergeykosarchuk@gmail.com>
# Copyright (c) 2018 Steven M. Vascellaro <svascellaro@gmail.com>
# Copyright (c) 2018 Mike Frysinger <vapier@gmail.com>
# Copyright (c) 2018 Chris Lamb <chris@chris-lamb.co.uk>
# Copyright (c) 2018 glmdgrielson <32415403+glmdgrielson@users.noreply.github.com>
# Copyright (c) 2019 Daniel Draper <Germandrummer92@users.noreply.github.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2019 Niko Wenselowski <niko@nerdno.de>
# Copyright (c) 2019 Nikita Sobolev <mail@sobolevn.me>
# Copyright (c) 2019 Oisín Moran <OisinMoran@users.noreply.github.com>
# Copyright (c) 2019 Fantix King <fantix@uchicago.edu>
# Copyright (c) 2020 Peter Kolbus <peter.kolbus@gmail.com>
# Copyright (c) 2020 ethan-leba <ethanleba5@gmail.com>
# Copyright (c) 2020 へーさん <hira9603859504@gmail.com>
# Copyright (c) 2020 Damien Baty <damien.baty@polyconseil.fr>
# Copyright (c) 2020 Ram Rachum <ram@rachum.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2020 bernie gray <bfgray3@users.noreply.github.com>
# Copyright (c) 2020 Gabriel R Sezefredo <g@briel.dev>
# Copyright (c) 2020 Benny <benny.mueller91@gmail.com>
# Copyright (c) 2020 Anubhav <35621759+anubh-v@users.noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Tushar Sadhwani <tushar.sadhwani000@gmail.com>
# Copyright (c) 2021 Tim Martin <tim@asymptotic.co.uk>
# Copyright (c) 2021 Jaehoon Hwang <jaehoonhwang@users.noreply.github.com>
# Copyright (c) 2021 jaydesl <35102795+jaydesl@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 bot <bot@noreply.github.com>
# Copyright (c) 2021 Yilei "Dolee" Yang <yileiyang@google.com>
# Copyright (c) 2021 Lorena B <46202743+lorena-b@users.noreply.github.com>
# Copyright (c) 2021 David Liu <david@cs.toronto.edu>
# Copyright (c) 2021 Andreas Finkler <andi.finkler@gmail.com>
# Copyright (c) 2021 Or Bahari <orbahari@mail.tau.ac.il>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""basic checker for Python code"""
import collections
import itertools
import re
import sys
from typing import Any, Dict, Iterator, Optional, Pattern, cast

from metaflow._vendor import astroid
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint import checkers, interfaces
from metaflow._vendor.pylint import utils as lint_utils
from metaflow._vendor.pylint.checkers import utils
from metaflow._vendor.pylint.checkers.utils import (
    infer_all,
    is_overload_stub,
    is_property_deleter,
    is_property_setter,
)
from metaflow._vendor.pylint.reporters.ureports import nodes as reporter_nodes
from metaflow._vendor.pylint.utils import LinterStats
from metaflow._vendor.pylint.utils.utils import get_global_option

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from metaflow._vendor.typing_extensions import Literal


class NamingStyle:
    """It may seem counterintuitive that single naming style has multiple "accepted"
    forms of regular expressions, but we need to special-case stuff like dunder names
    in method names."""

    ANY: Pattern[str] = re.compile(".*")
    CLASS_NAME_RGX: Pattern[str] = ANY
    MOD_NAME_RGX: Pattern[str] = ANY
    CONST_NAME_RGX: Pattern[str] = ANY
    COMP_VAR_RGX: Pattern[str] = ANY
    DEFAULT_NAME_RGX: Pattern[str] = ANY
    CLASS_ATTRIBUTE_RGX: Pattern[str] = ANY

    @classmethod
    def get_regex(cls, name_type):
        return {
            "module": cls.MOD_NAME_RGX,
            "const": cls.CONST_NAME_RGX,
            "class": cls.CLASS_NAME_RGX,
            "function": cls.DEFAULT_NAME_RGX,
            "method": cls.DEFAULT_NAME_RGX,
            "attr": cls.DEFAULT_NAME_RGX,
            "argument": cls.DEFAULT_NAME_RGX,
            "variable": cls.DEFAULT_NAME_RGX,
            "class_attribute": cls.CLASS_ATTRIBUTE_RGX,
            "class_const": cls.CONST_NAME_RGX,
            "inlinevar": cls.COMP_VAR_RGX,
        }[name_type]


class SnakeCaseStyle(NamingStyle):
    """Regex rules for snake_case naming style."""

    CLASS_NAME_RGX = re.compile(r"[^\W\dA-Z][^\WA-Z]+$")
    MOD_NAME_RGX = re.compile(r"[^\W\dA-Z][^\WA-Z]*$")
    CONST_NAME_RGX = re.compile(r"([^\W\dA-Z][^\WA-Z]*|__.*__)$")
    COMP_VAR_RGX = re.compile(r"[^\W\dA-Z][^\WA-Z]*$")
    DEFAULT_NAME_RGX = re.compile(
        r"([^\W\dA-Z][^\WA-Z]{2,}|_[^\WA-Z]*|__[^\WA-Z\d_][^\WA-Z]+__)$"
    )
    CLASS_ATTRIBUTE_RGX = re.compile(r"([^\W\dA-Z][^\WA-Z]{2,}|__.*__)$")


class CamelCaseStyle(NamingStyle):
    """Regex rules for camelCase naming style."""

    CLASS_NAME_RGX = re.compile(r"[^\W\dA-Z][^\W_]+$")
    MOD_NAME_RGX = re.compile(r"[^\W\dA-Z][^\W_]*$")
    CONST_NAME_RGX = re.compile(r"([^\W\dA-Z][^\W_]*|__.*__)$")
    COMP_VAR_RGX = re.compile(r"[^\W\dA-Z][^\W_]*$")
    DEFAULT_NAME_RGX = re.compile(r"([^\W\dA-Z][^\W_]{2,}|__[^\W\dA-Z_]\w+__)$")
    CLASS_ATTRIBUTE_RGX = re.compile(r"([^\W\dA-Z][^\W_]{2,}|__.*__)$")


class PascalCaseStyle(NamingStyle):
    """Regex rules for PascalCase naming style."""

    CLASS_NAME_RGX = re.compile(r"[^\W\da-z][^\W_]+$")
    MOD_NAME_RGX = re.compile(r"[^\W\da-z][^\W_]+$")
    CONST_NAME_RGX = re.compile(r"([^\W\da-z][^\W_]*|__.*__)$")
    COMP_VAR_RGX = re.compile(r"[^\W\da-z][^\W_]+$")
    DEFAULT_NAME_RGX = re.compile(r"([^\W\da-z][^\W_]{2,}|__[^\W\dA-Z_]\w+__)$")
    CLASS_ATTRIBUTE_RGX = re.compile(r"[^\W\da-z][^\W_]{2,}$")


class UpperCaseStyle(NamingStyle):
    """Regex rules for UPPER_CASE naming style."""

    CLASS_NAME_RGX = re.compile(r"[^\W\da-z][^\Wa-z]+$")
    MOD_NAME_RGX = re.compile(r"[^\W\da-z][^\Wa-z]+$")
    CONST_NAME_RGX = re.compile(r"([^\W\da-z][^\Wa-z]*|__.*__)$")
    COMP_VAR_RGX = re.compile(r"[^\W\da-z][^\Wa-z]+$")
    DEFAULT_NAME_RGX = re.compile(r"([^\W\da-z][^\Wa-z]{2,}|__[^\W\dA-Z_]\w+__)$")
    CLASS_ATTRIBUTE_RGX = re.compile(r"[^\W\da-z][^\Wa-z]{2,}$")


class AnyStyle(NamingStyle):
    pass


NAMING_STYLES = {
    "snake_case": SnakeCaseStyle,
    "camelCase": CamelCaseStyle,
    "PascalCase": PascalCaseStyle,
    "UPPER_CASE": UpperCaseStyle,
    "any": AnyStyle,
}

# do not require a doc string on private/system methods
NO_REQUIRED_DOC_RGX = re.compile("^_")
REVERSED_PROTOCOL_METHOD = "__reversed__"
SEQUENCE_PROTOCOL_METHODS = ("__getitem__", "__len__")
REVERSED_METHODS = (SEQUENCE_PROTOCOL_METHODS, (REVERSED_PROTOCOL_METHOD,))
TYPECHECK_COMPARISON_OPERATORS = frozenset(("is", "is not", "==", "!="))
LITERAL_NODE_TYPES = (nodes.Const, nodes.Dict, nodes.List, nodes.Set)
UNITTEST_CASE = "unittest.case"
TYPE_QNAME = "builtins.type"
ABC_METACLASSES = {"_py_abc.ABCMeta", "abc.ABCMeta"}  # Python 3.7+,

# Name categories that are always consistent with all naming conventions.
EXEMPT_NAME_CATEGORIES = {"exempt", "ignore"}

# A mapping from qname -> symbol, to be used when generating messages
# about dangerous default values as arguments
DEFAULT_ARGUMENT_SYMBOLS = dict(
    zip(
        [".".join(["builtins", x]) for x in ("set", "dict", "list")],
        ["set()", "{}", "[]"],
    ),
    **{
        x: f"{x}()"
        for x in (
            "collections.deque",
            "collections.ChainMap",
            "collections.Counter",
            "collections.OrderedDict",
            "collections.defaultdict",
            "collections.UserDict",
            "collections.UserList",
        )
    },
)

COMPARISON_OPERATORS = frozenset(("==", "!=", "<", ">", "<=", ">="))
# List of methods which can be redefined
REDEFINABLE_METHODS = frozenset(("__module__",))
TYPING_FORWARD_REF_QNAME = "typing.ForwardRef"


def _redefines_import(node):
    """Detect that the given node (AssignName) is inside an
    exception handler and redefines an import from the tryexcept body.
    Returns True if the node redefines an import, False otherwise.
    """
    current = node
    while current and not isinstance(current.parent, nodes.ExceptHandler):
        current = current.parent
    if not current or not utils.error_of_type(current.parent, ImportError):
        return False
    try_block = current.parent.parent
    for import_node in try_block.nodes_of_class((nodes.ImportFrom, nodes.Import)):
        for name, alias in import_node.names:
            if alias:
                if alias == node.name:
                    return True
            elif name == node.name:
                return True
    return False


LOOPLIKE_NODES = (
    nodes.For,
    nodes.ListComp,
    nodes.SetComp,
    nodes.DictComp,
    nodes.GeneratorExp,
)


def in_loop(node: nodes.NodeNG) -> bool:
    """Return whether the node is inside a kind of for loop"""
    return any(isinstance(parent, LOOPLIKE_NODES) for parent in node.node_ancestors())


def in_nested_list(nested_list, obj):
    """return true if the object is an element of <nested_list> or of a nested
    list
    """
    for elmt in nested_list:
        if isinstance(elmt, (list, tuple)):
            if in_nested_list(elmt, obj):
                return True
        elif elmt == obj:
            return True
    return False


def _get_break_loop_node(break_node):
    """
    Returns the loop node that holds the break node in arguments.

    Args:
        break_node (astroid.Break): the break node of interest.

    Returns:
        astroid.For or astroid.While: the loop node holding the break node.
    """
    loop_nodes = (nodes.For, nodes.While)
    parent = break_node.parent
    while not isinstance(parent, loop_nodes) or break_node in getattr(
        parent, "orelse", []
    ):
        break_node = parent
        parent = parent.parent
        if parent is None:
            break
    return parent


def _loop_exits_early(loop):
    """
    Returns true if a loop may ends up in a break statement.

    Args:
        loop (astroid.For, astroid.While): the loop node inspected.

    Returns:
        bool: True if the loop may ends up in a break statement, False otherwise.
    """
    loop_nodes = (nodes.For, nodes.While)
    definition_nodes = (nodes.FunctionDef, nodes.ClassDef)
    inner_loop_nodes = [
        _node
        for _node in loop.nodes_of_class(loop_nodes, skip_klass=definition_nodes)
        if _node != loop
    ]
    return any(
        _node
        for _node in loop.nodes_of_class(nodes.Break, skip_klass=definition_nodes)
        if _get_break_loop_node(_node) not in inner_loop_nodes
    )


def _is_multi_naming_match(match, node_type, confidence):
    return (
        match is not None
        and match.lastgroup is not None
        and match.lastgroup not in EXEMPT_NAME_CATEGORIES
        and (node_type != "method" or confidence != interfaces.INFERENCE_FAILURE)
    )


BUILTIN_PROPERTY = "builtins.property"


def _get_properties(config):
    """Returns a tuple of property classes and names.

    Property classes are fully qualified, such as 'abc.abstractproperty' and
    property names are the actual names, such as 'abstract_property'.
    """
    property_classes = {BUILTIN_PROPERTY}
    property_names = set()  # Not returning 'property', it has its own check.
    if config is not None:
        property_classes.update(config.property_classes)
        property_names.update(
            prop.rsplit(".", 1)[-1] for prop in config.property_classes
        )
    return property_classes, property_names


def _determine_function_name_type(node: nodes.FunctionDef, config=None):
    """Determine the name type whose regex the a function's name should match.

    :param node: A function node.
    :param config: Configuration from which to pull additional property classes.
    :type config: :class:`optparse.Values`

    :returns: One of ('function', 'method', 'attr')
    :rtype: str
    """
    property_classes, property_names = _get_properties(config)
    if not node.is_method():
        return "function"

    if is_property_setter(node) or is_property_deleter(node):
        # If the function is decorated using the prop_method.{setter,getter}
        # form, treat it like an attribute as well.
        return "attr"

    decorators = node.decorators.nodes if node.decorators else []
    for decorator in decorators:
        # If the function is a property (decorated with @property
        # or @abc.abstractproperty), the name type is 'attr'.
        if isinstance(decorator, nodes.Name) or (
            isinstance(decorator, nodes.Attribute)
            and decorator.attrname in property_names
        ):
            inferred = utils.safe_infer(decorator)
            if (
                inferred
                and hasattr(inferred, "qname")
                and inferred.qname() in property_classes
            ):
                return "attr"
    return "method"


def _has_abstract_methods(node):
    """
    Determine if the given `node` has abstract methods.

    The methods should be made abstract by decorating them
    with `abc` decorators.
    """
    return len(utils.unimplemented_abstract_methods(node)) > 0


def report_by_type_stats(
    sect,
    stats: LinterStats,
    old_stats: Optional[LinterStats],
):
    """make a report of

    * percentage of different types documented
    * percentage of different types with a bad name
    """
    # percentage of different types documented and/or with a bad name
    nice_stats: Dict[str, Dict[str, str]] = {}
    for node_type in ("module", "class", "method", "function"):
        node_type = cast(Literal["function", "class", "method", "module"], node_type)
        total = stats.get_node_count(node_type)
        nice_stats[node_type] = {}
        if total != 0:
            undocumented_node = stats.get_undocumented(node_type)
            documented = total - undocumented_node
            percent = (documented * 100.0) / total
            nice_stats[node_type]["percent_documented"] = f"{percent:.2f}"
            badname_node = stats.get_bad_names(node_type)
            percent = (badname_node * 100.0) / total
            nice_stats[node_type]["percent_badname"] = f"{percent:.2f}"
    lines = ["type", "number", "old number", "difference", "%documented", "%badname"]
    for node_type in ("module", "class", "method", "function"):
        node_type = cast(Literal["function", "class", "method", "module"], node_type)
        new = stats.get_node_count(node_type)
        old = old_stats.get_node_count(node_type) if old_stats else None
        diff_str = lint_utils.diff_string(old, new) if old else None
        lines += [
            node_type,
            str(new),
            str(old) if old else "NC",
            diff_str if diff_str else "NC",
            nice_stats[node_type].get("percent_documented", "0"),
            nice_stats[node_type].get("percent_badname", "0"),
        ]
    sect.append(reporter_nodes.Table(children=lines, cols=6, rheaders=1))


def redefined_by_decorator(node):
    """return True if the object is a method redefined via decorator.

    For example:
        @property
        def x(self): return self._x
        @x.setter
        def x(self, value): self._x = value
    """
    if node.decorators:
        for decorator in node.decorators.nodes:
            if (
                isinstance(decorator, nodes.Attribute)
                and getattr(decorator.expr, "name", None) == node.name
            ):
                return True
    return False


class _BasicChecker(checkers.BaseChecker):
    __implements__ = interfaces.IAstroidChecker
    name = "basic"


class BasicErrorChecker(_BasicChecker):
    msgs = {
        "E0100": (
            "__init__ method is a generator",
            "init-is-generator",
            "Used when the special class method __init__ is turned into a "
            "generator by a yield in its body.",
        ),
        "E0101": (
            "Explicit return in __init__",
            "return-in-init",
            "Used when the special class method __init__ has an explicit "
            "return value.",
        ),
        "E0102": (
            "%s already defined line %s",
            "function-redefined",
            "Used when a function / class / method is redefined.",
        ),
        "E0103": (
            "%r not properly in loop",
            "not-in-loop",
            "Used when break or continue keywords are used outside a loop.",
        ),
        "E0104": (
            "Return outside function",
            "return-outside-function",
            'Used when a "return" statement is found outside a function or method.',
        ),
        "E0105": (
            "Yield outside function",
            "yield-outside-function",
            'Used when a "yield" statement is found outside a function or method.',
        ),
        "E0106": (
            "Return with argument inside generator",
            "return-arg-in-generator",
            'Used when a "return" statement with an argument is found '
            "outside in a generator function or method (e.g. with some "
            '"yield" statements).',
            {"maxversion": (3, 3)},
        ),
        "E0107": (
            "Use of the non-existent %s operator",
            "nonexistent-operator",
            "Used when you attempt to use the C-style pre-increment or "
            "pre-decrement operator -- and ++, which doesn't exist in Python.",
        ),
        "E0108": (
            "Duplicate argument name %s in function definition",
            "duplicate-argument-name",
            "Duplicate argument names in function definitions are syntax errors.",
        ),
        "E0110": (
            "Abstract class %r with abstract methods instantiated",
            "abstract-class-instantiated",
            "Used when an abstract class with `abc.ABCMeta` as metaclass "
            "has abstract methods and is instantiated.",
        ),
        "W0120": (
            "Else clause on loop without a break statement",
            "useless-else-on-loop",
            "Loops should only have an else clause if they can exit early "
            "with a break statement, otherwise the statements under else "
            "should be on the same scope as the loop itself.",
        ),
        "E0112": (
            "More than one starred expression in assignment",
            "too-many-star-expressions",
            "Emitted when there are more than one starred "
            "expressions (`*x`) in an assignment. This is a SyntaxError.",
        ),
        "E0113": (
            "Starred assignment target must be in a list or tuple",
            "invalid-star-assignment-target",
            "Emitted when a star expression is used as a starred assignment target.",
        ),
        "E0114": (
            "Can use starred expression only in assignment target",
            "star-needs-assignment-target",
            "Emitted when a star expression is not used in an assignment target.",
        ),
        "E0115": (
            "Name %r is nonlocal and global",
            "nonlocal-and-global",
            "Emitted when a name is both nonlocal and global.",
        ),
        "E0116": (
            "'continue' not supported inside 'finally' clause",
            "continue-in-finally",
            "Emitted when the `continue` keyword is found "
            "inside a finally clause, which is a SyntaxError.",
            {"maxversion": (3, 8)},
        ),
        "E0117": (
            "nonlocal name %s found without binding",
            "nonlocal-without-binding",
            "Emitted when a nonlocal variable does not have an attached "
            "name somewhere in the parent scopes",
        ),
        "E0118": (
            "Name %r is used prior to global declaration",
            "used-prior-global-declaration",
            "Emitted when a name is used prior a global declaration, "
            "which results in an error since Python 3.6.",
            {"minversion": (3, 6)},
        ),
    }

    @utils.check_messages("function-redefined")
    def visit_classdef(self, node: nodes.ClassDef) -> None:
        self._check_redefinition("class", node)

    def _too_many_starred_for_tuple(self, assign_tuple):
        starred_count = 0
        for elem in assign_tuple.itered():
            if isinstance(elem, nodes.Tuple):
                return self._too_many_starred_for_tuple(elem)
            if isinstance(elem, nodes.Starred):
                starred_count += 1
        return starred_count > 1

    @utils.check_messages("too-many-star-expressions", "invalid-star-assignment-target")
    def visit_assign(self, node: nodes.Assign) -> None:
        # Check *a, *b = ...
        assign_target = node.targets[0]
        # Check *a = b
        if isinstance(node.targets[0], nodes.Starred):
            self.add_message("invalid-star-assignment-target", node=node)

        if not isinstance(assign_target, nodes.Tuple):
            return
        if self._too_many_starred_for_tuple(assign_target):
            self.add_message("too-many-star-expressions", node=node)

    @utils.check_messages("star-needs-assignment-target")
    def visit_starred(self, node: nodes.Starred) -> None:
        """Check that a Starred expression is used in an assignment target."""
        if isinstance(node.parent, nodes.Call):
            # f(*args) is converted to Call(args=[Starred]), so ignore
            # them for this check.
            return
        if isinstance(node.parent, (nodes.List, nodes.Tuple, nodes.Set, nodes.Dict)):
            # PEP 448 unpacking.
            return

        stmt = node.statement()
        if not isinstance(stmt, nodes.Assign):
            return

        if stmt.value is node or stmt.value.parent_of(node):
            self.add_message("star-needs-assignment-target", node=node)

    @utils.check_messages(
        "init-is-generator",
        "return-in-init",
        "function-redefined",
        "return-arg-in-generator",
        "duplicate-argument-name",
        "nonlocal-and-global",
        "used-prior-global-declaration",
    )
    def visit_functiondef(self, node: nodes.FunctionDef) -> None:
        self._check_nonlocal_and_global(node)
        self._check_name_used_prior_global(node)
        if not redefined_by_decorator(
            node
        ) and not utils.is_registered_in_singledispatch_function(node):
            self._check_redefinition(node.is_method() and "method" or "function", node)
        # checks for max returns, branch, return in __init__
        returns = node.nodes_of_class(
            nodes.Return, skip_klass=(nodes.FunctionDef, nodes.ClassDef)
        )
        if node.is_method() and node.name == "__init__":
            if node.is_generator():
                self.add_message("init-is-generator", node=node)
            else:
                values = [r.value for r in returns]
                # Are we returning anything but None from constructors
                if any(v for v in values if not utils.is_none(v)):
                    self.add_message("return-in-init", node=node)
        # Check for duplicate names by clustering args with same name for detailed report
        arg_clusters = collections.defaultdict(list)
        arguments: Iterator[Any] = filter(None, [node.args.args, node.args.kwonlyargs])

        for arg in itertools.chain.from_iterable(arguments):
            arg_clusters[arg.name].append(arg)

        # provide detailed report about each repeated argument
        for argument_duplicates in arg_clusters.values():
            if len(argument_duplicates) != 1:
                for argument in argument_duplicates:
                    self.add_message(
                        "duplicate-argument-name",
                        line=argument.lineno,
                        node=argument,
                        args=(argument.name,),
                    )

    visit_asyncfunctiondef = visit_functiondef

    def _check_name_used_prior_global(self, node):

        scope_globals = {
            name: child
            for child in node.nodes_of_class(nodes.Global)
            for name in child.names
            if child.scope() is node
        }

        if not scope_globals:
            return

        for node_name in node.nodes_of_class(nodes.Name):
            if node_name.scope() is not node:
                continue

            name = node_name.name
            corresponding_global = scope_globals.get(name)
            if not corresponding_global:
                continue

            global_lineno = corresponding_global.fromlineno
            if global_lineno and global_lineno > node_name.fromlineno:
                self.add_message(
                    "used-prior-global-declaration", node=node_name, args=(name,)
                )

    def _check_nonlocal_and_global(self, node):
        """Check that a name is both nonlocal and global."""

        def same_scope(current):
            return current.scope() is node

        from_iter = itertools.chain.from_iterable
        nonlocals = set(
            from_iter(
                child.names
                for child in node.nodes_of_class(nodes.Nonlocal)
                if same_scope(child)
            )
        )

        if not nonlocals:
            return

        global_vars = set(
            from_iter(
                child.names
                for child in node.nodes_of_class(nodes.Global)
                if same_scope(child)
            )
        )
        for name in nonlocals.intersection(global_vars):
            self.add_message("nonlocal-and-global", args=(name,), node=node)

    @utils.check_messages("return-outside-function")
    def visit_return(self, node: nodes.Return) -> None:
        if not isinstance(node.frame(), nodes.FunctionDef):
            self.add_message("return-outside-function", node=node)

    @utils.check_messages("yield-outside-function")
    def visit_yield(self, node: nodes.Yield) -> None:
        self._check_yield_outside_func(node)

    @utils.check_messages("yield-outside-function")
    def visit_yieldfrom(self, node: nodes.YieldFrom) -> None:
        self._check_yield_outside_func(node)

    @utils.check_messages("not-in-loop", "continue-in-finally")
    def visit_continue(self, node: nodes.Continue) -> None:
        self._check_in_loop(node, "continue")

    @utils.check_messages("not-in-loop")
    def visit_break(self, node: nodes.Break) -> None:
        self._check_in_loop(node, "break")

    @utils.check_messages("useless-else-on-loop")
    def visit_for(self, node: nodes.For) -> None:
        self._check_else_on_loop(node)

    @utils.check_messages("useless-else-on-loop")
    def visit_while(self, node: nodes.While) -> None:
        self._check_else_on_loop(node)

    @utils.check_messages("nonexistent-operator")
    def visit_unaryop(self, node: nodes.UnaryOp) -> None:
        """check use of the non-existent ++ and -- operator operator"""
        if (
            (node.op in "+-")
            and isinstance(node.operand, nodes.UnaryOp)
            and (node.operand.op == node.op)
        ):
            self.add_message("nonexistent-operator", node=node, args=node.op * 2)

    def _check_nonlocal_without_binding(self, node, name):
        current_scope = node.scope()
        while True:
            if current_scope.parent is None:
                break

            if not isinstance(current_scope, (nodes.ClassDef, nodes.FunctionDef)):
                self.add_message("nonlocal-without-binding", args=(name,), node=node)
                return

            if name not in current_scope.locals:
                current_scope = current_scope.parent.scope()
                continue

            # Okay, found it.
            return

        if not isinstance(current_scope, nodes.FunctionDef):
            self.add_message("nonlocal-without-binding", args=(name,), node=node)

    @utils.check_messages("nonlocal-without-binding")
    def visit_nonlocal(self, node: nodes.Nonlocal) -> None:
        for name in node.names:
            self._check_nonlocal_without_binding(node, name)

    @utils.check_messages("abstract-class-instantiated")
    def visit_call(self, node: nodes.Call) -> None:
        """Check instantiating abstract class with
        abc.ABCMeta as metaclass.
        """
        for inferred in infer_all(node.func):
            self._check_inferred_class_is_abstract(inferred, node)

    def _check_inferred_class_is_abstract(self, inferred, node):
        if not isinstance(inferred, nodes.ClassDef):
            return

        klass = utils.node_frame_class(node)
        if klass is inferred:
            # Don't emit the warning if the class is instantiated
            # in its own body or if the call is not an instance
            # creation. If the class is instantiated into its own
            # body, we're expecting that it knows what it is doing.
            return

        # __init__ was called
        abstract_methods = _has_abstract_methods(inferred)

        if not abstract_methods:
            return

        metaclass = inferred.metaclass()

        if metaclass is None:
            # Python 3.4 has `abc.ABC`, which won't be detected
            # by ClassNode.metaclass()
            for ancestor in inferred.ancestors():
                if ancestor.qname() == "abc.ABC":
                    self.add_message(
                        "abstract-class-instantiated", args=(inferred.name,), node=node
                    )
                    break

            return

        if metaclass.qname() in ABC_METACLASSES:
            self.add_message(
                "abstract-class-instantiated", args=(inferred.name,), node=node
            )

    def _check_yield_outside_func(self, node):
        if not isinstance(node.frame(), (nodes.FunctionDef, nodes.Lambda)):
            self.add_message("yield-outside-function", node=node)

    def _check_else_on_loop(self, node):
        """Check that any loop with an else clause has a break statement."""
        if node.orelse and not _loop_exits_early(node):
            self.add_message(
                "useless-else-on-loop",
                node=node,
                # This is not optimal, but the line previous
                # to the first statement in the else clause
                # will usually be the one that contains the else:.
                line=node.orelse[0].lineno - 1,
            )

    def _check_in_loop(self, node, node_name):
        """check that a node is inside a for or while loop"""
        for parent in node.node_ancestors():
            if isinstance(parent, (nodes.For, nodes.While)):
                if node not in parent.orelse:
                    return

            if isinstance(parent, (nodes.ClassDef, nodes.FunctionDef)):
                break
            if (
                isinstance(parent, nodes.TryFinally)
                and node in parent.finalbody
                and isinstance(node, nodes.Continue)
            ):
                self.add_message("continue-in-finally", node=node)

        self.add_message("not-in-loop", node=node, args=node_name)

    def _check_redefinition(self, redeftype, node):
        """check for redefinition of a function / method / class name"""
        parent_frame = node.parent.frame()

        # Ignore function stubs created for type information
        redefinitions = [
            i
            for i in parent_frame.locals[node.name]
            if not (isinstance(i.parent, nodes.AnnAssign) and i.parent.simple)
        ]
        defined_self = next(
            (local for local in redefinitions if not utils.is_overload_stub(local)),
            node,
        )
        if defined_self is not node and not astroid.are_exclusive(node, defined_self):
            # Additional checks for methods which are not considered
            # redefined, since they are already part of the base API.
            if (
                isinstance(parent_frame, nodes.ClassDef)
                and node.name in REDEFINABLE_METHODS
            ):
                return

            # Skip typing.overload() functions.
            if utils.is_overload_stub(node):
                return

            # Exempt functions redefined on a condition.
            if isinstance(node.parent, nodes.If):
                # Exempt "if not <func>" cases
                if (
                    isinstance(node.parent.test, nodes.UnaryOp)
                    and node.parent.test.op == "not"
                    and isinstance(node.parent.test.operand, nodes.Name)
                    and node.parent.test.operand.name == node.name
                ):
                    return

                # Exempt "if <func> is not None" cases
                # pylint: disable=too-many-boolean-expressions
                if (
                    isinstance(node.parent.test, nodes.Compare)
                    and isinstance(node.parent.test.left, nodes.Name)
                    and node.parent.test.left.name == node.name
                    and node.parent.test.ops[0][0] == "is"
                    and isinstance(node.parent.test.ops[0][1], nodes.Const)
                    and node.parent.test.ops[0][1].value is None
                ):
                    return

            # Check if we have forward references for this node.
            try:
                redefinition_index = redefinitions.index(node)
            except ValueError:
                pass
            else:
                for redefinition in redefinitions[:redefinition_index]:
                    inferred = utils.safe_infer(redefinition)
                    if (
                        inferred
                        and isinstance(inferred, astroid.Instance)
                        and inferred.qname() == TYPING_FORWARD_REF_QNAME
                    ):
                        return

            dummy_variables_rgx = lint_utils.get_global_option(
                self, "dummy-variables-rgx", default=None
            )
            if dummy_variables_rgx and dummy_variables_rgx.match(node.name):
                return
            self.add_message(
                "function-redefined",
                node=node,
                args=(redeftype, defined_self.fromlineno),
            )


class BasicChecker(_BasicChecker):
    """checks for :
    * doc strings
    * number of arguments, local variables, branches, returns and statements in
    functions, methods
    * required module attributes
    * dangerous default values as arguments
    * redefinition of function / method / class
    * uses of the global statement
    """

    __implements__ = interfaces.IAstroidChecker

    name = "basic"
    msgs = {
        "W0101": (
            "Unreachable code",
            "unreachable",
            'Used when there is some code behind a "return" or "raise" '
            "statement, which will never be accessed.",
        ),
        "W0102": (
            "Dangerous default value %s as argument",
            "dangerous-default-value",
            "Used when a mutable value as list or dictionary is detected in "
            "a default value for an argument.",
        ),
        "W0104": (
            "Statement seems to have no effect",
            "pointless-statement",
            "Used when a statement doesn't have (or at least seems to) any effect.",
        ),
        "W0105": (
            "String statement has no effect",
            "pointless-string-statement",
            "Used when a string is used as a statement (which of course "
            "has no effect). This is a particular case of W0104 with its "
            "own message so you can easily disable it if you're using "
            "those strings as documentation, instead of comments.",
        ),
        "W0106": (
            'Expression "%s" is assigned to nothing',
            "expression-not-assigned",
            "Used when an expression that is not a function call is assigned "
            "to nothing. Probably something else was intended.",
        ),
        "W0108": (
            "Lambda may not be necessary",
            "unnecessary-lambda",
            "Used when the body of a lambda expression is a function call "
            "on the same argument list as the lambda itself; such lambda "
            "expressions are in all but a few cases replaceable with the "
            "function being called in the body of the lambda.",
        ),
        "W0109": (
            "Duplicate key %r in dictionary",
            "duplicate-key",
            "Used when a dictionary expression binds the same key multiple times.",
        ),
        "W0122": (
            "Use of exec",
            "exec-used",
            'Used when you use the "exec" statement (function for Python '
            "3), to discourage its usage. That doesn't "
            "mean you cannot use it !",
        ),
        "W0123": (
            "Use of eval",
            "eval-used",
            'Used when you use the "eval" function, to discourage its '
            "usage. Consider using `ast.literal_eval` for safely evaluating "
            "strings containing Python expressions "
            "from untrusted sources. ",
        ),
        "W0150": (
            "%s statement in finally block may swallow exception",
            "lost-exception",
            "Used when a break or a return statement is found inside the "
            "finally clause of a try...finally block: the exceptions raised "
            "in the try clause will be silently swallowed instead of being "
            "re-raised.",
        ),
        "W0199": (
            "Assert called on a 2-item-tuple. Did you mean 'assert x,y'?",
            "assert-on-tuple",
            "A call of assert on a tuple will always evaluate to true if "
            "the tuple is not empty, and will always evaluate to false if "
            "it is.",
        ),
        "W0124": (
            'Following "as" with another context manager looks like a tuple.',
            "confusing-with-statement",
            "Emitted when a `with` statement component returns multiple values "
            "and uses name binding with `as` only for a part of those values, "
            "as in with ctx() as a, b. This can be misleading, since it's not "
            "clear if the context manager returns a tuple or if the node without "
            "a name binding is another context manager.",
        ),
        "W0125": (
            "Using a conditional statement with a constant value",
            "using-constant-test",
            "Emitted when a conditional statement (If or ternary if) "
            "uses a constant value for its test. This might not be what "
            "the user intended to do.",
        ),
        "W0126": (
            "Using a conditional statement with potentially wrong function or method call due to missing parentheses",
            "missing-parentheses-for-call-in-test",
            "Emitted when a conditional statement (If or ternary if) "
            "seems to wrongly call a function due to missing parentheses",
        ),
        "W0127": (
            "Assigning the same variable %r to itself",
            "self-assigning-variable",
            "Emitted when we detect that a variable is assigned to itself",
        ),
        "W0128": (
            "Redeclared variable %r in assignment",
            "redeclared-assigned-name",
            "Emitted when we detect that a variable was redeclared in the same assignment.",
        ),
        "E0111": (
            "The first reversed() argument is not a sequence",
            "bad-reversed-sequence",
            "Used when the first argument to reversed() builtin "
            "isn't a sequence (does not implement __reversed__, "
            "nor __getitem__ and __len__",
        ),
        "E0119": (
            "format function is not called on str",
            "misplaced-format-function",
            "Emitted when format function is not called on str object. "
            'e.g doing print("value: {}").format(123) instead of '
            'print("value: {}".format(123)). This might not be what the user '
            "intended to do.",
        ),
        "W0129": (
            "Assert statement has a string literal as its first argument. The assert will %s fail.",
            "assert-on-string-literal",
            "Used when an assert statement has a string literal as its first argument, which will "
            "cause the assert to always pass.",
        ),
    }

    reports = (("RP0101", "Statistics by type", report_by_type_stats),)

    def __init__(self, linter):
        super().__init__(linter)
        self._tryfinallys = None

    def open(self):
        """initialize visit variables and statistics"""
        py_version = get_global_option(self, "py-version")
        self._py38_plus = py_version >= (3, 8)
        self._tryfinallys = []
        self.linter.stats.reset_node_count()

    @utils.check_messages("using-constant-test", "missing-parentheses-for-call-in-test")
    def visit_if(self, node: nodes.If) -> None:
        self._check_using_constant_test(node, node.test)

    @utils.check_messages("using-constant-test", "missing-parentheses-for-call-in-test")
    def visit_ifexp(self, node: nodes.IfExp) -> None:
        self._check_using_constant_test(node, node.test)

    @utils.check_messages("using-constant-test", "missing-parentheses-for-call-in-test")
    def visit_comprehension(self, node: nodes.Comprehension) -> None:
        if node.ifs:
            for if_test in node.ifs:
                self._check_using_constant_test(node, if_test)

    def _check_using_constant_test(self, node, test):
        const_nodes = (
            nodes.Module,
            nodes.GeneratorExp,
            nodes.Lambda,
            nodes.FunctionDef,
            nodes.ClassDef,
            astroid.bases.Generator,
            astroid.UnboundMethod,
            astroid.BoundMethod,
            nodes.Module,
        )
        structs = (nodes.Dict, nodes.Tuple, nodes.Set, nodes.List)

        # These nodes are excepted, since they are not constant
        # values, requiring a computation to happen.
        except_nodes = (
            nodes.Call,
            nodes.BinOp,
            nodes.BoolOp,
            nodes.UnaryOp,
            nodes.Subscript,
        )
        inferred = None
        emit = isinstance(test, (nodes.Const,) + structs + const_nodes)
        if not isinstance(test, except_nodes):
            inferred = utils.safe_infer(test)

        if emit:
            self.add_message("using-constant-test", node=node)
        elif isinstance(inferred, const_nodes):
            # If the constant node is a FunctionDef or Lambda then
            # it may be an illicit function call due to missing parentheses
            call_inferred = None
            try:
                if isinstance(inferred, nodes.FunctionDef):
                    call_inferred = inferred.infer_call_result()
                elif isinstance(inferred, nodes.Lambda):
                    call_inferred = inferred.infer_call_result(node)
            except astroid.InferenceError:
                call_inferred = None
            if call_inferred:
                try:
                    for inf_call in call_inferred:
                        if inf_call != astroid.Uninferable:
                            self.add_message(
                                "missing-parentheses-for-call-in-test", node=node
                            )
                            break
                except astroid.InferenceError:
                    pass
            self.add_message("using-constant-test", node=node)

    def visit_module(self, _: nodes.Module) -> None:
        """check module name, docstring and required arguments"""
        self.linter.stats.node_count["module"] += 1

    def visit_classdef(self, _: nodes.ClassDef) -> None:
        """check module name, docstring and redefinition
        increment branch counter
        """
        self.linter.stats.node_count["klass"] += 1

    @utils.check_messages(
        "pointless-statement", "pointless-string-statement", "expression-not-assigned"
    )
    def visit_expr(self, node: nodes.Expr) -> None:
        """Check for various kind of statements without effect"""
        expr = node.value
        if isinstance(expr, nodes.Const) and isinstance(expr.value, str):
            # treat string statement in a separated message
            # Handle PEP-257 attribute docstrings.
            # An attribute docstring is defined as being a string right after
            # an assignment at the module level, class level or __init__ level.
            scope = expr.scope()
            if isinstance(scope, (nodes.ClassDef, nodes.Module, nodes.FunctionDef)):
                if isinstance(scope, nodes.FunctionDef) and scope.name != "__init__":
                    pass
                else:
                    sibling = expr.previous_sibling()
                    if (
                        sibling is not None
                        and sibling.scope() is scope
                        and isinstance(sibling, (nodes.Assign, nodes.AnnAssign))
                    ):
                        return
            self.add_message("pointless-string-statement", node=node)
            return

        # Ignore if this is :
        # * a direct function call
        # * the unique child of a try/except body
        # * a yield statement
        # * an ellipsis (which can be used on Python 3 instead of pass)
        # warn W0106 if we have any underlying function call (we can't predict
        # side effects), else pointless-statement
        if (
            isinstance(expr, (nodes.Yield, nodes.Await, nodes.Call))
            or (isinstance(node.parent, nodes.TryExcept) and node.parent.body == [node])
            or (isinstance(expr, nodes.Const) and expr.value is Ellipsis)
        ):
            return
        if any(expr.nodes_of_class(nodes.Call)):
            self.add_message(
                "expression-not-assigned", node=node, args=expr.as_string()
            )
        else:
            self.add_message("pointless-statement", node=node)

    @staticmethod
    def _filter_vararg(node, call_args):
        # Return the arguments for the given call which are
        # not passed as vararg.
        for arg in call_args:
            if isinstance(arg, nodes.Starred):
                if (
                    isinstance(arg.value, nodes.Name)
                    and arg.value.name != node.args.vararg
                ):
                    yield arg
            else:
                yield arg

    @staticmethod
    def _has_variadic_argument(args, variadic_name):
        if not args:
            return True
        for arg in args:
            if isinstance(arg.value, nodes.Name):
                if arg.value.name != variadic_name:
                    return True
            else:
                return True
        return False

    @utils.check_messages("unnecessary-lambda")
    def visit_lambda(self, node: nodes.Lambda) -> None:
        """check whether or not the lambda is suspicious"""
        # if the body of the lambda is a call expression with the same
        # argument list as the lambda itself, then the lambda is
        # possibly unnecessary and at least suspicious.
        if node.args.defaults:
            # If the arguments of the lambda include defaults, then a
            # judgment cannot be made because there is no way to check
            # that the defaults defined by the lambda are the same as
            # the defaults defined by the function called in the body
            # of the lambda.
            return
        call = node.body
        if not isinstance(call, nodes.Call):
            # The body of the lambda must be a function call expression
            # for the lambda to be unnecessary.
            return
        if isinstance(node.body.func, nodes.Attribute) and isinstance(
            node.body.func.expr, nodes.Call
        ):
            # Chained call, the intermediate call might
            # return something else (but we don't check that, yet).
            return

        call_site = astroid.arguments.CallSite.from_call(call)
        ordinary_args = list(node.args.args)
        new_call_args = list(self._filter_vararg(node, call.args))
        if node.args.kwarg:
            if self._has_variadic_argument(call.kwargs, node.args.kwarg):
                return

        if node.args.vararg:
            if self._has_variadic_argument(call.starargs, node.args.vararg):
                return
        elif call.starargs:
            return

        if call.keywords:
            # Look for additional keyword arguments that are not part
            # of the lambda's signature
            lambda_kwargs = {keyword.name for keyword in node.args.defaults}
            if len(lambda_kwargs) != len(call_site.keyword_arguments):
                # Different lengths, so probably not identical
                return
            if set(call_site.keyword_arguments).difference(lambda_kwargs):
                return

        # The "ordinary" arguments must be in a correspondence such that:
        # ordinary_args[i].name == call.args[i].name.
        if len(ordinary_args) != len(new_call_args):
            return
        for arg, passed_arg in zip(ordinary_args, new_call_args):
            if not isinstance(passed_arg, nodes.Name):
                return
            if arg.name != passed_arg.name:
                return

        self.add_message("unnecessary-lambda", line=node.fromlineno, node=node)

    @utils.check_messages("dangerous-default-value")
    def visit_functiondef(self, node: nodes.FunctionDef) -> None:
        """check function name, docstring, arguments, redefinition,
        variable names, max locals
        """
        if node.is_method():
            self.linter.stats.node_count["method"] += 1
        else:
            self.linter.stats.node_count["function"] += 1
        self._check_dangerous_default(node)

    visit_asyncfunctiondef = visit_functiondef

    def _check_dangerous_default(self, node):
        """Check for dangerous default values as arguments."""

        def is_iterable(internal_node):
            return isinstance(internal_node, (nodes.List, nodes.Set, nodes.Dict))

        defaults = node.args.defaults or [] + node.args.kw_defaults or []
        for default in defaults:
            if not default:
                continue
            try:
                value = next(default.infer())
            except astroid.InferenceError:
                continue

            if (
                isinstance(value, astroid.Instance)
                and value.qname() in DEFAULT_ARGUMENT_SYMBOLS
            ):
                if value is default:
                    msg = DEFAULT_ARGUMENT_SYMBOLS[value.qname()]
                elif isinstance(value, astroid.Instance) or is_iterable(value):
                    # We are here in the following situation(s):
                    #   * a dict/set/list/tuple call which wasn't inferred
                    #     to a syntax node ({}, () etc.). This can happen
                    #     when the arguments are invalid or unknown to
                    #     the inference.
                    #   * a variable from somewhere else, which turns out to be a list
                    #     or a dict.
                    if is_iterable(default):
                        msg = value.pytype()
                    elif isinstance(default, nodes.Call):
                        msg = f"{value.name}() ({value.qname()})"
                    else:
                        msg = f"{default.as_string()} ({value.qname()})"
                else:
                    # this argument is a name
                    msg = f"{default.as_string()} ({DEFAULT_ARGUMENT_SYMBOLS[value.qname()]})"
                self.add_message("dangerous-default-value", node=node, args=(msg,))

    @utils.check_messages("unreachable", "lost-exception")
    def visit_return(self, node: nodes.Return) -> None:
        """1 - check is the node has a right sibling (if so, that's some
        unreachable code)
        2 - check is the node is inside the finally clause of a try...finally
        block
        """
        self._check_unreachable(node)
        # Is it inside final body of a try...finally block ?
        self._check_not_in_finally(node, "return", (nodes.FunctionDef,))

    @utils.check_messages("unreachable")
    def visit_continue(self, node: nodes.Continue) -> None:
        """check is the node has a right sibling (if so, that's some unreachable
        code)
        """
        self._check_unreachable(node)

    @utils.check_messages("unreachable", "lost-exception")
    def visit_break(self, node: nodes.Break) -> None:
        """1 - check is the node has a right sibling (if so, that's some
        unreachable code)
        2 - check is the node is inside the finally clause of a try...finally
        block
        """
        # 1 - Is it right sibling ?
        self._check_unreachable(node)
        # 2 - Is it inside final body of a try...finally block ?
        self._check_not_in_finally(node, "break", (nodes.For, nodes.While))

    @utils.check_messages("unreachable")
    def visit_raise(self, node: nodes.Raise) -> None:
        """check if the node has a right sibling (if so, that's some unreachable
        code)
        """
        self._check_unreachable(node)

    def _check_misplaced_format_function(self, call_node):
        if not isinstance(call_node.func, nodes.Attribute):
            return
        if call_node.func.attrname != "format":
            return

        expr = utils.safe_infer(call_node.func.expr)
        if expr is astroid.Uninferable:
            return
        if not expr:
            # we are doubtful on inferred type of node, so here just check if format
            # was called on print()
            call_expr = call_node.func.expr
            if not isinstance(call_expr, nodes.Call):
                return
            if (
                isinstance(call_expr.func, nodes.Name)
                and call_expr.func.name == "print"
            ):
                self.add_message("misplaced-format-function", node=call_node)

    @utils.check_messages(
        "eval-used", "exec-used", "bad-reversed-sequence", "misplaced-format-function"
    )
    def visit_call(self, node: nodes.Call) -> None:
        """visit a Call node -> check if this is not a disallowed builtin
        call and check for * or ** use
        """
        self._check_misplaced_format_function(node)
        if isinstance(node.func, nodes.Name):
            name = node.func.name
            # ignore the name if it's not a builtin (i.e. not defined in the
            # locals nor globals scope)
            if not (name in node.frame() or name in node.root()):
                if name == "exec":
                    self.add_message("exec-used", node=node)
                elif name == "reversed":
                    self._check_reversed(node)
                elif name == "eval":
                    self.add_message("eval-used", node=node)

    @utils.check_messages("assert-on-tuple", "assert-on-string-literal")
    def visit_assert(self, node: nodes.Assert) -> None:
        """check whether assert is used on a tuple or string literal."""
        if (
            node.fail is None
            and isinstance(node.test, nodes.Tuple)
            and len(node.test.elts) == 2
        ):
            self.add_message("assert-on-tuple", node=node)

        if isinstance(node.test, nodes.Const) and isinstance(node.test.value, str):
            if node.test.value:
                when = "never"
            else:
                when = "always"
            self.add_message("assert-on-string-literal", node=node, args=(when,))

    @utils.check_messages("duplicate-key")
    def visit_dict(self, node: nodes.Dict) -> None:
        """check duplicate key in dictionary"""
        keys = set()
        for k, _ in node.items:
            if isinstance(k, nodes.Const):
                key = k.value
            elif isinstance(k, nodes.Attribute):
                key = k.as_string()
            else:
                continue
            if key in keys:
                self.add_message("duplicate-key", node=node, args=key)
            keys.add(key)

    def visit_tryfinally(self, node: nodes.TryFinally) -> None:
        """update try...finally flag"""
        self._tryfinallys.append(node)

    def leave_tryfinally(self, _: nodes.TryFinally) -> None:
        """update try...finally flag"""
        self._tryfinallys.pop()

    def _check_unreachable(self, node):
        """check unreachable code"""
        unreach_stmt = node.next_sibling()
        if unreach_stmt is not None:
            if (
                isinstance(node, nodes.Return)
                and isinstance(unreach_stmt, nodes.Expr)
                and isinstance(unreach_stmt.value, nodes.Yield)
            ):
                # Don't add 'unreachable' for empty generators.
                # Only add warning if 'yield' is followed by another node.
                unreach_stmt = unreach_stmt.next_sibling()
                if unreach_stmt is None:
                    return
            self.add_message("unreachable", node=unreach_stmt)

    def _check_not_in_finally(self, node, node_name, breaker_classes=()):
        """check that a node is not inside a finally clause of a
        try...finally statement.
        If we found before a try...finally block a parent which its type is
        in breaker_classes, we skip the whole check."""
        # if self._tryfinallys is empty, we're not an in try...finally block
        if not self._tryfinallys:
            return
        # the node could be a grand-grand...-children of the try...finally
        _parent = node.parent
        _node = node
        while _parent and not isinstance(_parent, breaker_classes):
            if hasattr(_parent, "finalbody") and _node in _parent.finalbody:
                self.add_message("lost-exception", node=node, args=node_name)
                return
            _node = _parent
            _parent = _node.parent

    def _check_reversed(self, node):
        """check that the argument to `reversed` is a sequence"""
        try:
            argument = utils.safe_infer(utils.get_argument_from_call(node, position=0))
        except utils.NoSuchArgumentError:
            pass
        else:
            if argument is astroid.Uninferable:
                return
            if argument is None:
                # Nothing was inferred.
                # Try to see if we have iter().
                if isinstance(node.args[0], nodes.Call):
                    try:
                        func = next(node.args[0].func.infer())
                    except astroid.InferenceError:
                        return
                    if getattr(
                        func, "name", None
                    ) == "iter" and utils.is_builtin_object(func):
                        self.add_message("bad-reversed-sequence", node=node)
                return

            if isinstance(argument, (nodes.List, nodes.Tuple)):
                return

            # dicts are reversible, but only from Python 3.8 onwards. Prior to
            # that, any class based on dict must explicitly provide a
            # __reversed__ method
            if not self._py38_plus and isinstance(argument, astroid.Instance):
                if any(
                    ancestor.name == "dict" and utils.is_builtin_object(ancestor)
                    for ancestor in itertools.chain(
                        (argument._proxied,), argument._proxied.ancestors()
                    )
                ):
                    try:
                        argument.locals[REVERSED_PROTOCOL_METHOD]
                    except KeyError:
                        self.add_message("bad-reversed-sequence", node=node)
                    return

            if hasattr(argument, "getattr"):
                # everything else is not a proper sequence for reversed()
                for methods in REVERSED_METHODS:
                    for meth in methods:
                        try:
                            argument.getattr(meth)
                        except astroid.NotFoundError:
                            break
                    else:
                        break
                else:
                    self.add_message("bad-reversed-sequence", node=node)
            else:
                self.add_message("bad-reversed-sequence", node=node)

    @utils.check_messages("confusing-with-statement")
    def visit_with(self, node: nodes.With) -> None:
        # a "with" statement with multiple managers corresponds
        # to one AST "With" node with multiple items
        pairs = node.items
        if pairs:
            for prev_pair, pair in zip(pairs, pairs[1:]):
                if isinstance(prev_pair[1], nodes.AssignName) and (
                    pair[1] is None and not isinstance(pair[0], nodes.Call)
                ):
                    # Don't emit a message if the second is a function call
                    # there's no way that can be mistaken for a name assignment.
                    # If the line number doesn't match
                    # we assume it's a nested "with".
                    self.add_message("confusing-with-statement", node=node)

    def _check_self_assigning_variable(self, node):
        # Detect assigning to the same variable.

        scope = node.scope()
        scope_locals = scope.locals

        rhs_names = []
        targets = node.targets
        if isinstance(targets[0], nodes.Tuple):
            if len(targets) != 1:
                # A complex assignment, so bail out early.
                return
            targets = targets[0].elts
            if len(targets) == 1:
                # Unpacking a variable into the same name.
                return

        if isinstance(node.value, nodes.Name):
            if len(targets) != 1:
                return
            rhs_names = [node.value]
        elif isinstance(node.value, nodes.Tuple):
            rhs_count = len(node.value.elts)
            if len(targets) != rhs_count or rhs_count == 1:
                return
            rhs_names = node.value.elts

        for target, lhs_name in zip(targets, rhs_names):
            if not isinstance(lhs_name, nodes.Name):
                continue
            if not isinstance(target, nodes.AssignName):
                continue
            if isinstance(scope, nodes.ClassDef) and target.name in scope_locals:
                # Check that the scope is different than a class level, which is usually
                # a pattern to expose module level attributes as class level ones.
                continue
            if target.name == lhs_name.name:
                self.add_message(
                    "self-assigning-variable", args=(target.name,), node=target
                )

    def _check_redeclared_assign_name(self, targets):
        dummy_variables_rgx = lint_utils.get_global_option(
            self, "dummy-variables-rgx", default=None
        )

        for target in targets:
            if not isinstance(target, nodes.Tuple):
                continue

            found_names = []
            for element in target.elts:
                if isinstance(element, nodes.Tuple):
                    self._check_redeclared_assign_name([element])
                elif isinstance(element, nodes.AssignName) and element.name != "_":
                    if dummy_variables_rgx and dummy_variables_rgx.match(element.name):
                        return
                    found_names.append(element.name)

            names = collections.Counter(found_names)
            for name, count in names.most_common():
                if count > 1:
                    self.add_message(
                        "redeclared-assigned-name", args=(name,), node=target
                    )

    @utils.check_messages("self-assigning-variable", "redeclared-assigned-name")
    def visit_assign(self, node: nodes.Assign) -> None:
        self._check_self_assigning_variable(node)
        self._check_redeclared_assign_name(node.targets)

    @utils.check_messages("redeclared-assigned-name")
    def visit_for(self, node: nodes.For) -> None:
        self._check_redeclared_assign_name([node.target])


KNOWN_NAME_TYPES = {
    "module",
    "const",
    "class",
    "function",
    "method",
    "attr",
    "argument",
    "variable",
    "class_attribute",
    "class_const",
    "inlinevar",
}

HUMAN_READABLE_TYPES = {
    "module": "module",
    "const": "constant",
    "class": "class",
    "function": "function",
    "method": "method",
    "attr": "attribute",
    "argument": "argument",
    "variable": "variable",
    "class_attribute": "class attribute",
    "class_const": "class constant",
    "inlinevar": "inline iteration",
}

DEFAULT_NAMING_STYLES = {
    "module": "snake_case",
    "const": "UPPER_CASE",
    "class": "PascalCase",
    "function": "snake_case",
    "method": "snake_case",
    "attr": "snake_case",
    "argument": "snake_case",
    "variable": "snake_case",
    "class_attribute": "any",
    "class_const": "UPPER_CASE",
    "inlinevar": "any",
}


def _create_naming_options():
    name_options = []
    for name_type in sorted(KNOWN_NAME_TYPES):
        human_readable_name = HUMAN_READABLE_TYPES[name_type]
        default_style = DEFAULT_NAMING_STYLES[name_type]
        name_type = name_type.replace("_", "-")
        name_options.append(
            (
                f"{name_type}-naming-style",
                {
                    "default": default_style,
                    "type": "choice",
                    "choices": list(NAMING_STYLES.keys()),
                    "metavar": "<style>",
                    "help": f"Naming style matching correct {human_readable_name} names.",
                },
            )
        )
        name_options.append(
            (
                f"{name_type}-rgx",
                {
                    "default": None,
                    "type": "regexp",
                    "metavar": "<regexp>",
                    "help": f"Regular expression matching correct {human_readable_name} names. Overrides {name_type}-naming-style.",
                },
            )
        )
    return tuple(name_options)


class NameChecker(_BasicChecker):
    msgs = {
        "C0103": (
            '%s name "%s" doesn\'t conform to %s',
            "invalid-name",
            "Used when the name doesn't conform to naming rules "
            "associated to its type (constant, variable, class...).",
        ),
        "C0104": (
            'Disallowed name "%s"',
            "disallowed-name",
            "Used when the name matches bad-names or bad-names-rgxs- (unauthorized names).",
            {
                "old_names": [
                    ("C0102", "blacklisted-name"),
                ]
            },
        ),
        "C0144": (
            '%s name "%s" contains a non-ASCII unicode character',
            "non-ascii-name",
            "Used when the name contains at least one non-ASCII unicode character.",
        ),
        "W0111": (
            "Name %s will become a keyword in Python %s",
            "assign-to-new-keyword",
            "Used when assignment will become invalid in future "
            "Python release due to introducing new keyword.",
        ),
    }

    options = (
        (
            "good-names",
            {
                "default": ("i", "j", "k", "ex", "Run", "_"),
                "type": "csv",
                "metavar": "<names>",
                "help": "Good variable names which should always be accepted,"
                " separated by a comma.",
            },
        ),
        (
            "good-names-rgxs",
            {
                "default": "",
                "type": "regexp_csv",
                "metavar": "<names>",
                "help": "Good variable names regexes, separated by a comma. If names match any regex,"
                " they will always be accepted",
            },
        ),
        (
            "bad-names",
            {
                "default": ("foo", "bar", "baz", "toto", "tutu", "tata"),
                "type": "csv",
                "metavar": "<names>",
                "help": "Bad variable names which should always be refused, "
                "separated by a comma.",
            },
        ),
        (
            "bad-names-rgxs",
            {
                "default": "",
                "type": "regexp_csv",
                "metavar": "<names>",
                "help": "Bad variable names regexes, separated by a comma. If names match any regex,"
                " they will always be refused",
            },
        ),
        (
            "name-group",
            {
                "default": (),
                "type": "csv",
                "metavar": "<name1:name2>",
                "help": (
                    "Colon-delimited sets of names that determine each"
                    " other's naming style when the name regexes"
                    " allow several styles."
                ),
            },
        ),
        (
            "include-naming-hint",
            {
                "default": False,
                "type": "yn",
                "metavar": "<y or n>",
                "help": "Include a hint for the correct naming format with invalid-name.",
            },
        ),
        (
            "property-classes",
            {
                "default": ("abc.abstractproperty",),
                "type": "csv",
                "metavar": "<decorator names>",
                "help": "List of decorators that produce properties, such as "
                "abc.abstractproperty. Add to this list to register "
                "other decorators that produce valid properties. "
                "These decorators are taken in consideration only for invalid-name.",
            },
        ),
    ) + _create_naming_options()

    KEYWORD_ONSET = {(3, 7): {"async", "await"}}

    def __init__(self, linter):
        super().__init__(linter)
        self._name_category = {}
        self._name_group = {}
        self._bad_names = {}
        self._name_regexps = {}
        self._name_hints = {}
        self._good_names_rgxs_compiled = []
        self._bad_names_rgxs_compiled = []
        self._non_ascii_rgx_compiled = re.compile("[^\u0000-\u007F]")

    def open(self):
        self.linter.stats.reset_bad_names()
        for group in self.config.name_group:
            for name_type in group.split(":"):
                self._name_group[name_type] = f"group_{group}"

        regexps, hints = self._create_naming_rules()
        self._name_regexps = regexps
        self._name_hints = hints
        self._good_names_rgxs_compiled = [
            re.compile(rgxp) for rgxp in self.config.good_names_rgxs
        ]
        self._bad_names_rgxs_compiled = [
            re.compile(rgxp) for rgxp in self.config.bad_names_rgxs
        ]

    def _create_naming_rules(self):
        regexps = {}
        hints = {}

        for name_type in KNOWN_NAME_TYPES:
            naming_style_option_name = f"{name_type}_naming_style"
            naming_style_name = getattr(self.config, naming_style_option_name)

            regexps[name_type] = NAMING_STYLES[naming_style_name].get_regex(name_type)

            custom_regex_setting_name = f"{name_type}_rgx"
            custom_regex = getattr(self.config, custom_regex_setting_name, None)
            if custom_regex is not None:
                regexps[name_type] = custom_regex

            if custom_regex is not None:
                hints[name_type] = f"{custom_regex.pattern!r} pattern"
            else:
                hints[name_type] = f"{naming_style_name} naming style"

        return regexps, hints

    @utils.check_messages("disallowed-name", "invalid-name", "non-ascii-name")
    def visit_module(self, node: nodes.Module) -> None:
        self._check_name("module", node.name.split(".")[-1], node)
        self._bad_names = {}

    def leave_module(self, _: nodes.Module) -> None:
        for all_groups in self._bad_names.values():
            if len(all_groups) < 2:
                continue
            groups = collections.defaultdict(list)
            min_warnings = sys.maxsize
            prevalent_group, _ = max(all_groups.items(), key=lambda item: len(item[1]))
            for group in all_groups.values():
                groups[len(group)].append(group)
                min_warnings = min(len(group), min_warnings)
            if len(groups[min_warnings]) > 1:
                by_line = sorted(
                    groups[min_warnings],
                    key=lambda group: min(warning[0].lineno for warning in group),
                )
                warnings = itertools.chain(*by_line[1:])
            else:
                warnings = groups[min_warnings][0]
            for args in warnings:
                self._raise_name_warning(prevalent_group, *args)

    @utils.check_messages(
        "disallowed-name", "invalid-name", "assign-to-new-keyword", "non-ascii-name"
    )
    def visit_classdef(self, node: nodes.ClassDef) -> None:
        self._check_assign_to_new_keyword_violation(node.name, node)
        self._check_name("class", node.name, node)
        for attr, anodes in node.instance_attrs.items():
            if not any(node.instance_attr_ancestors(attr)):
                self._check_name("attr", attr, anodes[0])

    @utils.check_messages(
        "disallowed-name", "invalid-name", "assign-to-new-keyword", "non-ascii-name"
    )
    def visit_functiondef(self, node: nodes.FunctionDef) -> None:
        # Do not emit any warnings if the method is just an implementation
        # of a base class method.
        self._check_assign_to_new_keyword_violation(node.name, node)
        confidence = interfaces.HIGH
        if node.is_method():
            if utils.overrides_a_method(node.parent.frame(), node.name):
                return
            confidence = (
                interfaces.INFERENCE
                if utils.has_known_bases(node.parent.frame())
                else interfaces.INFERENCE_FAILURE
            )

        self._check_name(
            _determine_function_name_type(node, config=self.config),
            node.name,
            node,
            confidence,
        )
        # Check argument names
        args = node.args.args
        if args is not None:
            self._recursive_check_names(args)

    visit_asyncfunctiondef = visit_functiondef

    @utils.check_messages("disallowed-name", "invalid-name", "non-ascii-name")
    def visit_global(self, node: nodes.Global) -> None:
        for name in node.names:
            self._check_name("const", name, node)

    @utils.check_messages(
        "disallowed-name", "invalid-name", "assign-to-new-keyword", "non-ascii-name"
    )
    def visit_assignname(self, node: nodes.AssignName) -> None:
        """check module level assigned names"""
        self._check_assign_to_new_keyword_violation(node.name, node)
        frame = node.frame()
        assign_type = node.assign_type()
        if isinstance(assign_type, nodes.Comprehension):
            self._check_name("inlinevar", node.name, node)
        elif isinstance(frame, nodes.Module):
            if isinstance(assign_type, nodes.Assign):
                if isinstance(utils.safe_infer(assign_type.value), nodes.ClassDef):
                    self._check_name("class", node.name, node)
                # Don't emit if the name redefines an import
                # in an ImportError except handler.
                elif not _redefines_import(node) and isinstance(
                    utils.safe_infer(assign_type.value), nodes.Const
                ):
                    self._check_name("const", node.name, node)
            elif isinstance(
                assign_type, nodes.AnnAssign
            ) and utils.is_assign_name_annotated_with(node, "Final"):
                self._check_name("const", node.name, node)
        elif isinstance(frame, nodes.FunctionDef):
            # global introduced variable aren't in the function locals
            if node.name in frame and node.name not in frame.argnames():
                if not _redefines_import(node):
                    self._check_name("variable", node.name, node)
        elif isinstance(frame, nodes.ClassDef):
            if not list(frame.local_attr_ancestors(node.name)):
                for ancestor in frame.ancestors():
                    if (
                        ancestor.name == "Enum"
                        and ancestor.root().name == "enum"
                        or utils.is_assign_name_annotated_with(node, "Final")
                    ):
                        self._check_name("class_const", node.name, node)
                        break
                else:
                    self._check_name("class_attribute", node.name, node)

    def _recursive_check_names(self, args):
        """check names in a possibly recursive list <arg>"""
        for arg in args:
            if isinstance(arg, nodes.AssignName):
                self._check_name("argument", arg.name, arg)
            else:
                self._recursive_check_names(arg.elts)

    def _find_name_group(self, node_type):
        return self._name_group.get(node_type, node_type)

    def _raise_name_warning(
        self,
        prevalent_group: Optional[str],
        node: nodes.NodeNG,
        node_type: str,
        name: str,
        confidence,
        warning: str = "invalid-name",
    ) -> None:
        type_label = HUMAN_READABLE_TYPES[node_type]
        hint = self._name_hints[node_type]
        if prevalent_group:
            # This happens in the multi naming match case. The expected
            # prevalent group needs to be spelled out to make the message
            # correct.
            hint = f"the `{prevalent_group}` group in the {hint}"
        if self.config.include_naming_hint:
            hint += f" ({self._name_regexps[node_type].pattern!r} pattern)"
        args = (
            (type_label.capitalize(), name, hint)
            if warning == "invalid-name"
            else (type_label.capitalize(), name)
        )

        self.add_message(warning, node=node, args=args, confidence=confidence)
        self.linter.stats.increase_bad_name(node_type, 1)

    def _name_allowed_by_regex(self, name: str) -> bool:
        return name in self.config.good_names or any(
            pattern.match(name) for pattern in self._good_names_rgxs_compiled
        )

    def _name_disallowed_by_regex(self, name: str) -> bool:
        return name in self.config.bad_names or any(
            pattern.match(name) for pattern in self._bad_names_rgxs_compiled
        )

    def _check_name(self, node_type, name, node, confidence=interfaces.HIGH):
        """check for a name using the type's regexp"""
        non_ascii_match = self._non_ascii_rgx_compiled.match(name)
        if non_ascii_match is not None:
            self._raise_name_warning(
                None, node, node_type, name, confidence, warning="non-ascii-name"
            )

        def _should_exempt_from_invalid_name(node):
            if node_type == "variable":
                inferred = utils.safe_infer(node)
                if isinstance(inferred, nodes.ClassDef):
                    return True
            return False

        if self._name_allowed_by_regex(name=name):
            return
        if self._name_disallowed_by_regex(name=name):
            self.linter.stats.increase_bad_name(node_type, 1)
            self.add_message("disallowed-name", node=node, args=name)
            return
        regexp = self._name_regexps[node_type]
        match = regexp.match(name)

        if _is_multi_naming_match(match, node_type, confidence):
            name_group = self._find_name_group(node_type)
            bad_name_group = self._bad_names.setdefault(name_group, {})
            warnings = bad_name_group.setdefault(match.lastgroup, [])
            warnings.append((node, node_type, name, confidence))

        if match is None and not _should_exempt_from_invalid_name(node):
            self._raise_name_warning(None, node, node_type, name, confidence)

    def _check_assign_to_new_keyword_violation(self, name, node):
        keyword_first_version = self._name_became_keyword_in_version(
            name, self.KEYWORD_ONSET
        )
        if keyword_first_version is not None:
            self.add_message(
                "assign-to-new-keyword",
                node=node,
                args=(name, keyword_first_version),
                confidence=interfaces.HIGH,
            )

    @staticmethod
    def _name_became_keyword_in_version(name, rules):
        for version, keywords in rules.items():
            if name in keywords and sys.version_info < version:
                return ".".join(str(v) for v in version)
        return None


class DocStringChecker(_BasicChecker):
    msgs = {
        "C0112": (
            "Empty %s docstring",
            "empty-docstring",
            "Used when a module, function, class or method has an empty "
            "docstring (it would be too easy ;).",
            {"old_names": [("W0132", "old-empty-docstring")]},
        ),
        "C0114": (
            "Missing module docstring",
            "missing-module-docstring",
            "Used when a module has no docstring."
            "Empty modules do not require a docstring.",
            {"old_names": [("C0111", "missing-docstring")]},
        ),
        "C0115": (
            "Missing class docstring",
            "missing-class-docstring",
            "Used when a class has no docstring."
            "Even an empty class must have a docstring.",
            {"old_names": [("C0111", "missing-docstring")]},
        ),
        "C0116": (
            "Missing function or method docstring",
            "missing-function-docstring",
            "Used when a function or method has no docstring."
            "Some special methods like __init__ do not require a "
            "docstring.",
            {"old_names": [("C0111", "missing-docstring")]},
        ),
    }
    options = (
        (
            "no-docstring-rgx",
            {
                "default": NO_REQUIRED_DOC_RGX,
                "type": "regexp",
                "metavar": "<regexp>",
                "help": "Regular expression which should only match "
                "function or class names that do not require a "
                "docstring.",
            },
        ),
        (
            "docstring-min-length",
            {
                "default": -1,
                "type": "int",
                "metavar": "<int>",
                "help": (
                    "Minimum line length for functions/classes that"
                    " require docstrings, shorter ones are exempt."
                ),
            },
        ),
    )

    def open(self):
        self.linter.stats.reset_undocumented()

    @utils.check_messages("missing-docstring", "empty-docstring")
    def visit_module(self, node: nodes.Module) -> None:
        self._check_docstring("module", node)

    @utils.check_messages("missing-docstring", "empty-docstring")
    def visit_classdef(self, node: nodes.ClassDef) -> None:
        if self.config.no_docstring_rgx.match(node.name) is None:
            self._check_docstring("class", node)

    @utils.check_messages("missing-docstring", "empty-docstring")
    def visit_functiondef(self, node: nodes.FunctionDef) -> None:
        if self.config.no_docstring_rgx.match(node.name) is None:
            ftype = "method" if node.is_method() else "function"
            if (
                is_property_setter(node)
                or is_property_deleter(node)
                or is_overload_stub(node)
            ):
                return

            if isinstance(node.parent.frame(), nodes.ClassDef):
                overridden = False
                confidence = (
                    interfaces.INFERENCE
                    if utils.has_known_bases(node.parent.frame())
                    else interfaces.INFERENCE_FAILURE
                )
                # check if node is from a method overridden by its ancestor
                for ancestor in node.parent.frame().ancestors():
                    if ancestor.qname() == "builtins.object":
                        continue
                    if node.name in ancestor and isinstance(
                        ancestor[node.name], nodes.FunctionDef
                    ):
                        overridden = True
                        break
                self._check_docstring(
                    ftype, node, report_missing=not overridden, confidence=confidence  # type: ignore[arg-type]
                )
            elif isinstance(node.parent.frame(), nodes.Module):
                self._check_docstring(ftype, node)  # type: ignore[arg-type]
            else:
                return

    visit_asyncfunctiondef = visit_functiondef

    def _check_docstring(
        self,
        node_type: Literal["class", "function", "method", "module"],
        node,
        report_missing=True,
        confidence=interfaces.HIGH,
    ):
        """check the node has a non empty docstring"""
        docstring = node.doc
        if docstring is None:
            docstring = _infer_dunder_doc_attribute(node)

        if docstring is None:
            if not report_missing:
                return
            lines = utils.get_node_last_lineno(node) - node.lineno

            if node_type == "module" and not lines:
                # If the module has no body, there's no reason
                # to require a docstring.
                return
            max_lines = self.config.docstring_min_length

            if node_type != "module" and max_lines > -1 and lines < max_lines:
                return
            if node_type == "class":
                self.linter.stats.undocumented["klass"] += 1
            else:
                self.linter.stats.undocumented[node_type] += 1
            if (
                node.body
                and isinstance(node.body[0], nodes.Expr)
                and isinstance(node.body[0].value, nodes.Call)
            ):
                # Most likely a string with a format call. Let's see.
                func = utils.safe_infer(node.body[0].value.func)
                if isinstance(func, astroid.BoundMethod) and isinstance(
                    func.bound, astroid.Instance
                ):
                    # Strings.
                    if func.bound.name in {"str", "unicode", "bytes"}:
                        return
            if node_type == "module":
                message = "missing-module-docstring"
            elif node_type == "class":
                message = "missing-class-docstring"
            else:
                message = "missing-function-docstring"
            self.add_message(message, node=node, confidence=confidence)
        elif not docstring.strip():
            if node_type == "class":
                self.linter.stats.undocumented["klass"] += 1
            else:
                self.linter.stats.undocumented[node_type] += 1
            self.add_message(
                "empty-docstring", node=node, args=(node_type,), confidence=confidence
            )


class PassChecker(_BasicChecker):
    """check if the pass statement is really necessary"""

    msgs = {
        "W0107": (
            "Unnecessary pass statement",
            "unnecessary-pass",
            'Used when a "pass" statement that can be avoided is encountered.',
        )
    }

    @utils.check_messages("unnecessary-pass")
    def visit_pass(self, node: nodes.Pass) -> None:
        if len(node.parent.child_sequence(node)) > 1 or (
            isinstance(node.parent, (nodes.ClassDef, nodes.FunctionDef))
            and (node.parent.doc is not None)
        ):
            self.add_message("unnecessary-pass", node=node)


def _is_one_arg_pos_call(call):
    """Is this a call with exactly 1 argument,
    where that argument is positional?
    """
    return isinstance(call, nodes.Call) and len(call.args) == 1 and not call.keywords


def _infer_dunder_doc_attribute(node):
    # Try to see if we have a `__doc__` attribute.
    try:
        docstring = node["__doc__"]
    except KeyError:
        return None

    docstring = utils.safe_infer(docstring)
    if not docstring:
        return None
    if not isinstance(docstring, nodes.Const):
        return None
    return docstring.value


class ComparisonChecker(_BasicChecker):
    """Checks for comparisons

    - singleton comparison: 'expr == True', 'expr == False' and 'expr == None'
    - yoda condition: 'const "comp" right' where comp can be '==', '!=', '<',
      '<=', '>' or '>=', and right can be a variable, an attribute, a method or
      a function
    """

    msgs = {
        "C0121": (
            "Comparison %s should be %s",
            "singleton-comparison",
            "Used when an expression is compared to singleton "
            "values like True, False or None.",
        ),
        "C0123": (
            "Use isinstance() rather than type() for a typecheck.",
            "unidiomatic-typecheck",
            "The idiomatic way to perform an explicit typecheck in "
            "Python is to use isinstance(x, Y) rather than "
            "type(x) == Y, type(x) is Y. Though there are unusual "
            "situations where these give different results.",
            {"old_names": [("W0154", "old-unidiomatic-typecheck")]},
        ),
        "R0123": (
            "Comparison to literal",
            "literal-comparison",
            "Used when comparing an object to a literal, which is usually "
            "what you do not want to do, since you can compare to a different "
            "literal than what was expected altogether.",
        ),
        "R0124": (
            "Redundant comparison - %s",
            "comparison-with-itself",
            "Used when something is compared against itself.",
        ),
        "W0143": (
            "Comparing against a callable, did you omit the parenthesis?",
            "comparison-with-callable",
            "This message is emitted when pylint detects that a comparison with a "
            "callable was made, which might suggest that some parenthesis were omitted, "
            "resulting in potential unwanted behaviour.",
        ),
        "W0177": (
            "Comparison %s should be %s",
            "nan-comparison",
            "Used when an expression is compared to NaN"
            "values like numpy.NaN and float('nan')",
        ),
    }

    def _check_singleton_comparison(
        self, left_value, right_value, root_node, checking_for_absence: bool = False
    ):
        """Check if == or != is being used to compare a singleton value"""
        singleton_values = (True, False, None)

        def _is_singleton_const(node) -> bool:
            return isinstance(node, nodes.Const) and any(
                node.value is value for value in singleton_values
            )

        if _is_singleton_const(left_value):
            singleton, other_value = left_value.value, right_value
        elif _is_singleton_const(right_value):
            singleton, other_value = right_value.value, left_value
        else:
            return

        singleton_comparison_example = {False: "'{} is {}'", True: "'{} is not {}'"}

        # True/False singletons have a special-cased message in case the user is
        # mistakenly using == or != to check for truthiness
        if singleton in {True, False}:
            suggestion_template = (
                "{} if checking for the singleton value {}, or {} if testing for {}"
            )
            truthiness_example = {False: "not {}", True: "{}"}
            truthiness_phrase = {True: "truthiness", False: "falsiness"}

            # Looks for comparisons like x == True or x != False
            checking_truthiness = singleton is not checking_for_absence

            suggestion = suggestion_template.format(
                singleton_comparison_example[checking_for_absence].format(
                    left_value.as_string(), right_value.as_string()
                ),
                singleton,
                (
                    "'bool({})'"
                    if not utils.is_test_condition(root_node) and checking_truthiness
                    else "'{}'"
                ).format(
                    truthiness_example[checking_truthiness].format(
                        other_value.as_string()
                    )
                ),
                truthiness_phrase[checking_truthiness],
            )
        else:
            suggestion = singleton_comparison_example[checking_for_absence].format(
                left_value.as_string(), right_value.as_string()
            )
        self.add_message(
            "singleton-comparison",
            node=root_node,
            args=(f"'{root_node.as_string()}'", suggestion),
        )

    def _check_nan_comparison(
        self, left_value, right_value, root_node, checking_for_absence: bool = False
    ):
        def _is_float_nan(node):
            try:
                if isinstance(node, nodes.Call) and len(node.args) == 1:
                    if (
                        node.args[0].value.lower() == "nan"
                        and node.inferred()[0].pytype() == "builtins.float"
                    ):
                        return True
                return False
            except AttributeError:
                return False

        def _is_numpy_nan(node):
            if isinstance(node, nodes.Attribute) and node.attrname == "NaN":
                if isinstance(node.expr, nodes.Name):
                    return node.expr.name in {"numpy", "nmp", "np"}
            return False

        def _is_nan(node) -> bool:
            return _is_float_nan(node) or _is_numpy_nan(node)

        nan_left = _is_nan(left_value)
        if not nan_left and not _is_nan(right_value):
            return

        absence_text = ""
        if checking_for_absence:
            absence_text = "not "
        if nan_left:
            suggestion = f"'{absence_text}math.isnan({right_value.as_string()})'"
        else:
            suggestion = f"'{absence_text}math.isnan({left_value.as_string()})'"
        self.add_message(
            "nan-comparison",
            node=root_node,
            args=(f"'{root_node.as_string()}'", suggestion),
        )

    def _check_literal_comparison(self, literal, node: nodes.Compare):
        """Check if we compare to a literal, which is usually what we do not want to do."""
        is_other_literal = isinstance(literal, (nodes.List, nodes.Dict, nodes.Set))
        is_const = False
        if isinstance(literal, nodes.Const):
            if isinstance(literal.value, bool) or literal.value is None:
                # Not interested in this values.
                return
            is_const = isinstance(literal.value, (bytes, str, int, float))

        if is_const or is_other_literal:
            self.add_message("literal-comparison", node=node)

    def _check_logical_tautology(self, node: nodes.Compare):
        """Check if identifier is compared against itself.
        :param node: Compare node
        :Example:
        val = 786
        if val == val:  # [comparison-with-itself]
            pass
        """
        left_operand = node.left
        right_operand = node.ops[0][1]
        operator = node.ops[0][0]
        if isinstance(left_operand, nodes.Const) and isinstance(
            right_operand, nodes.Const
        ):
            left_operand = left_operand.value
            right_operand = right_operand.value
        elif isinstance(left_operand, nodes.Name) and isinstance(
            right_operand, nodes.Name
        ):
            left_operand = left_operand.name
            right_operand = right_operand.name

        if left_operand == right_operand:
            suggestion = f"{left_operand} {operator} {right_operand}"
            self.add_message("comparison-with-itself", node=node, args=(suggestion,))

    def _check_callable_comparison(self, node):
        operator = node.ops[0][0]
        if operator not in COMPARISON_OPERATORS:
            return

        bare_callables = (nodes.FunctionDef, astroid.BoundMethod)
        left_operand, right_operand = node.left, node.ops[0][1]
        # this message should be emitted only when there is comparison of bare callable
        # with non bare callable.
        if (
            sum(
                1
                for operand in (left_operand, right_operand)
                if isinstance(utils.safe_infer(operand), bare_callables)
            )
            == 1
        ):
            self.add_message("comparison-with-callable", node=node)

    @utils.check_messages(
        "singleton-comparison",
        "unidiomatic-typecheck",
        "literal-comparison",
        "comparison-with-itself",
        "comparison-with-callable",
    )
    def visit_compare(self, node: nodes.Compare) -> None:
        self._check_callable_comparison(node)
        self._check_logical_tautology(node)
        self._check_unidiomatic_typecheck(node)
        # NOTE: this checker only works with binary comparisons like 'x == 42'
        # but not 'x == y == 42'
        if len(node.ops) != 1:
            return

        left = node.left
        operator, right = node.ops[0]

        if operator in {"==", "!="}:
            self._check_singleton_comparison(
                left, right, node, checking_for_absence=operator == "!="
            )

        if operator in {"==", "!=", "is", "is not"}:
            self._check_nan_comparison(
                left, right, node, checking_for_absence=operator in {"!=", "is not"}
            )
        if operator in {"is", "is not"}:
            self._check_literal_comparison(right, node)

    def _check_unidiomatic_typecheck(self, node):
        operator, right = node.ops[0]
        if operator in TYPECHECK_COMPARISON_OPERATORS:
            left = node.left
            if _is_one_arg_pos_call(left):
                self._check_type_x_is_y(node, left, operator, right)

    def _check_type_x_is_y(self, node, left, operator, right):
        """Check for expressions like type(x) == Y."""
        left_func = utils.safe_infer(left.func)
        if not (
            isinstance(left_func, nodes.ClassDef) and left_func.qname() == TYPE_QNAME
        ):
            return

        if operator in {"is", "is not"} and _is_one_arg_pos_call(right):
            right_func = utils.safe_infer(right.func)
            if (
                isinstance(right_func, nodes.ClassDef)
                and right_func.qname() == TYPE_QNAME
            ):
                # type(x) == type(a)
                right_arg = utils.safe_infer(right.args[0])
                if not isinstance(right_arg, LITERAL_NODE_TYPES):
                    # not e.g. type(x) == type([])
                    return
        self.add_message("unidiomatic-typecheck", node=node)


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(BasicErrorChecker(linter))
    linter.register_checker(BasicChecker(linter))
    linter.register_checker(NameChecker(linter))
    linter.register_checker(DocStringChecker(linter))
    linter.register_checker(PassChecker(linter))
    linter.register_checker(ComparisonChecker(linter))
