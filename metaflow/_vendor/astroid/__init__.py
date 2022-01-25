# Copyright (c) 2006-2013, 2015 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2014 Google, Inc.
# Copyright (c) 2014 Eevee (Alex Munroe) <amunroe@yelp.com>
# Copyright (c) 2015-2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2016 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2016 Moises Lopez <moylop260@vauxoo.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2019 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Python Abstract Syntax Tree New Generation

The aim of this module is to provide a common base representation of
python source code for projects such as pychecker, pyreverse,
pylint... Well, actually the development of this library is essentially
governed by pylint's needs.

It extends class defined in the python's _ast module with some
additional methods and attributes. Instance attributes are added by a
builder object, which can either generate extended ast (let's call
them astroid ;) by visiting an existent ast tree or by inspecting living
object. Methods are added by monkey patching ast classes.

Main modules are:

* nodes and scoped_nodes for more information about methods and
  attributes added to different node classes

* the manager contains a high level object to get astroid trees from
  source files and living objects. It maintains a cache of previously
  constructed tree for quick access

* builder contains the class responsible to build astroid trees
"""

from importlib import import_module
from pathlib import Path

# isort: off
# We have an isort: off on '__version__' because the packaging need to access
# the version before the dependencies are installed (in particular 'wrapt'
# that is imported in astroid.inference)
from metaflow._vendor.astroid.__pkginfo__ import __version__, version
from metaflow._vendor.astroid.nodes import node_classes, scoped_nodes

# isort: on

from metaflow._vendor.astroid import inference, raw_building
from metaflow._vendor.astroid.astroid_manager import MANAGER
from metaflow._vendor.astroid.bases import BaseInstance, BoundMethod, Instance, UnboundMethod
from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import extract_node, parse
from metaflow._vendor.astroid.const import Context, Del, Load, Store
from metaflow._vendor.astroid.exceptions import *
from metaflow._vendor.astroid.inference_tip import _inference_tip_cached, inference_tip
from metaflow._vendor.astroid.objects import ExceptionInstance

# isort: off
# It's impossible to import from astroid.nodes with a wildcard, because
# there is a cyclic import that prevent creating an __all__ in astroid/nodes
# and we need astroid/scoped_nodes and astroid/node_classes to work. So
# importing with a wildcard would clash with astroid/nodes/scoped_nodes
# and astroid/nodes/node_classes.
from metaflow._vendor.astroid.nodes import (  # pylint: disable=redefined-builtin (Ellipsis)
    CONST_CLS,
    AnnAssign,
    Arguments,
    Assert,
    Assign,
    AssignAttr,
    AssignName,
    AsyncFor,
    AsyncFunctionDef,
    AsyncWith,
    Attribute,
    AugAssign,
    Await,
    BinOp,
    BoolOp,
    Break,
    Call,
    ClassDef,
    Compare,
    Comprehension,
    ComprehensionScope,
    Const,
    Continue,
    Decorators,
    DelAttr,
    Delete,
    DelName,
    Dict,
    DictComp,
    DictUnpack,
    Ellipsis,
    EmptyNode,
    EvaluatedObject,
    ExceptHandler,
    Expr,
    ExtSlice,
    For,
    FormattedValue,
    FunctionDef,
    GeneratorExp,
    Global,
    If,
    IfExp,
    Import,
    ImportFrom,
    Index,
    JoinedStr,
    Keyword,
    Lambda,
    List,
    ListComp,
    Match,
    MatchAs,
    MatchCase,
    MatchClass,
    MatchMapping,
    MatchOr,
    MatchSequence,
    MatchSingleton,
    MatchStar,
    MatchValue,
    Module,
    Name,
    NamedExpr,
    NodeNG,
    Nonlocal,
    Pass,
    Raise,
    Return,
    Set,
    SetComp,
    Slice,
    Starred,
    Subscript,
    TryExcept,
    TryFinally,
    Tuple,
    UnaryOp,
    Unknown,
    While,
    With,
    Yield,
    YieldFrom,
    are_exclusive,
    builtin_lookup,
    unpack_infer,
    function_to_method,
)

# isort: on

from metaflow._vendor.astroid.util import Uninferable

# load brain plugins
ASTROID_INSTALL_DIRECTORY = Path(__file__).parent
BRAIN_MODULES_DIRECTORY = ASTROID_INSTALL_DIRECTORY / "brain"
for module in BRAIN_MODULES_DIRECTORY.iterdir():
    if module.suffix == ".py":
        import_module(f"astroid.brain.{module.stem}")
