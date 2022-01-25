# Copyright (c) 2009-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2013-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2013-2014 Google, Inc.
# Copyright (c) 2014 Alexander Presnyakov <flagist0@gmail.com>
# Copyright (c) 2014 Eevee (Alex Munroe) <amunroe@yelp.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2016-2017 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2016 Jared Garst <jgarst@users.noreply.github.com>
# Copyright (c) 2017 Hugo <hugovk@users.noreply.github.com>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2017 rr- <rr-@sakuya.pl>
# Copyright (c) 2018-2019 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2018 Tomas Gavenciak <gavento@ucw.cz>
# Copyright (c) 2018 Serhiy Storchaka <storchaka@gmail.com>
# Copyright (c) 2018 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2019-2021 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2019 Zbigniew Jędrzejewski-Szmek <zbyszek@in.waw.pl>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Federico Bond <federicobond@gmail.com>
# Copyright (c) 2021 hippo91 <guillaume.peillex@gmail.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""this module contains utilities for rebuilding an _ast tree in
order to get a single Astroid representation
"""

import sys
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from metaflow._vendor.astroid import nodes
from metaflow._vendor.astroid._ast import ParserModule, get_parser_module, parse_function_type_comment
from metaflow._vendor.astroid.const import PY38, PY38_PLUS, Context
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes import NodeNG

if sys.version_info >= (3, 8):
    from typing import Final
else:
    from metaflow._vendor.typing_extensions import Final

if TYPE_CHECKING:
    import ast


REDIRECT: Final[Dict[str, str]] = {
    "arguments": "Arguments",
    "comprehension": "Comprehension",
    "ListCompFor": "Comprehension",
    "GenExprFor": "Comprehension",
    "excepthandler": "ExceptHandler",
    "keyword": "Keyword",
    "match_case": "MatchCase",
}


T_Doc = TypeVar(
    "T_Doc",
    "ast.Module",
    "ast.ClassDef",
    Union["ast.FunctionDef", "ast.AsyncFunctionDef"],
)
T_Function = TypeVar("T_Function", nodes.FunctionDef, nodes.AsyncFunctionDef)
T_For = TypeVar("T_For", nodes.For, nodes.AsyncFor)
T_With = TypeVar("T_With", nodes.With, nodes.AsyncWith)


# noinspection PyMethodMayBeStatic
class TreeRebuilder:
    """Rebuilds the _ast tree to become an Astroid tree"""

    def __init__(
        self, manager: AstroidManager, parser_module: Optional[ParserModule] = None
    ):
        self._manager = manager
        self._global_names: List[Dict[str, List[nodes.Global]]] = []
        self._import_from_nodes: List[nodes.ImportFrom] = []
        self._delayed_assattr: List[nodes.AssignAttr] = []
        self._visit_meths: Dict[
            Type["ast.AST"], Callable[["ast.AST", NodeNG], NodeNG]
        ] = {}

        if parser_module is None:
            self._parser_module = get_parser_module()
        else:
            self._parser_module = parser_module
        self._module = self._parser_module.module

    def _get_doc(self, node: T_Doc) -> Tuple[T_Doc, Optional[str]]:
        try:
            if node.body and isinstance(node.body[0], self._module.Expr):
                first_value = node.body[0].value
                if isinstance(first_value, self._module.Str) or (
                    PY38_PLUS
                    and isinstance(first_value, self._module.Constant)
                    and isinstance(first_value.value, str)
                ):
                    doc = first_value.value if PY38_PLUS else first_value.s
                    node.body = node.body[1:]
                    return node, doc
        except IndexError:
            pass  # ast built from scratch
        return node, None

    def _get_context(
        self,
        node: Union[
            "ast.Attribute",
            "ast.List",
            "ast.Name",
            "ast.Subscript",
            "ast.Starred",
            "ast.Tuple",
        ],
    ) -> Context:
        return self._parser_module.context_classes.get(type(node.ctx), Context.Load)

    def visit_module(
        self, node: "ast.Module", modname: str, modpath: str, package: bool
    ) -> nodes.Module:
        """visit a Module node by returning a fresh instance of it

        Note: Method not called by 'visit'
        """
        node, doc = self._get_doc(node)
        newnode = nodes.Module(
            name=modname,
            doc=doc,
            file=modpath,
            path=[modpath],
            package=package,
            parent=None,
        )
        newnode.postinit([self.visit(child, newnode) for child in node.body])
        return newnode

    if sys.version_info >= (3, 10):

        @overload
        def visit(self, node: "ast.arg", parent: NodeNG) -> nodes.AssignName:
            ...

        @overload
        def visit(self, node: "ast.arguments", parent: NodeNG) -> nodes.Arguments:
            ...

        @overload
        def visit(self, node: "ast.Assert", parent: NodeNG) -> nodes.Assert:
            ...

        @overload
        def visit(
            self, node: "ast.AsyncFunctionDef", parent: NodeNG
        ) -> nodes.AsyncFunctionDef:
            ...

        @overload
        def visit(self, node: "ast.AsyncFor", parent: NodeNG) -> nodes.AsyncFor:
            ...

        @overload
        def visit(self, node: "ast.Await", parent: NodeNG) -> nodes.Await:
            ...

        @overload
        def visit(self, node: "ast.AsyncWith", parent: NodeNG) -> nodes.AsyncWith:
            ...

        @overload
        def visit(self, node: "ast.Assign", parent: NodeNG) -> nodes.Assign:
            ...

        @overload
        def visit(self, node: "ast.AnnAssign", parent: NodeNG) -> nodes.AnnAssign:
            ...

        @overload
        def visit(self, node: "ast.AugAssign", parent: NodeNG) -> nodes.AugAssign:
            ...

        @overload
        def visit(self, node: "ast.BinOp", parent: NodeNG) -> nodes.BinOp:
            ...

        @overload
        def visit(self, node: "ast.BoolOp", parent: NodeNG) -> nodes.BoolOp:
            ...

        @overload
        def visit(self, node: "ast.Break", parent: NodeNG) -> nodes.Break:
            ...

        @overload
        def visit(self, node: "ast.Call", parent: NodeNG) -> nodes.Call:
            ...

        @overload
        def visit(self, node: "ast.ClassDef", parent: NodeNG) -> nodes.ClassDef:
            ...

        @overload
        def visit(self, node: "ast.Continue", parent: NodeNG) -> nodes.Continue:
            ...

        @overload
        def visit(self, node: "ast.Compare", parent: NodeNG) -> nodes.Compare:
            ...

        @overload
        def visit(
            self, node: "ast.comprehension", parent: NodeNG
        ) -> nodes.Comprehension:
            ...

        @overload
        def visit(self, node: "ast.Delete", parent: NodeNG) -> nodes.Delete:
            ...

        @overload
        def visit(self, node: "ast.Dict", parent: NodeNG) -> nodes.Dict:
            ...

        @overload
        def visit(self, node: "ast.DictComp", parent: NodeNG) -> nodes.DictComp:
            ...

        @overload
        def visit(self, node: "ast.Expr", parent: NodeNG) -> nodes.Expr:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.Ellipsis", parent: NodeNG) -> nodes.Const:
            ...

        @overload
        def visit(
            self, node: "ast.ExceptHandler", parent: NodeNG
        ) -> nodes.ExceptHandler:
            ...

        # Not used in Python 3.9+
        @overload
        def visit(self, node: "ast.ExtSlice", parent: nodes.Subscript) -> nodes.Tuple:
            ...

        @overload
        def visit(self, node: "ast.For", parent: NodeNG) -> nodes.For:
            ...

        @overload
        def visit(self, node: "ast.ImportFrom", parent: NodeNG) -> nodes.ImportFrom:
            ...

        @overload
        def visit(self, node: "ast.FunctionDef", parent: NodeNG) -> nodes.FunctionDef:
            ...

        @overload
        def visit(self, node: "ast.GeneratorExp", parent: NodeNG) -> nodes.GeneratorExp:
            ...

        @overload
        def visit(self, node: "ast.Attribute", parent: NodeNG) -> nodes.Attribute:
            ...

        @overload
        def visit(self, node: "ast.Global", parent: NodeNG) -> nodes.Global:
            ...

        @overload
        def visit(self, node: "ast.If", parent: NodeNG) -> nodes.If:
            ...

        @overload
        def visit(self, node: "ast.IfExp", parent: NodeNG) -> nodes.IfExp:
            ...

        @overload
        def visit(self, node: "ast.Import", parent: NodeNG) -> nodes.Import:
            ...

        @overload
        def visit(self, node: "ast.JoinedStr", parent: NodeNG) -> nodes.JoinedStr:
            ...

        @overload
        def visit(
            self, node: "ast.FormattedValue", parent: NodeNG
        ) -> nodes.FormattedValue:
            ...

        @overload
        def visit(self, node: "ast.NamedExpr", parent: NodeNG) -> nodes.NamedExpr:
            ...

        # Not used in Python 3.9+
        @overload
        def visit(self, node: "ast.Index", parent: nodes.Subscript) -> NodeNG:
            ...

        @overload
        def visit(self, node: "ast.keyword", parent: NodeNG) -> nodes.Keyword:
            ...

        @overload
        def visit(self, node: "ast.Lambda", parent: NodeNG) -> nodes.Lambda:
            ...

        @overload
        def visit(self, node: "ast.List", parent: NodeNG) -> nodes.List:
            ...

        @overload
        def visit(self, node: "ast.ListComp", parent: NodeNG) -> nodes.ListComp:
            ...

        @overload
        def visit(
            self, node: "ast.Name", parent: NodeNG
        ) -> Union[nodes.Name, nodes.Const, nodes.AssignName, nodes.DelName]:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.NameConstant", parent: NodeNG) -> nodes.Const:
            ...

        @overload
        def visit(self, node: "ast.Nonlocal", parent: NodeNG) -> nodes.Nonlocal:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.Str", parent: NodeNG) -> nodes.Const:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.Bytes", parent: NodeNG) -> nodes.Const:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.Num", parent: NodeNG) -> nodes.Const:
            ...

        @overload
        def visit(self, node: "ast.Constant", parent: NodeNG) -> nodes.Const:
            ...

        @overload
        def visit(self, node: "ast.Pass", parent: NodeNG) -> nodes.Pass:
            ...

        @overload
        def visit(self, node: "ast.Raise", parent: NodeNG) -> nodes.Raise:
            ...

        @overload
        def visit(self, node: "ast.Return", parent: NodeNG) -> nodes.Return:
            ...

        @overload
        def visit(self, node: "ast.Set", parent: NodeNG) -> nodes.Set:
            ...

        @overload
        def visit(self, node: "ast.SetComp", parent: NodeNG) -> nodes.SetComp:
            ...

        @overload
        def visit(self, node: "ast.Slice", parent: nodes.Subscript) -> nodes.Slice:
            ...

        @overload
        def visit(self, node: "ast.Subscript", parent: NodeNG) -> nodes.Subscript:
            ...

        @overload
        def visit(self, node: "ast.Starred", parent: NodeNG) -> nodes.Starred:
            ...

        @overload
        def visit(
            self, node: "ast.Try", parent: NodeNG
        ) -> Union[nodes.TryExcept, nodes.TryFinally]:
            ...

        @overload
        def visit(self, node: "ast.Tuple", parent: NodeNG) -> nodes.Tuple:
            ...

        @overload
        def visit(self, node: "ast.UnaryOp", parent: NodeNG) -> nodes.UnaryOp:
            ...

        @overload
        def visit(self, node: "ast.While", parent: NodeNG) -> nodes.While:
            ...

        @overload
        def visit(self, node: "ast.With", parent: NodeNG) -> nodes.With:
            ...

        @overload
        def visit(self, node: "ast.Yield", parent: NodeNG) -> nodes.Yield:
            ...

        @overload
        def visit(self, node: "ast.YieldFrom", parent: NodeNG) -> nodes.YieldFrom:
            ...

        @overload
        def visit(self, node: "ast.Match", parent: NodeNG) -> nodes.Match:
            ...

        @overload
        def visit(self, node: "ast.match_case", parent: NodeNG) -> nodes.MatchCase:
            ...

        @overload
        def visit(self, node: "ast.MatchValue", parent: NodeNG) -> nodes.MatchValue:
            ...

        @overload
        def visit(
            self, node: "ast.MatchSingleton", parent: NodeNG
        ) -> nodes.MatchSingleton:
            ...

        @overload
        def visit(
            self, node: "ast.MatchSequence", parent: NodeNG
        ) -> nodes.MatchSequence:
            ...

        @overload
        def visit(self, node: "ast.MatchMapping", parent: NodeNG) -> nodes.MatchMapping:
            ...

        @overload
        def visit(self, node: "ast.MatchClass", parent: NodeNG) -> nodes.MatchClass:
            ...

        @overload
        def visit(self, node: "ast.MatchStar", parent: NodeNG) -> nodes.MatchStar:
            ...

        @overload
        def visit(self, node: "ast.MatchAs", parent: NodeNG) -> nodes.MatchAs:
            ...

        @overload
        def visit(self, node: "ast.MatchOr", parent: NodeNG) -> nodes.MatchOr:
            ...

        @overload
        def visit(self, node: "ast.pattern", parent: NodeNG) -> nodes.Pattern:
            ...

        @overload
        def visit(self, node: "ast.AST", parent: NodeNG) -> NodeNG:
            ...

        @overload
        def visit(self, node: None, parent: NodeNG) -> None:
            ...

        def visit(self, node: Optional["ast.AST"], parent: NodeNG) -> Optional[NodeNG]:
            if node is None:
                return None
            cls = node.__class__
            if cls in self._visit_meths:
                visit_method = self._visit_meths[cls]
            else:
                cls_name = cls.__name__
                visit_name = "visit_" + REDIRECT.get(cls_name, cls_name).lower()
                visit_method = getattr(self, visit_name)
                self._visit_meths[cls] = visit_method
            return visit_method(node, parent)

    else:

        @overload
        def visit(self, node: "ast.arg", parent: NodeNG) -> nodes.AssignName:
            ...

        @overload
        def visit(self, node: "ast.arguments", parent: NodeNG) -> nodes.Arguments:
            ...

        @overload
        def visit(self, node: "ast.Assert", parent: NodeNG) -> nodes.Assert:
            ...

        @overload
        def visit(
            self, node: "ast.AsyncFunctionDef", parent: NodeNG
        ) -> nodes.AsyncFunctionDef:
            ...

        @overload
        def visit(self, node: "ast.AsyncFor", parent: NodeNG) -> nodes.AsyncFor:
            ...

        @overload
        def visit(self, node: "ast.Await", parent: NodeNG) -> nodes.Await:
            ...

        @overload
        def visit(self, node: "ast.AsyncWith", parent: NodeNG) -> nodes.AsyncWith:
            ...

        @overload
        def visit(self, node: "ast.Assign", parent: NodeNG) -> nodes.Assign:
            ...

        @overload
        def visit(self, node: "ast.AnnAssign", parent: NodeNG) -> nodes.AnnAssign:
            ...

        @overload
        def visit(self, node: "ast.AugAssign", parent: NodeNG) -> nodes.AugAssign:
            ...

        @overload
        def visit(self, node: "ast.BinOp", parent: NodeNG) -> nodes.BinOp:
            ...

        @overload
        def visit(self, node: "ast.BoolOp", parent: NodeNG) -> nodes.BoolOp:
            ...

        @overload
        def visit(self, node: "ast.Break", parent: NodeNG) -> nodes.Break:
            ...

        @overload
        def visit(self, node: "ast.Call", parent: NodeNG) -> nodes.Call:
            ...

        @overload
        def visit(self, node: "ast.ClassDef", parent: NodeNG) -> nodes.ClassDef:
            ...

        @overload
        def visit(self, node: "ast.Continue", parent: NodeNG) -> nodes.Continue:
            ...

        @overload
        def visit(self, node: "ast.Compare", parent: NodeNG) -> nodes.Compare:
            ...

        @overload
        def visit(
            self, node: "ast.comprehension", parent: NodeNG
        ) -> nodes.Comprehension:
            ...

        @overload
        def visit(self, node: "ast.Delete", parent: NodeNG) -> nodes.Delete:
            ...

        @overload
        def visit(self, node: "ast.Dict", parent: NodeNG) -> nodes.Dict:
            ...

        @overload
        def visit(self, node: "ast.DictComp", parent: NodeNG) -> nodes.DictComp:
            ...

        @overload
        def visit(self, node: "ast.Expr", parent: NodeNG) -> nodes.Expr:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.Ellipsis", parent: NodeNG) -> nodes.Const:
            ...

        @overload
        def visit(
            self, node: "ast.ExceptHandler", parent: NodeNG
        ) -> nodes.ExceptHandler:
            ...

        # Not used in Python 3.9+
        @overload
        def visit(self, node: "ast.ExtSlice", parent: nodes.Subscript) -> nodes.Tuple:
            ...

        @overload
        def visit(self, node: "ast.For", parent: NodeNG) -> nodes.For:
            ...

        @overload
        def visit(self, node: "ast.ImportFrom", parent: NodeNG) -> nodes.ImportFrom:
            ...

        @overload
        def visit(self, node: "ast.FunctionDef", parent: NodeNG) -> nodes.FunctionDef:
            ...

        @overload
        def visit(self, node: "ast.GeneratorExp", parent: NodeNG) -> nodes.GeneratorExp:
            ...

        @overload
        def visit(self, node: "ast.Attribute", parent: NodeNG) -> nodes.Attribute:
            ...

        @overload
        def visit(self, node: "ast.Global", parent: NodeNG) -> nodes.Global:
            ...

        @overload
        def visit(self, node: "ast.If", parent: NodeNG) -> nodes.If:
            ...

        @overload
        def visit(self, node: "ast.IfExp", parent: NodeNG) -> nodes.IfExp:
            ...

        @overload
        def visit(self, node: "ast.Import", parent: NodeNG) -> nodes.Import:
            ...

        @overload
        def visit(self, node: "ast.JoinedStr", parent: NodeNG) -> nodes.JoinedStr:
            ...

        @overload
        def visit(
            self, node: "ast.FormattedValue", parent: NodeNG
        ) -> nodes.FormattedValue:
            ...

        @overload
        def visit(self, node: "ast.NamedExpr", parent: NodeNG) -> nodes.NamedExpr:
            ...

        # Not used in Python 3.9+
        @overload
        def visit(self, node: "ast.Index", parent: nodes.Subscript) -> NodeNG:
            ...

        @overload
        def visit(self, node: "ast.keyword", parent: NodeNG) -> nodes.Keyword:
            ...

        @overload
        def visit(self, node: "ast.Lambda", parent: NodeNG) -> nodes.Lambda:
            ...

        @overload
        def visit(self, node: "ast.List", parent: NodeNG) -> nodes.List:
            ...

        @overload
        def visit(self, node: "ast.ListComp", parent: NodeNG) -> nodes.ListComp:
            ...

        @overload
        def visit(
            self, node: "ast.Name", parent: NodeNG
        ) -> Union[nodes.Name, nodes.Const, nodes.AssignName, nodes.DelName]:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.NameConstant", parent: NodeNG) -> nodes.Const:
            ...

        @overload
        def visit(self, node: "ast.Nonlocal", parent: NodeNG) -> nodes.Nonlocal:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.Str", parent: NodeNG) -> nodes.Const:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.Bytes", parent: NodeNG) -> nodes.Const:
            ...

        # Not used in Python 3.8+
        @overload
        def visit(self, node: "ast.Num", parent: NodeNG) -> nodes.Const:
            ...

        @overload
        def visit(self, node: "ast.Constant", parent: NodeNG) -> nodes.Const:
            ...

        @overload
        def visit(self, node: "ast.Pass", parent: NodeNG) -> nodes.Pass:
            ...

        @overload
        def visit(self, node: "ast.Raise", parent: NodeNG) -> nodes.Raise:
            ...

        @overload
        def visit(self, node: "ast.Return", parent: NodeNG) -> nodes.Return:
            ...

        @overload
        def visit(self, node: "ast.Set", parent: NodeNG) -> nodes.Set:
            ...

        @overload
        def visit(self, node: "ast.SetComp", parent: NodeNG) -> nodes.SetComp:
            ...

        @overload
        def visit(self, node: "ast.Slice", parent: nodes.Subscript) -> nodes.Slice:
            ...

        @overload
        def visit(self, node: "ast.Subscript", parent: NodeNG) -> nodes.Subscript:
            ...

        @overload
        def visit(self, node: "ast.Starred", parent: NodeNG) -> nodes.Starred:
            ...

        @overload
        def visit(
            self, node: "ast.Try", parent: NodeNG
        ) -> Union[nodes.TryExcept, nodes.TryFinally]:
            ...

        @overload
        def visit(self, node: "ast.Tuple", parent: NodeNG) -> nodes.Tuple:
            ...

        @overload
        def visit(self, node: "ast.UnaryOp", parent: NodeNG) -> nodes.UnaryOp:
            ...

        @overload
        def visit(self, node: "ast.While", parent: NodeNG) -> nodes.While:
            ...

        @overload
        def visit(self, node: "ast.With", parent: NodeNG) -> nodes.With:
            ...

        @overload
        def visit(self, node: "ast.Yield", parent: NodeNG) -> nodes.Yield:
            ...

        @overload
        def visit(self, node: "ast.YieldFrom", parent: NodeNG) -> nodes.YieldFrom:
            ...

        @overload
        def visit(self, node: "ast.AST", parent: NodeNG) -> NodeNG:
            ...

        @overload
        def visit(self, node: None, parent: NodeNG) -> None:
            ...

        def visit(self, node: Optional["ast.AST"], parent: NodeNG) -> Optional[NodeNG]:
            if node is None:
                return None
            cls = node.__class__
            if cls in self._visit_meths:
                visit_method = self._visit_meths[cls]
            else:
                cls_name = cls.__name__
                visit_name = "visit_" + REDIRECT.get(cls_name, cls_name).lower()
                visit_method = getattr(self, visit_name)
                self._visit_meths[cls] = visit_method
            return visit_method(node, parent)

    def _save_assignment(self, node: Union[nodes.AssignName, nodes.DelName]) -> None:
        """save assignment situation since node.parent is not available yet"""
        if self._global_names and node.name in self._global_names[-1]:
            node.root().set_local(node.name, node)
        else:
            assert node.parent
            node.parent.set_local(node.name, node)

    def visit_arg(self, node: "ast.arg", parent: NodeNG) -> nodes.AssignName:
        """visit an arg node by returning a fresh AssName instance"""
        return self.visit_assignname(node, parent, node.arg)

    def visit_arguments(self, node: "ast.arguments", parent: NodeNG) -> nodes.Arguments:
        """visit an Arguments node by returning a fresh instance of it"""
        vararg: Optional[str] = None
        kwarg: Optional[str] = None
        newnode = nodes.Arguments(
            node.vararg.arg if node.vararg else None,
            node.kwarg.arg if node.kwarg else None,
            parent,
        )
        args = [self.visit(child, newnode) for child in node.args]
        defaults = [self.visit(child, newnode) for child in node.defaults]
        varargannotation: Optional[NodeNG] = None
        kwargannotation: Optional[NodeNG] = None
        posonlyargs: List[nodes.AssignName] = []
        if node.vararg:
            vararg = node.vararg.arg
            varargannotation = self.visit(node.vararg.annotation, newnode)
        if node.kwarg:
            kwarg = node.kwarg.arg
            kwargannotation = self.visit(node.kwarg.annotation, newnode)

        if PY38:
            # In Python 3.8 'end_lineno' and 'end_col_offset'
            # for 'kwonlyargs' don't include the annotation.
            for arg in node.kwonlyargs:
                if arg.annotation is not None:
                    arg.end_lineno = arg.annotation.end_lineno
                    arg.end_col_offset = arg.annotation.end_col_offset

        kwonlyargs = [self.visit(child, newnode) for child in node.kwonlyargs]
        kw_defaults = [self.visit(child, newnode) for child in node.kw_defaults]
        annotations = [self.visit(arg.annotation, newnode) for arg in node.args]
        kwonlyargs_annotations = [
            self.visit(arg.annotation, newnode) for arg in node.kwonlyargs
        ]

        posonlyargs_annotations: List[Optional[NodeNG]] = []
        if PY38_PLUS:
            posonlyargs = [self.visit(child, newnode) for child in node.posonlyargs]
            posonlyargs_annotations = [
                self.visit(arg.annotation, newnode) for arg in node.posonlyargs
            ]
        type_comment_args = [
            self.check_type_comment(child, parent=newnode) for child in node.args
        ]
        type_comment_kwonlyargs = [
            self.check_type_comment(child, parent=newnode) for child in node.kwonlyargs
        ]
        type_comment_posonlyargs: List[Optional[NodeNG]] = []
        if PY38_PLUS:
            type_comment_posonlyargs = [
                self.check_type_comment(child, parent=newnode)
                for child in node.posonlyargs
            ]

        newnode.postinit(
            args=args,
            defaults=defaults,
            kwonlyargs=kwonlyargs,
            posonlyargs=posonlyargs,
            kw_defaults=kw_defaults,
            annotations=annotations,
            kwonlyargs_annotations=kwonlyargs_annotations,
            posonlyargs_annotations=posonlyargs_annotations,
            varargannotation=varargannotation,
            kwargannotation=kwargannotation,
            type_comment_args=type_comment_args,
            type_comment_kwonlyargs=type_comment_kwonlyargs,
            type_comment_posonlyargs=type_comment_posonlyargs,
        )
        # save argument names in locals:
        assert newnode.parent
        if vararg:
            newnode.parent.set_local(vararg, newnode)
        if kwarg:
            newnode.parent.set_local(kwarg, newnode)
        return newnode

    def visit_assert(self, node: "ast.Assert", parent: NodeNG) -> nodes.Assert:
        """visit a Assert node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Assert(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Assert(node.lineno, node.col_offset, parent)
        msg: Optional[NodeNG] = None
        if node.msg:
            msg = self.visit(node.msg, newnode)
        newnode.postinit(self.visit(node.test, newnode), msg)
        return newnode

    def check_type_comment(
        self,
        node: Union[
            "ast.Assign",
            "ast.arg",
            "ast.For",
            "ast.AsyncFor",
            "ast.With",
            "ast.AsyncWith",
        ],
        parent: Union[
            nodes.Assign,
            nodes.Arguments,
            nodes.For,
            nodes.AsyncFor,
            nodes.With,
            nodes.AsyncWith,
        ],
    ) -> Optional[NodeNG]:
        type_comment = getattr(node, "type_comment", None)  # Added in Python 3.8
        if not type_comment:
            return None

        try:
            type_comment_ast = self._parser_module.parse(type_comment)
        except SyntaxError:
            # Invalid type comment, just skip it.
            return None

        type_object = self.visit(type_comment_ast.body[0], parent=parent)
        if not isinstance(type_object, nodes.Expr):
            return None

        return type_object.value

    def check_function_type_comment(
        self, node: Union["ast.FunctionDef", "ast.AsyncFunctionDef"], parent: NodeNG
    ) -> Optional[Tuple[Optional[NodeNG], List[NodeNG]]]:
        type_comment = getattr(node, "type_comment", None)  # Added in Python 3.8
        if not type_comment:
            return None

        try:
            type_comment_ast = parse_function_type_comment(type_comment)
        except SyntaxError:
            # Invalid type comment, just skip it.
            return None

        if not type_comment_ast:
            return None

        returns: Optional[NodeNG] = None
        argtypes: List[NodeNG] = [
            self.visit(elem, parent) for elem in (type_comment_ast.argtypes or [])
        ]
        if type_comment_ast.returns:
            returns = self.visit(type_comment_ast.returns, parent)

        return returns, argtypes

    def visit_asyncfunctiondef(
        self, node: "ast.AsyncFunctionDef", parent: NodeNG
    ) -> nodes.AsyncFunctionDef:
        return self._visit_functiondef(nodes.AsyncFunctionDef, node, parent)

    def visit_asyncfor(self, node: "ast.AsyncFor", parent: NodeNG) -> nodes.AsyncFor:
        return self._visit_for(nodes.AsyncFor, node, parent)

    def visit_await(self, node: "ast.Await", parent: NodeNG) -> nodes.Await:
        if sys.version_info >= (3, 8):
            newnode = nodes.Await(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Await(node.lineno, node.col_offset, parent)
        newnode.postinit(value=self.visit(node.value, newnode))
        return newnode

    def visit_asyncwith(self, node: "ast.AsyncWith", parent: NodeNG) -> nodes.AsyncWith:
        return self._visit_with(nodes.AsyncWith, node, parent)

    def visit_assign(self, node: "ast.Assign", parent: NodeNG) -> nodes.Assign:
        """visit a Assign node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Assign(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Assign(node.lineno, node.col_offset, parent)
        type_annotation = self.check_type_comment(node, parent=newnode)
        newnode.postinit(
            targets=[self.visit(child, newnode) for child in node.targets],
            value=self.visit(node.value, newnode),
            type_annotation=type_annotation,
        )
        return newnode

    def visit_annassign(self, node: "ast.AnnAssign", parent: NodeNG) -> nodes.AnnAssign:
        """visit an AnnAssign node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.AnnAssign(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.AnnAssign(node.lineno, node.col_offset, parent)
        newnode.postinit(
            target=self.visit(node.target, newnode),
            annotation=self.visit(node.annotation, newnode),
            simple=node.simple,
            value=self.visit(node.value, newnode),
        )
        return newnode

    @overload
    def visit_assignname(
        self, node: "ast.AST", parent: NodeNG, node_name: str
    ) -> nodes.AssignName:
        ...

    @overload
    def visit_assignname(
        self, node: "ast.AST", parent: NodeNG, node_name: None
    ) -> None:
        ...

    def visit_assignname(
        self, node: "ast.AST", parent: NodeNG, node_name: Optional[str]
    ) -> Optional[nodes.AssignName]:
        """visit a node and return a AssignName node

        Note: Method not called by 'visit'
        """
        if node_name is None:
            return None
        if sys.version_info >= (3, 8):
            newnode = nodes.AssignName(
                name=node_name,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.AssignName(
                node_name,
                node.lineno,
                node.col_offset,
                parent,
            )
        self._save_assignment(newnode)
        return newnode

    def visit_augassign(self, node: "ast.AugAssign", parent: NodeNG) -> nodes.AugAssign:
        """visit a AugAssign node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.AugAssign(
                op=self._parser_module.bin_op_classes[type(node.op)] + "=",
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.AugAssign(
                self._parser_module.bin_op_classes[type(node.op)] + "=",
                node.lineno,
                node.col_offset,
                parent,
            )
        newnode.postinit(
            self.visit(node.target, newnode), self.visit(node.value, newnode)
        )
        return newnode

    def visit_binop(self, node: "ast.BinOp", parent: NodeNG) -> nodes.BinOp:
        """visit a BinOp node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.BinOp(
                op=self._parser_module.bin_op_classes[type(node.op)],
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.BinOp(
                self._parser_module.bin_op_classes[type(node.op)],
                node.lineno,
                node.col_offset,
                parent,
            )
        newnode.postinit(
            self.visit(node.left, newnode), self.visit(node.right, newnode)
        )
        return newnode

    def visit_boolop(self, node: "ast.BoolOp", parent: NodeNG) -> nodes.BoolOp:
        """visit a BoolOp node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.BoolOp(
                op=self._parser_module.bool_op_classes[type(node.op)],
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.BoolOp(
                self._parser_module.bool_op_classes[type(node.op)],
                node.lineno,
                node.col_offset,
                parent,
            )
        newnode.postinit([self.visit(child, newnode) for child in node.values])
        return newnode

    def visit_break(self, node: "ast.Break", parent: NodeNG) -> nodes.Break:
        """visit a Break node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            return nodes.Break(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        return nodes.Break(node.lineno, node.col_offset, parent)

    def visit_call(self, node: "ast.Call", parent: NodeNG) -> nodes.Call:
        """visit a CallFunc node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Call(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Call(node.lineno, node.col_offset, parent)
        newnode.postinit(
            func=self.visit(node.func, newnode),
            args=[self.visit(child, newnode) for child in node.args],
            keywords=[self.visit(child, newnode) for child in node.keywords],
        )
        return newnode

    def visit_classdef(
        self, node: "ast.ClassDef", parent: NodeNG, newstyle: bool = True
    ) -> nodes.ClassDef:
        """visit a ClassDef node to become astroid"""
        node, doc = self._get_doc(node)
        if sys.version_info >= (3, 8):
            newnode = nodes.ClassDef(
                name=node.name,
                doc=doc,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.ClassDef(
                node.name, doc, node.lineno, node.col_offset, parent
            )
        metaclass = None
        for keyword in node.keywords:
            if keyword.arg == "metaclass":
                metaclass = self.visit(keyword, newnode).value
                break
        decorators = self.visit_decorators(node, newnode)
        newnode.postinit(
            [self.visit(child, newnode) for child in node.bases],
            [self.visit(child, newnode) for child in node.body],
            decorators,
            newstyle,
            metaclass,
            [
                self.visit(kwd, newnode)
                for kwd in node.keywords
                if kwd.arg != "metaclass"
            ],
        )
        return newnode

    def visit_continue(self, node: "ast.Continue", parent: NodeNG) -> nodes.Continue:
        """visit a Continue node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            return nodes.Continue(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        return nodes.Continue(node.lineno, node.col_offset, parent)

    def visit_compare(self, node: "ast.Compare", parent: NodeNG) -> nodes.Compare:
        """visit a Compare node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Compare(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Compare(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.left, newnode),
            [
                (
                    self._parser_module.cmp_op_classes[op.__class__],
                    self.visit(expr, newnode),
                )
                for (op, expr) in zip(node.ops, node.comparators)
            ],
        )
        return newnode

    def visit_comprehension(
        self, node: "ast.comprehension", parent: NodeNG
    ) -> nodes.Comprehension:
        """visit a Comprehension node by returning a fresh instance of it"""
        newnode = nodes.Comprehension(parent)
        newnode.postinit(
            self.visit(node.target, newnode),
            self.visit(node.iter, newnode),
            [self.visit(child, newnode) for child in node.ifs],
            bool(node.is_async),
        )
        return newnode

    def visit_decorators(
        self,
        node: Union["ast.ClassDef", "ast.FunctionDef", "ast.AsyncFunctionDef"],
        parent: NodeNG,
    ) -> Optional[nodes.Decorators]:
        """visit a Decorators node by returning a fresh instance of it

        Note: Method not called by 'visit'
        """
        if not node.decorator_list:
            return None
        # /!\ node is actually an _ast.FunctionDef node while
        # parent is an astroid.nodes.FunctionDef node
        if sys.version_info >= (3, 8):
            # Set the line number of the first decorator for Python 3.8+.
            lineno = node.decorator_list[0].lineno
            end_lineno = node.decorator_list[-1].end_lineno
            end_col_offset = node.decorator_list[-1].end_col_offset
        else:
            lineno = node.lineno
            end_lineno = None
            end_col_offset = None
        newnode = nodes.Decorators(
            lineno=lineno,
            col_offset=node.col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            parent=parent,
        )
        newnode.postinit([self.visit(child, newnode) for child in node.decorator_list])
        return newnode

    def visit_delete(self, node: "ast.Delete", parent: NodeNG) -> nodes.Delete:
        """visit a Delete node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Delete(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Delete(node.lineno, node.col_offset, parent)
        newnode.postinit([self.visit(child, newnode) for child in node.targets])
        return newnode

    def _visit_dict_items(
        self, node: "ast.Dict", parent: NodeNG, newnode: nodes.Dict
    ) -> Generator[Tuple[NodeNG, NodeNG], None, None]:
        for key, value in zip(node.keys, node.values):
            rebuilt_key: NodeNG
            rebuilt_value = self.visit(value, newnode)
            if not key:
                # Extended unpacking
                if sys.version_info >= (3, 8):
                    rebuilt_key = nodes.DictUnpack(
                        lineno=rebuilt_value.lineno,
                        col_offset=rebuilt_value.col_offset,
                        end_lineno=rebuilt_value.end_lineno,
                        end_col_offset=rebuilt_value.end_col_offset,
                        parent=parent,
                    )
                else:
                    rebuilt_key = nodes.DictUnpack(
                        rebuilt_value.lineno, rebuilt_value.col_offset, parent
                    )
            else:
                rebuilt_key = self.visit(key, newnode)
            yield rebuilt_key, rebuilt_value

    def visit_dict(self, node: "ast.Dict", parent: NodeNG) -> nodes.Dict:
        """visit a Dict node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Dict(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Dict(node.lineno, node.col_offset, parent)
        items = list(self._visit_dict_items(node, parent, newnode))
        newnode.postinit(items)
        return newnode

    def visit_dictcomp(self, node: "ast.DictComp", parent: NodeNG) -> nodes.DictComp:
        """visit a DictComp node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.DictComp(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.DictComp(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.key, newnode),
            self.visit(node.value, newnode),
            [self.visit(child, newnode) for child in node.generators],
        )
        return newnode

    def visit_expr(self, node: "ast.Expr", parent: NodeNG) -> nodes.Expr:
        """visit a Expr node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Expr(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Expr(node.lineno, node.col_offset, parent)
        newnode.postinit(self.visit(node.value, newnode))
        return newnode

    # Not used in Python 3.8+.
    def visit_ellipsis(self, node: "ast.Ellipsis", parent: NodeNG) -> nodes.Const:
        """visit an Ellipsis node by returning a fresh instance of Const"""
        return nodes.Const(
            value=Ellipsis,
            lineno=node.lineno,
            col_offset=node.col_offset,
            parent=parent,
        )

    def visit_excepthandler(
        self, node: "ast.ExceptHandler", parent: NodeNG
    ) -> nodes.ExceptHandler:
        """visit an ExceptHandler node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.ExceptHandler(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.ExceptHandler(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.type, newnode),
            self.visit_assignname(node, newnode, node.name),
            [self.visit(child, newnode) for child in node.body],
        )
        return newnode

    # Not used in Python 3.9+.
    def visit_extslice(
        self, node: "ast.ExtSlice", parent: nodes.Subscript
    ) -> nodes.Tuple:
        """visit an ExtSlice node by returning a fresh instance of Tuple"""
        # ExtSlice doesn't have lineno or col_offset information
        newnode = nodes.Tuple(ctx=Context.Load, parent=parent)
        newnode.postinit([self.visit(dim, newnode) for dim in node.dims])  # type: ignore[attr-defined]
        return newnode

    @overload
    def _visit_for(
        self, cls: Type[nodes.For], node: "ast.For", parent: NodeNG
    ) -> nodes.For:
        ...

    @overload
    def _visit_for(
        self, cls: Type[nodes.AsyncFor], node: "ast.AsyncFor", parent: NodeNG
    ) -> nodes.AsyncFor:
        ...

    def _visit_for(
        self, cls: Type[T_For], node: Union["ast.For", "ast.AsyncFor"], parent: NodeNG
    ) -> T_For:
        """visit a For node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = cls(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = cls(node.lineno, node.col_offset, parent)
        type_annotation = self.check_type_comment(node, parent=newnode)
        newnode.postinit(
            target=self.visit(node.target, newnode),
            iter=self.visit(node.iter, newnode),
            body=[self.visit(child, newnode) for child in node.body],
            orelse=[self.visit(child, newnode) for child in node.orelse],
            type_annotation=type_annotation,
        )
        return newnode

    def visit_for(self, node: "ast.For", parent: NodeNG) -> nodes.For:
        return self._visit_for(nodes.For, node, parent)

    def visit_importfrom(
        self, node: "ast.ImportFrom", parent: NodeNG
    ) -> nodes.ImportFrom:
        """visit an ImportFrom node by returning a fresh instance of it"""
        names = [(alias.name, alias.asname) for alias in node.names]
        if sys.version_info >= (3, 8):
            newnode = nodes.ImportFrom(
                fromname=node.module or "",
                names=names,
                level=node.level or None,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.ImportFrom(
                node.module or "",
                names,
                node.level or None,
                node.lineno,
                node.col_offset,
                parent,
            )
        # store From names to add them to locals after building
        self._import_from_nodes.append(newnode)
        return newnode

    @overload
    def _visit_functiondef(
        self, cls: Type[nodes.FunctionDef], node: "ast.FunctionDef", parent: NodeNG
    ) -> nodes.FunctionDef:
        ...

    @overload
    def _visit_functiondef(
        self,
        cls: Type[nodes.AsyncFunctionDef],
        node: "ast.AsyncFunctionDef",
        parent: NodeNG,
    ) -> nodes.AsyncFunctionDef:
        ...

    def _visit_functiondef(
        self,
        cls: Type[T_Function],
        node: Union["ast.FunctionDef", "ast.AsyncFunctionDef"],
        parent: NodeNG,
    ) -> T_Function:
        """visit an FunctionDef node to become astroid"""
        self._global_names.append({})
        node, doc = self._get_doc(node)

        lineno = node.lineno
        if PY38_PLUS and node.decorator_list:
            # Python 3.8 sets the line number of a decorated function
            # to be the actual line number of the function, but the
            # previous versions expected the decorator's line number instead.
            # We reset the function's line number to that of the
            # first decorator to maintain backward compatibility.
            # It's not ideal but this discrepancy was baked into
            # the framework for *years*.
            lineno = node.decorator_list[0].lineno

        if sys.version_info >= (3, 8):
            newnode = cls(
                name=node.name,
                doc=doc,
                lineno=lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = cls(node.name, doc, lineno, node.col_offset, parent)
        decorators = self.visit_decorators(node, newnode)
        returns: Optional[NodeNG]
        if node.returns:
            returns = self.visit(node.returns, newnode)
        else:
            returns = None

        type_comment_args = type_comment_returns = None
        type_comment_annotation = self.check_function_type_comment(node, newnode)
        if type_comment_annotation:
            type_comment_returns, type_comment_args = type_comment_annotation
        newnode.postinit(
            args=self.visit(node.args, newnode),
            body=[self.visit(child, newnode) for child in node.body],
            decorators=decorators,
            returns=returns,
            type_comment_returns=type_comment_returns,
            type_comment_args=type_comment_args,
        )
        self._global_names.pop()
        return newnode

    def visit_functiondef(
        self, node: "ast.FunctionDef", parent: NodeNG
    ) -> nodes.FunctionDef:
        return self._visit_functiondef(nodes.FunctionDef, node, parent)

    def visit_generatorexp(
        self, node: "ast.GeneratorExp", parent: NodeNG
    ) -> nodes.GeneratorExp:
        """visit a GeneratorExp node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.GeneratorExp(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.GeneratorExp(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.elt, newnode),
            [self.visit(child, newnode) for child in node.generators],
        )
        return newnode

    def visit_attribute(
        self, node: "ast.Attribute", parent: NodeNG
    ) -> Union[nodes.Attribute, nodes.AssignAttr, nodes.DelAttr]:
        """visit an Attribute node by returning a fresh instance of it"""
        context = self._get_context(node)
        newnode: Union[nodes.Attribute, nodes.AssignAttr, nodes.DelAttr]
        if context == Context.Del:
            # FIXME : maybe we should reintroduce and visit_delattr ?
            # for instance, deactivating assign_ctx
            if sys.version_info >= (3, 8):
                newnode = nodes.DelAttr(
                    attrname=node.attr,
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                    end_lineno=node.end_lineno,
                    end_col_offset=node.end_col_offset,
                    parent=parent,
                )
            else:
                newnode = nodes.DelAttr(node.attr, node.lineno, node.col_offset, parent)
        elif context == Context.Store:
            if sys.version_info >= (3, 8):
                newnode = nodes.AssignAttr(
                    attrname=node.attr,
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                    end_lineno=node.end_lineno,
                    end_col_offset=node.end_col_offset,
                    parent=parent,
                )
            else:
                newnode = nodes.AssignAttr(
                    node.attr, node.lineno, node.col_offset, parent
                )
            # Prohibit a local save if we are in an ExceptHandler.
            if not isinstance(parent, nodes.ExceptHandler):
                # mypy doesn't recognize that newnode has to be AssignAttr because it doesn't support ParamSpec
                # See https://github.com/python/mypy/issues/8645
                self._delayed_assattr.append(newnode)  # type: ignore[arg-type]
        else:
            # pylint: disable-next=else-if-used
            # Preserve symmetry with other cases
            if sys.version_info >= (3, 8):
                newnode = nodes.Attribute(
                    attrname=node.attr,
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                    end_lineno=node.end_lineno,
                    end_col_offset=node.end_col_offset,
                    parent=parent,
                )
            else:
                newnode = nodes.Attribute(
                    node.attr, node.lineno, node.col_offset, parent
                )
        newnode.postinit(self.visit(node.value, newnode))
        return newnode

    def visit_global(self, node: "ast.Global", parent: NodeNG) -> nodes.Global:
        """visit a Global node to become astroid"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Global(
                names=node.names,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Global(
                node.names,
                node.lineno,
                node.col_offset,
                parent,
            )
        if self._global_names:  # global at the module level, no effect
            for name in node.names:
                self._global_names[-1].setdefault(name, []).append(newnode)
        return newnode

    def visit_if(self, node: "ast.If", parent: NodeNG) -> nodes.If:
        """visit an If node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.If(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.If(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.test, newnode),
            [self.visit(child, newnode) for child in node.body],
            [self.visit(child, newnode) for child in node.orelse],
        )
        return newnode

    def visit_ifexp(self, node: "ast.IfExp", parent: NodeNG) -> nodes.IfExp:
        """visit a IfExp node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.IfExp(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.IfExp(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.test, newnode),
            self.visit(node.body, newnode),
            self.visit(node.orelse, newnode),
        )
        return newnode

    def visit_import(self, node: "ast.Import", parent: NodeNG) -> nodes.Import:
        """visit a Import node by returning a fresh instance of it"""
        names = [(alias.name, alias.asname) for alias in node.names]
        if sys.version_info >= (3, 8):
            newnode = nodes.Import(
                names=names,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Import(
                names,
                node.lineno,
                node.col_offset,
                parent,
            )
        # save import names in parent's locals:
        for (name, asname) in newnode.names:
            name = asname or name
            parent.set_local(name.split(".")[0], newnode)
        return newnode

    def visit_joinedstr(self, node: "ast.JoinedStr", parent: NodeNG) -> nodes.JoinedStr:
        if sys.version_info >= (3, 8):
            newnode = nodes.JoinedStr(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.JoinedStr(node.lineno, node.col_offset, parent)
        newnode.postinit([self.visit(child, newnode) for child in node.values])
        return newnode

    def visit_formattedvalue(
        self, node: "ast.FormattedValue", parent: NodeNG
    ) -> nodes.FormattedValue:
        if sys.version_info >= (3, 8):
            newnode = nodes.FormattedValue(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.FormattedValue(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.value, newnode),
            node.conversion,
            self.visit(node.format_spec, newnode),
        )
        return newnode

    def visit_namedexpr(self, node: "ast.NamedExpr", parent: NodeNG) -> nodes.NamedExpr:
        if sys.version_info >= (3, 8):
            newnode = nodes.NamedExpr(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.NamedExpr(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.target, newnode), self.visit(node.value, newnode)
        )
        return newnode

    # Not used in Python 3.9+.
    def visit_index(self, node: "ast.Index", parent: nodes.Subscript) -> NodeNG:
        """visit a Index node by returning a fresh instance of NodeNG"""
        return self.visit(node.value, parent)  # type: ignore[attr-defined]

    def visit_keyword(self, node: "ast.keyword", parent: NodeNG) -> nodes.Keyword:
        """visit a Keyword node by returning a fresh instance of it"""
        if sys.version_info >= (3, 9):
            newnode = nodes.Keyword(
                arg=node.arg,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Keyword(node.arg, parent=parent)
        newnode.postinit(self.visit(node.value, newnode))
        return newnode

    def visit_lambda(self, node: "ast.Lambda", parent: NodeNG) -> nodes.Lambda:
        """visit a Lambda node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Lambda(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Lambda(node.lineno, node.col_offset, parent)
        newnode.postinit(self.visit(node.args, newnode), self.visit(node.body, newnode))
        return newnode

    def visit_list(self, node: "ast.List", parent: NodeNG) -> nodes.List:
        """visit a List node by returning a fresh instance of it"""
        context = self._get_context(node)
        if sys.version_info >= (3, 8):
            newnode = nodes.List(
                ctx=context,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.List(
                ctx=context,
                lineno=node.lineno,
                col_offset=node.col_offset,
                parent=parent,
            )
        newnode.postinit([self.visit(child, newnode) for child in node.elts])
        return newnode

    def visit_listcomp(self, node: "ast.ListComp", parent: NodeNG) -> nodes.ListComp:
        """visit a ListComp node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.ListComp(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.ListComp(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.elt, newnode),
            [self.visit(child, newnode) for child in node.generators],
        )
        return newnode

    def visit_name(
        self, node: "ast.Name", parent: NodeNG
    ) -> Union[nodes.Name, nodes.AssignName, nodes.DelName]:
        """visit a Name node by returning a fresh instance of it"""
        context = self._get_context(node)
        newnode: Union[nodes.Name, nodes.AssignName, nodes.DelName]
        if context == Context.Del:
            if sys.version_info >= (3, 8):
                newnode = nodes.DelName(
                    name=node.id,
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                    end_lineno=node.end_lineno,
                    end_col_offset=node.end_col_offset,
                    parent=parent,
                )
            else:
                newnode = nodes.DelName(node.id, node.lineno, node.col_offset, parent)
        elif context == Context.Store:
            if sys.version_info >= (3, 8):
                newnode = nodes.AssignName(
                    name=node.id,
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                    end_lineno=node.end_lineno,
                    end_col_offset=node.end_col_offset,
                    parent=parent,
                )
            else:
                newnode = nodes.AssignName(
                    node.id, node.lineno, node.col_offset, parent
                )
        else:
            # pylint: disable-next=else-if-used
            # Preserve symmetry with other cases
            if sys.version_info >= (3, 8):
                newnode = nodes.Name(
                    name=node.id,
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                    end_lineno=node.end_lineno,
                    end_col_offset=node.end_col_offset,
                    parent=parent,
                )
            else:
                newnode = nodes.Name(node.id, node.lineno, node.col_offset, parent)
        # XXX REMOVE me :
        if context in (Context.Del, Context.Store):  # 'Aug' ??
            newnode = cast(Union[nodes.AssignName, nodes.DelName], newnode)
            self._save_assignment(newnode)
        return newnode

    # Not used in Python 3.8+.
    def visit_nameconstant(
        self, node: "ast.NameConstant", parent: NodeNG
    ) -> nodes.Const:
        # For singleton values True / False / None
        return nodes.Const(
            node.value,
            node.lineno,
            node.col_offset,
            parent,
        )

    def visit_nonlocal(self, node: "ast.Nonlocal", parent: NodeNG) -> nodes.Nonlocal:
        """visit a Nonlocal node and return a new instance of it"""
        if sys.version_info >= (3, 8):
            return nodes.Nonlocal(
                names=node.names,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        return nodes.Nonlocal(
            node.names,
            node.lineno,
            node.col_offset,
            parent,
        )

    def visit_constant(self, node: "ast.Constant", parent: NodeNG) -> nodes.Const:
        """visit a Constant node by returning a fresh instance of Const"""
        if sys.version_info >= (3, 8):
            return nodes.Const(
                value=node.value,
                kind=node.kind,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        return nodes.Const(
            node.value,
            node.lineno,
            node.col_offset,
            parent,
            node.kind,
        )

    # Not used in Python 3.8+.
    def visit_str(
        self, node: Union["ast.Str", "ast.Bytes"], parent: NodeNG
    ) -> nodes.Const:
        """visit a String/Bytes node by returning a fresh instance of Const"""
        return nodes.Const(
            node.s,
            node.lineno,
            node.col_offset,
            parent,
        )

    # Not used in Python 3.8+
    visit_bytes = visit_str

    # Not used in Python 3.8+.
    def visit_num(self, node: "ast.Num", parent: NodeNG) -> nodes.Const:
        """visit a Num node by returning a fresh instance of Const"""
        return nodes.Const(
            node.n,
            node.lineno,
            node.col_offset,
            parent,
        )

    def visit_pass(self, node: "ast.Pass", parent: NodeNG) -> nodes.Pass:
        """visit a Pass node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            return nodes.Pass(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        return nodes.Pass(node.lineno, node.col_offset, parent)

    def visit_raise(self, node: "ast.Raise", parent: NodeNG) -> nodes.Raise:
        """visit a Raise node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Raise(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Raise(node.lineno, node.col_offset, parent)
        # no traceback; anyway it is not used in Pylint
        newnode.postinit(
            exc=self.visit(node.exc, newnode),
            cause=self.visit(node.cause, newnode),
        )
        return newnode

    def visit_return(self, node: "ast.Return", parent: NodeNG) -> nodes.Return:
        """visit a Return node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Return(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Return(node.lineno, node.col_offset, parent)
        if node.value is not None:
            newnode.postinit(self.visit(node.value, newnode))
        return newnode

    def visit_set(self, node: "ast.Set", parent: NodeNG) -> nodes.Set:
        """visit a Set node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Set(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Set(node.lineno, node.col_offset, parent)
        newnode.postinit([self.visit(child, newnode) for child in node.elts])
        return newnode

    def visit_setcomp(self, node: "ast.SetComp", parent: NodeNG) -> nodes.SetComp:
        """visit a SetComp node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.SetComp(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.SetComp(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.elt, newnode),
            [self.visit(child, newnode) for child in node.generators],
        )
        return newnode

    def visit_slice(self, node: "ast.Slice", parent: nodes.Subscript) -> nodes.Slice:
        """visit a Slice node by returning a fresh instance of it"""
        if sys.version_info >= (3, 9):
            newnode = nodes.Slice(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Slice(parent=parent)
        newnode.postinit(
            lower=self.visit(node.lower, newnode),
            upper=self.visit(node.upper, newnode),
            step=self.visit(node.step, newnode),
        )
        return newnode

    def visit_subscript(self, node: "ast.Subscript", parent: NodeNG) -> nodes.Subscript:
        """visit a Subscript node by returning a fresh instance of it"""
        context = self._get_context(node)
        if sys.version_info >= (3, 8):
            newnode = nodes.Subscript(
                ctx=context,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Subscript(
                ctx=context,
                lineno=node.lineno,
                col_offset=node.col_offset,
                parent=parent,
            )
        newnode.postinit(
            self.visit(node.value, newnode), self.visit(node.slice, newnode)
        )
        return newnode

    def visit_starred(self, node: "ast.Starred", parent: NodeNG) -> nodes.Starred:
        """visit a Starred node and return a new instance of it"""
        context = self._get_context(node)
        if sys.version_info >= (3, 8):
            newnode = nodes.Starred(
                ctx=context,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Starred(
                ctx=context,
                lineno=node.lineno,
                col_offset=node.col_offset,
                parent=parent,
            )
        newnode.postinit(self.visit(node.value, newnode))
        return newnode

    def visit_tryexcept(self, node: "ast.Try", parent: NodeNG) -> nodes.TryExcept:
        """visit a TryExcept node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.TryExcept(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.TryExcept(node.lineno, node.col_offset, parent)
        newnode.postinit(
            [self.visit(child, newnode) for child in node.body],
            [self.visit(child, newnode) for child in node.handlers],
            [self.visit(child, newnode) for child in node.orelse],
        )
        return newnode

    def visit_try(
        self, node: "ast.Try", parent: NodeNG
    ) -> Union[nodes.TryExcept, nodes.TryFinally, None]:
        # python 3.3 introduce a new Try node replacing
        # TryFinally/TryExcept nodes
        if node.finalbody:
            if sys.version_info >= (3, 8):
                newnode = nodes.TryFinally(
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                    end_lineno=node.end_lineno,
                    end_col_offset=node.end_col_offset,
                    parent=parent,
                )
            else:
                newnode = nodes.TryFinally(node.lineno, node.col_offset, parent)
            body: Union[List[nodes.TryExcept], List[NodeNG]]
            if node.handlers:
                body = [self.visit_tryexcept(node, newnode)]
            else:
                body = [self.visit(child, newnode) for child in node.body]
            newnode.postinit(body, [self.visit(n, newnode) for n in node.finalbody])
            return newnode
        if node.handlers:
            return self.visit_tryexcept(node, parent)
        return None

    def visit_tryfinally(self, node: "ast.Try", parent: NodeNG) -> nodes.TryFinally:
        """visit a TryFinally node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.TryFinally(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.TryFinally(node.lineno, node.col_offset, parent)
        newnode.postinit(
            [self.visit(child, newnode) for child in node.body],
            [self.visit(n, newnode) for n in node.finalbody],
        )
        return newnode

    def visit_tuple(self, node: "ast.Tuple", parent: NodeNG) -> nodes.Tuple:
        """visit a Tuple node by returning a fresh instance of it"""
        context = self._get_context(node)
        if sys.version_info >= (3, 8):
            newnode = nodes.Tuple(
                ctx=context,
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Tuple(
                ctx=context,
                lineno=node.lineno,
                col_offset=node.col_offset,
                parent=parent,
            )
        newnode.postinit([self.visit(child, newnode) for child in node.elts])
        return newnode

    def visit_unaryop(self, node: "ast.UnaryOp", parent: NodeNG) -> nodes.UnaryOp:
        """visit a UnaryOp node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.UnaryOp(
                op=self._parser_module.unary_op_classes[node.op.__class__],
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.UnaryOp(
                self._parser_module.unary_op_classes[node.op.__class__],
                node.lineno,
                node.col_offset,
                parent,
            )
        newnode.postinit(self.visit(node.operand, newnode))
        return newnode

    def visit_while(self, node: "ast.While", parent: NodeNG) -> nodes.While:
        """visit a While node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.While(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.While(node.lineno, node.col_offset, parent)
        newnode.postinit(
            self.visit(node.test, newnode),
            [self.visit(child, newnode) for child in node.body],
            [self.visit(child, newnode) for child in node.orelse],
        )
        return newnode

    @overload
    def _visit_with(
        self, cls: Type[nodes.With], node: "ast.With", parent: NodeNG
    ) -> nodes.With:
        ...

    @overload
    def _visit_with(
        self, cls: Type[nodes.AsyncWith], node: "ast.AsyncWith", parent: NodeNG
    ) -> nodes.AsyncWith:
        ...

    def _visit_with(
        self,
        cls: Type[T_With],
        node: Union["ast.With", "ast.AsyncWith"],
        parent: NodeNG,
    ) -> T_With:
        if sys.version_info >= (3, 8):
            newnode = cls(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = cls(node.lineno, node.col_offset, parent)

        def visit_child(child: "ast.withitem") -> Tuple[NodeNG, Optional[NodeNG]]:
            expr = self.visit(child.context_expr, newnode)
            var = self.visit(child.optional_vars, newnode)
            return expr, var

        type_annotation = self.check_type_comment(node, parent=newnode)
        newnode.postinit(
            items=[visit_child(child) for child in node.items],
            body=[self.visit(child, newnode) for child in node.body],
            type_annotation=type_annotation,
        )
        return newnode

    def visit_with(self, node: "ast.With", parent: NodeNG) -> NodeNG:
        return self._visit_with(nodes.With, node, parent)

    def visit_yield(self, node: "ast.Yield", parent: NodeNG) -> NodeNG:
        """visit a Yield node by returning a fresh instance of it"""
        if sys.version_info >= (3, 8):
            newnode = nodes.Yield(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.Yield(node.lineno, node.col_offset, parent)
        if node.value is not None:
            newnode.postinit(self.visit(node.value, newnode))
        return newnode

    def visit_yieldfrom(self, node: "ast.YieldFrom", parent: NodeNG) -> NodeNG:
        if sys.version_info >= (3, 8):
            newnode = nodes.YieldFrom(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
        else:
            newnode = nodes.YieldFrom(node.lineno, node.col_offset, parent)
        if node.value is not None:
            newnode.postinit(self.visit(node.value, newnode))
        return newnode

    if sys.version_info >= (3, 10):

        def visit_match(self, node: "ast.Match", parent: NodeNG) -> nodes.Match:
            newnode = nodes.Match(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
            newnode.postinit(
                subject=self.visit(node.subject, newnode),
                cases=[self.visit(case, newnode) for case in node.cases],
            )
            return newnode

        def visit_matchcase(
            self, node: "ast.match_case", parent: NodeNG
        ) -> nodes.MatchCase:
            newnode = nodes.MatchCase(parent=parent)
            newnode.postinit(
                pattern=self.visit(node.pattern, newnode),
                guard=self.visit(node.guard, newnode),
                body=[self.visit(child, newnode) for child in node.body],
            )
            return newnode

        def visit_matchvalue(
            self, node: "ast.MatchValue", parent: NodeNG
        ) -> nodes.MatchValue:
            newnode = nodes.MatchValue(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
            newnode.postinit(value=self.visit(node.value, newnode))
            return newnode

        def visit_matchsingleton(
            self, node: "ast.MatchSingleton", parent: NodeNG
        ) -> nodes.MatchSingleton:
            return nodes.MatchSingleton(
                value=node.value,  # type: ignore[arg-type] # See https://github.com/python/mypy/pull/10389
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )

        def visit_matchsequence(
            self, node: "ast.MatchSequence", parent: NodeNG
        ) -> nodes.MatchSequence:
            newnode = nodes.MatchSequence(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
            newnode.postinit(
                patterns=[self.visit(pattern, newnode) for pattern in node.patterns]
            )
            return newnode

        def visit_matchmapping(
            self, node: "ast.MatchMapping", parent: NodeNG
        ) -> nodes.MatchMapping:
            newnode = nodes.MatchMapping(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
            # Add AssignName node for 'node.name'
            # https://bugs.python.org/issue43994
            newnode.postinit(
                keys=[self.visit(child, newnode) for child in node.keys],
                patterns=[self.visit(pattern, newnode) for pattern in node.patterns],
                rest=self.visit_assignname(node, newnode, node.rest),
            )
            return newnode

        def visit_matchclass(
            self, node: "ast.MatchClass", parent: NodeNG
        ) -> nodes.MatchClass:
            newnode = nodes.MatchClass(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
            newnode.postinit(
                cls=self.visit(node.cls, newnode),
                patterns=[self.visit(pattern, newnode) for pattern in node.patterns],
                kwd_attrs=node.kwd_attrs,
                kwd_patterns=[
                    self.visit(pattern, newnode) for pattern in node.kwd_patterns
                ],
            )
            return newnode

        def visit_matchstar(
            self, node: "ast.MatchStar", parent: NodeNG
        ) -> nodes.MatchStar:
            newnode = nodes.MatchStar(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
            # Add AssignName node for 'node.name'
            # https://bugs.python.org/issue43994
            newnode.postinit(name=self.visit_assignname(node, newnode, node.name))
            return newnode

        def visit_matchas(self, node: "ast.MatchAs", parent: NodeNG) -> nodes.MatchAs:
            newnode = nodes.MatchAs(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
            # Add AssignName node for 'node.name'
            # https://bugs.python.org/issue43994
            newnode.postinit(
                pattern=self.visit(node.pattern, newnode),
                name=self.visit_assignname(node, newnode, node.name),
            )
            return newnode

        def visit_matchor(self, node: "ast.MatchOr", parent: NodeNG) -> nodes.MatchOr:
            newnode = nodes.MatchOr(
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=node.end_lineno,
                end_col_offset=node.end_col_offset,
                parent=parent,
            )
            newnode.postinit(
                patterns=[self.visit(pattern, newnode) for pattern in node.patterns]
            )
            return newnode
