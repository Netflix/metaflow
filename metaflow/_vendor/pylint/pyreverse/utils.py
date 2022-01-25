# Copyright (c) 2006, 2008, 2010, 2013-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2017, 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2020 yeting li <liyt@ios.ac.cn>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2020 bernie gray <bfgray3@users.noreply.github.com>
# Copyright (c) 2021 bot <bot@noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Mark Byrne <31762852+mbyrnepr2@users.noreply.github.com>
# Copyright (c) 2021 Andreas Finkler <andi.finkler@gmail.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Generic classes/functions for pyreverse core/extensions. """
import os
import re
import shutil
import sys
from typing import Optional, Union

from metaflow._vendor import astroid
from metaflow._vendor.astroid import nodes

RCFILE = ".pyreverserc"


def get_default_options():
    """Read config file and return list of options."""
    options = []
    home = os.environ.get("HOME", "")
    if home:
        rcfile = os.path.join(home, RCFILE)
        try:
            with open(rcfile, encoding="utf-8") as file_handle:
                options = file_handle.read().split()
        except OSError:
            pass  # ignore if no config file found
    return options


def insert_default_options():
    """insert default options to sys.argv"""
    options = get_default_options()
    options.reverse()
    for arg in options:
        sys.argv.insert(1, arg)


# astroid utilities ###########################################################
SPECIAL = re.compile(r"^__([^\W_]_*)+__$")
PRIVATE = re.compile(r"^__(_*[^\W_])+_?$")
PROTECTED = re.compile(r"^_\w*$")


def get_visibility(name):
    """return the visibility from a name: public, protected, private or special"""
    if SPECIAL.match(name):
        visibility = "special"
    elif PRIVATE.match(name):
        visibility = "private"
    elif PROTECTED.match(name):
        visibility = "protected"

    else:
        visibility = "public"
    return visibility


ABSTRACT = re.compile(r"^.*Abstract.*")
FINAL = re.compile(r"^[^\W\da-z]*$")


def is_abstract(node):
    """return true if the given class node correspond to an abstract class
    definition
    """
    return ABSTRACT.match(node.name)


def is_final(node):
    """return true if the given class/function node correspond to final
    definition
    """
    return FINAL.match(node.name)


def is_interface(node):
    # bw compat
    return node.type == "interface"


def is_exception(node):
    # bw compat
    return node.type == "exception"


# Helpers #####################################################################

_CONSTRUCTOR = 1
_SPECIAL = 2
_PROTECTED = 4
_PRIVATE = 8
MODES = {
    "ALL": 0,
    "PUB_ONLY": _SPECIAL + _PROTECTED + _PRIVATE,
    "SPECIAL": _SPECIAL,
    "OTHER": _PROTECTED + _PRIVATE,
}
VIS_MOD = {
    "special": _SPECIAL,
    "protected": _PROTECTED,
    "private": _PRIVATE,
    "public": 0,
}


class FilterMixIn:
    """filter nodes according to a mode and nodes' visibility"""

    def __init__(self, mode):
        "init filter modes"
        __mode = 0
        for nummod in mode.split("+"):
            try:
                __mode += MODES[nummod]
            except KeyError as ex:
                print(f"Unknown filter mode {ex}", file=sys.stderr)
        self.__mode = __mode

    def show_attr(self, node):
        """return true if the node should be treated"""
        visibility = get_visibility(getattr(node, "name", node))
        return not self.__mode & VIS_MOD[visibility]


class ASTWalker:
    """a walker visiting a tree in preorder, calling on the handler:

    * visit_<class name> on entering a node, where class name is the class of
    the node in lower case

    * leave_<class name> on leaving a node, where class name is the class of
    the node in lower case
    """

    def __init__(self, handler):
        self.handler = handler
        self._cache = {}

    def walk(self, node, _done=None):
        """walk on the tree from <node>, getting callbacks from handler"""
        if _done is None:
            _done = set()
        if node in _done:
            raise AssertionError((id(node), node, node.parent))
        _done.add(node)
        self.visit(node)
        for child_node in node.get_children():
            assert child_node is not node
            self.walk(child_node, _done)
        self.leave(node)
        assert node.parent is not node

    def get_callbacks(self, node):
        """get callbacks from handler for the visited node"""
        klass = node.__class__
        methods = self._cache.get(klass)
        if methods is None:
            handler = self.handler
            kid = klass.__name__.lower()
            e_method = getattr(
                handler, f"visit_{kid}", getattr(handler, "visit_default", None)
            )
            l_method = getattr(
                handler, f"leave_{kid}", getattr(handler, "leave_default", None)
            )
            self._cache[klass] = (e_method, l_method)
        else:
            e_method, l_method = methods
        return e_method, l_method

    def visit(self, node):
        """walk on the tree from <node>, getting callbacks from handler"""
        method = self.get_callbacks(node)[0]
        if method is not None:
            method(node)

    def leave(self, node):
        """walk on the tree from <node>, getting callbacks from handler"""
        method = self.get_callbacks(node)[1]
        if method is not None:
            method(node)


class LocalsVisitor(ASTWalker):
    """visit a project by traversing the locals dictionary"""

    def __init__(self):
        super().__init__(self)
        self._visited = set()

    def visit(self, node):
        """launch the visit starting from the given node"""
        if node in self._visited:
            return None

        self._visited.add(node)
        methods = self.get_callbacks(node)
        if methods[0] is not None:
            methods[0](node)
        if hasattr(node, "locals"):  # skip Instance and other proxy
            for local_node in node.values():
                self.visit(local_node)
        if methods[1] is not None:
            return methods[1](node)
        return None


def get_annotation_label(ann: Union[nodes.Name, nodes.Subscript]) -> str:
    label = ""
    if isinstance(ann, nodes.Subscript):
        label = ann.as_string()
    elif isinstance(ann, nodes.Name):
        label = ann.name
    return label


def get_annotation(
    node: Union[nodes.AssignAttr, nodes.AssignName]
) -> Optional[Union[nodes.Name, nodes.Subscript]]:
    """return the annotation for `node`"""
    ann = None
    if isinstance(node.parent, nodes.AnnAssign):
        ann = node.parent.annotation
    elif isinstance(node, nodes.AssignAttr):
        init_method = node.parent.parent
        try:
            annotations = dict(zip(init_method.locals, init_method.args.annotations))
            ann = annotations.get(node.parent.value.name)
        except AttributeError:
            pass
    else:
        return ann

    try:
        default, *_ = node.infer()
    except astroid.InferenceError:
        default = ""

    label = get_annotation_label(ann)
    if ann:
        label = (
            fr"Optional[{label}]"
            if getattr(default, "value", "value") is None
            and not label.startswith("Optional")
            else label
        )
    if label:
        ann.name = label
    return ann


def infer_node(node: Union[nodes.AssignAttr, nodes.AssignName]) -> set:
    """Return a set containing the node annotation if it exists
    otherwise return a set of the inferred types using the NodeNG.infer method"""

    ann = get_annotation(node)
    try:
        if ann:
            if isinstance(ann, nodes.Subscript):
                return {ann}
            return set(ann.infer())
        return set(node.infer())
    except astroid.InferenceError:
        return {ann} if ann else set()


def check_graphviz_availability():
    """Check if the ``dot`` command is available on the machine.
    This is needed if image output is desired and ``dot`` is used to convert
    from *.dot or *.gv into the final output format."""
    if shutil.which("dot") is None:
        print(
            "The requested output format is currently not available.\n"
            "Please install 'Graphviz' to have other output formats "
            "than 'dot' or 'vcg'."
        )
        sys.exit(32)
