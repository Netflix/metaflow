# Copyright (c) 2006-2015 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2012-2014 Google, Inc.
# Copyright (c) 2013 buck@yelp.com <buck@yelp.com>
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015-2016 Moises Lopez <moylop260@vauxoo.com>
# Copyright (c) 2015 Dmitry Pribysh <dmand@yandex.ru>
# Copyright (c) 2015 Cezar <celnazli@bitdefender.com>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2015 Noam Yorav-Raphael <noamraph@gmail.com>
# Copyright (c) 2015 James Morgensen <james.morgensen@gmail.com>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016, 2021 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2016 Jared Garst <cultofjared@gmail.com>
# Copyright (c) 2016 Maik Röder <maikroeder@gmail.com>
# Copyright (c) 2016 Glenn Matthews <glenn@e-dad.net>
# Copyright (c) 2017, 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017 Michka Popoff <michkapopoff@gmail.com>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2017 Erik Wright <erik.wright@shopify.com>
# Copyright (c) 2018 Lucas Cimon <lucas.cimon@gmail.com>
# Copyright (c) 2018 Hornwitser <github@hornwitser.no>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Natalie Serebryakova <natalie.serebryakova@Natalies-MacBook-Pro.local>
# Copyright (c) 2018 Mike Frysinger <vapier@gmail.com>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 Marianna Polatoglou <mpolatoglou@bloomberg.net>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019, 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2019 Nick Smith <clickthisnick@users.noreply.github.com>
# Copyright (c) 2019 Paul Renvoisé <renvoisepaul@gmail.com>
# Copyright (c) 2020 Peter Kolbus <peter.kolbus@gmail.com>
# Copyright (c) 2020 Damien Baty <damien.baty@polyconseil.fr>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 Tushar Sadhwani <tushar.sadhwani000@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Will Shanks <wsha@posteo.net>
# Copyright (c) 2021 Matus Valo <matusvalo@users.noreply.github.com>
# Copyright (c) 2021 Yu Shao, Pang <36848472+yushao2@users.noreply.github.com>
# Copyright (c) 2021 Andrew Howe <howeaj@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""imports checkers for Python code"""

import collections
import copy
import os
import sys
from distutils import sysconfig
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from metaflow._vendor import astroid
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker, DeprecatedMixin
from metaflow._vendor.pylint.checkers.utils import (
    check_messages,
    get_import_name,
    is_from_fallback_block,
    is_node_in_guarded_import_block,
    is_typing_guard,
    node_ignores_exception,
)
from metaflow._vendor.pylint.exceptions import EmptyReportError
from metaflow._vendor.pylint.graph import DotBackend, get_cycles
from metaflow._vendor.pylint.interfaces import IAstroidChecker
from metaflow._vendor.pylint.lint import PyLinter
from metaflow._vendor.pylint.reporters.ureports.nodes import Paragraph, Section, VerbatimText
from metaflow._vendor.pylint.utils import IsortDriver, get_global_option


def _qualified_names(modname):
    """Split the names of the given module into subparts

    For example,
        _qualified_names('pylint.checkers.ImportsChecker')
    returns
        ['pylint', 'pylint.checkers', 'pylint.checkers.ImportsChecker']
    """
    names = modname.split(".")
    return [".".join(names[0 : i + 1]) for i in range(len(names))]


def _get_first_import(node, context, name, base, level, alias):
    """return the node where [base.]<name> is imported or None if not found"""
    fullname = f"{base}.{name}" if base else name

    first = None
    found = False
    for first in context.body:
        if first is node:
            continue
        if first.scope() is node.scope() and first.fromlineno > node.fromlineno:
            continue
        if isinstance(first, nodes.Import):
            if any(fullname == iname[0] for iname in first.names):
                found = True
                break
        elif isinstance(first, nodes.ImportFrom):
            if level == first.level:
                for imported_name, imported_alias in first.names:
                    if fullname == f"{first.modname}.{imported_name}":
                        found = True
                        break
                    if (
                        name != "*"
                        and name == imported_name
                        and not (alias or imported_alias)
                    ):
                        found = True
                        break
                if found:
                    break
    if found and not astroid.are_exclusive(first, node):
        return first
    return None


def _ignore_import_failure(node, modname, ignored_modules):
    for submodule in _qualified_names(modname):
        if submodule in ignored_modules:
            return True

    if is_node_in_guarded_import_block(node):
        # Ignore import failure if part of guarded import block
        # I.e. `sys.version_info` or `typing.TYPE_CHECKING`
        return True

    return node_ignores_exception(node, ImportError)


# utilities to represents import dependencies as tree and dot graph ###########


def _make_tree_defs(mod_files_list):
    """get a list of 2-uple (module, list_of_files_which_import_this_module),
    it will return a dictionary to represent this as a tree
    """
    tree_defs = {}
    for mod, files in mod_files_list:
        node = (tree_defs, ())
        for prefix in mod.split("."):
            node = node[0].setdefault(prefix, [{}, []])
        node[1] += files
    return tree_defs


def _repr_tree_defs(data, indent_str=None):
    """return a string which represents imports as a tree"""
    lines = []
    nodes_items = data.items()
    for i, (mod, (sub, files)) in enumerate(sorted(nodes_items, key=lambda x: x[0])):
        files = "" if not files else f"({','.join(sorted(files))})"
        if indent_str is None:
            lines.append(f"{mod} {files}")
            sub_indent_str = "  "
        else:
            lines.append(fr"{indent_str}\-{mod} {files}")
            if i == len(nodes_items) - 1:
                sub_indent_str = f"{indent_str}  "
            else:
                sub_indent_str = f"{indent_str}| "
        if sub:
            lines.append(_repr_tree_defs(sub, sub_indent_str))
    return "\n".join(lines)


def _dependencies_graph(filename: str, dep_info: Dict[str, Set[str]]) -> str:
    """write dependencies as a dot (graphviz) file"""
    done = {}
    printer = DotBackend(os.path.splitext(os.path.basename(filename))[0], rankdir="LR")
    printer.emit('URL="." node[shape="box"]')
    for modname, dependencies in sorted(dep_info.items()):
        sorted_dependencies = sorted(dependencies)
        done[modname] = 1
        printer.emit_node(modname)
        for depmodname in sorted_dependencies:
            if depmodname not in done:
                done[depmodname] = 1
                printer.emit_node(depmodname)
    for depmodname, dependencies in sorted(dep_info.items()):
        for modname in sorted(dependencies):
            printer.emit_edge(modname, depmodname)
    return printer.generate(filename)


def _make_graph(
    filename: str, dep_info: Dict[str, Set[str]], sect: Section, gtype: str
):
    """generate a dependencies graph and add some information about it in the
    report's section
    """
    outputfile = _dependencies_graph(filename, dep_info)
    sect.append(Paragraph((f"{gtype}imports graph has been written to {outputfile}",)))


# the import checker itself ###################################################

MSGS = {
    "E0401": (
        "Unable to import %s",
        "import-error",
        "Used when pylint has been unable to import a module.",
        {"old_names": [("F0401", "old-import-error")]},
    ),
    "E0402": (
        "Attempted relative import beyond top-level package",
        "relative-beyond-top-level",
        "Used when a relative import tries to access too many levels "
        "in the current package.",
    ),
    "R0401": (
        "Cyclic import (%s)",
        "cyclic-import",
        "Used when a cyclic import between two or more modules is detected.",
    ),
    "R0402": (
        "Use 'from %s import %s' instead",
        "consider-using-from-import",
        "Emitted when a submodule of a package is imported and "
        "aliased with the same name. "
        "E.g., instead of ``import concurrent.futures as futures`` use "
        "``from concurrent import futures``",
    ),
    "W0401": (
        "Wildcard import %s",
        "wildcard-import",
        "Used when `from module import *` is detected.",
    ),
    "W0402": (
        "Uses of a deprecated module %r",
        "deprecated-module",
        "Used a module marked as deprecated is imported.",
    ),
    "W0404": (
        "Reimport %r (imported line %s)",
        "reimported",
        "Used when a module is reimported multiple times.",
    ),
    "W0406": (
        "Module import itself",
        "import-self",
        "Used when a module is importing itself.",
    ),
    "W0407": (
        "Prefer importing %r instead of %r",
        "preferred-module",
        "Used when a module imported has a preferred replacement module.",
    ),
    "W0410": (
        "__future__ import is not the first non docstring statement",
        "misplaced-future",
        "Python 2.5 and greater require __future__ import to be the "
        "first non docstring statement in the module.",
    ),
    "C0410": (
        "Multiple imports on one line (%s)",
        "multiple-imports",
        "Used when import statement importing multiple modules is detected.",
    ),
    "C0411": (
        "%s should be placed before %s",
        "wrong-import-order",
        "Used when PEP8 import order is not respected (standard imports "
        "first, then third-party libraries, then local imports)",
    ),
    "C0412": (
        "Imports from package %s are not grouped",
        "ungrouped-imports",
        "Used when imports are not grouped by packages",
    ),
    "C0413": (
        'Import "%s" should be placed at the top of the module',
        "wrong-import-position",
        "Used when code and imports are mixed",
    ),
    "C0414": (
        "Import alias does not rename original package",
        "useless-import-alias",
        "Used when an import alias is same as original package."
        "e.g using import numpy as numpy instead of import numpy as np",
    ),
    "C0415": (
        "Import outside toplevel (%s)",
        "import-outside-toplevel",
        "Used when an import statement is used anywhere other than the module "
        "toplevel. Move this import to the top of the file.",
    ),
}


DEFAULT_STANDARD_LIBRARY = ()
DEFAULT_KNOWN_THIRD_PARTY = ("enchant",)
DEFAULT_PREFERRED_MODULES = ()


class ImportsChecker(DeprecatedMixin, BaseChecker):
    """checks for
    * external modules dependencies
    * relative / wildcard imports
    * cyclic imports
    * uses of deprecated modules
    * uses of modules instead of preferred modules
    """

    __implements__ = IAstroidChecker

    name = "imports"
    msgs = MSGS
    priority = -2
    default_deprecated_modules = ()

    options = (
        (
            "deprecated-modules",
            {
                "default": default_deprecated_modules,
                "type": "csv",
                "metavar": "<modules>",
                "help": "Deprecated modules which should not be used,"
                " separated by a comma.",
            },
        ),
        (
            "preferred-modules",
            {
                "default": DEFAULT_PREFERRED_MODULES,
                "type": "csv",
                "metavar": "<module:preferred-module>",
                "help": "Couples of modules and preferred modules,"
                " separated by a comma.",
            },
        ),
        (
            "import-graph",
            {
                "default": "",
                "type": "string",
                "metavar": "<file.gv>",
                "help": "Output a graph (.gv or any supported image format) of"
                " all (i.e. internal and external) dependencies to the given file"
                " (report RP0402 must not be disabled).",
            },
        ),
        (
            "ext-import-graph",
            {
                "default": "",
                "type": "string",
                "metavar": "<file.gv>",
                "help": "Output a graph (.gv or any supported image format)"
                " of external dependencies to the given file"
                " (report RP0402 must not be disabled).",
            },
        ),
        (
            "int-import-graph",
            {
                "default": "",
                "type": "string",
                "metavar": "<file.gv>",
                "help": "Output a graph (.gv or any supported image format)"
                " of internal dependencies to the given file"
                " (report RP0402 must not be disabled).",
            },
        ),
        (
            "known-standard-library",
            {
                "default": DEFAULT_STANDARD_LIBRARY,
                "type": "csv",
                "metavar": "<modules>",
                "help": "Force import order to recognize a module as part of "
                "the standard compatibility libraries.",
            },
        ),
        (
            "known-third-party",
            {
                "default": DEFAULT_KNOWN_THIRD_PARTY,
                "type": "csv",
                "metavar": "<modules>",
                "help": "Force import order to recognize a module as part of "
                "a third party library.",
            },
        ),
        (
            "allow-any-import-level",
            {
                "default": (),
                "type": "csv",
                "metavar": "<modules>",
                "help": (
                    "List of modules that can be imported at any level, not just "
                    "the top level one."
                ),
            },
        ),
        (
            "analyse-fallback-blocks",
            {
                "default": False,
                "type": "yn",
                "metavar": "<y or n>",
                "help": "Analyse import fallback blocks. This can be used to "
                "support both Python 2 and 3 compatible code, which "
                "means that the block might have code that exists "
                "only in one or another interpreter, leading to false "
                "positives when analysed.",
            },
        ),
        (
            "allow-wildcard-with-all",
            {
                "default": False,
                "type": "yn",
                "metavar": "<y or n>",
                "help": "Allow wildcard imports from modules that define __all__.",
            },
        ),
    )

    def __init__(
        self, linter: Optional[PyLinter] = None
    ):  # pylint: disable=super-init-not-called # See https://github.com/PyCQA/pylint/issues/4941
        BaseChecker.__init__(self, linter)
        self.import_graph: collections.defaultdict = collections.defaultdict(set)
        self._imports_stack: List[Tuple[Any, Any]] = []
        self._first_non_import_node = None
        self._module_pkg: Dict[
            Any, Any
        ] = {}  # mapping of modules to the pkg they belong in
        self._allow_any_import_level: Set[Any] = set()
        self.reports = (
            ("RP0401", "External dependencies", self._report_external_dependencies),
            ("RP0402", "Modules dependencies graph", self._report_dependencies_graph),
        )

        self._site_packages = self._compute_site_packages()

    @staticmethod
    def _compute_site_packages():
        def _normalized_path(path):
            return os.path.normcase(os.path.abspath(path))

        paths = set()
        real_prefix = getattr(sys, "real_prefix", None)
        for prefix in filter(None, (real_prefix, sys.prefix)):
            path = sysconfig.get_python_lib(prefix=prefix)
            path = _normalized_path(path)
            paths.add(path)

        # Handle Debian's derivatives /usr/local.
        if os.path.isfile("/etc/debian_version"):
            for prefix in filter(None, (real_prefix, sys.prefix)):
                libpython = os.path.join(
                    prefix,
                    "local",
                    "lib",
                    "python" + sysconfig.get_python_version(),
                    "dist-packages",
                )
                paths.add(libpython)
        return paths

    def open(self):
        """called before visiting project (i.e set of modules)"""
        self.linter.stats.dependencies = {}
        self.linter.stats = self.linter.stats
        self.import_graph = collections.defaultdict(set)
        self._module_pkg = {}  # mapping of modules to the pkg they belong in
        self._excluded_edges = collections.defaultdict(set)
        self._ignored_modules = get_global_option(self, "ignored-modules", default=[])
        # Build a mapping {'module': 'preferred-module'}
        self.preferred_modules = dict(
            module.split(":")
            for module in self.config.preferred_modules
            if ":" in module
        )
        self._allow_any_import_level = set(self.config.allow_any_import_level)

    def _import_graph_without_ignored_edges(self):
        filtered_graph = copy.deepcopy(self.import_graph)
        for node in filtered_graph:
            filtered_graph[node].difference_update(self._excluded_edges[node])
        return filtered_graph

    def close(self):
        """called before visiting project (i.e set of modules)"""
        if self.linter.is_message_enabled("cyclic-import"):
            graph = self._import_graph_without_ignored_edges()
            vertices = list(graph)
            for cycle in get_cycles(graph, vertices=vertices):
                self.add_message("cyclic-import", args=" -> ".join(cycle))

    def deprecated_modules(self):
        """Callback returning the deprecated modules."""
        return self.config.deprecated_modules

    @check_messages(*MSGS)
    def visit_import(self, node: nodes.Import) -> None:
        """triggered when an import statement is seen"""
        self._check_reimport(node)
        self._check_import_as_rename(node)
        self._check_toplevel(node)

        names = [name for name, _ in node.names]
        if len(names) >= 2:
            self.add_message("multiple-imports", args=", ".join(names), node=node)

        for name in names:
            self.check_deprecated_module(node, name)
            self._check_preferred_module(node, name)
            imported_module = self._get_imported_module(node, name)
            if isinstance(node.parent, nodes.Module):
                # Allow imports nested
                self._check_position(node)
            if isinstance(node.scope(), nodes.Module):
                self._record_import(node, imported_module)

            if imported_module is None:
                continue

            self._add_imported_module(node, imported_module.name)

    @check_messages(*MSGS)
    def visit_importfrom(self, node: nodes.ImportFrom) -> None:
        """triggered when a from statement is seen"""
        basename = node.modname
        imported_module = self._get_imported_module(node, basename)

        self._check_import_as_rename(node)
        self._check_misplaced_future(node)
        self.check_deprecated_module(node, basename)
        self._check_preferred_module(node, basename)
        self._check_wildcard_imports(node, imported_module)
        self._check_same_line_imports(node)
        self._check_reimport(node, basename=basename, level=node.level)
        self._check_toplevel(node)

        if isinstance(node.parent, nodes.Module):
            # Allow imports nested
            self._check_position(node)
        if isinstance(node.scope(), nodes.Module):
            self._record_import(node, imported_module)
        if imported_module is None:
            return
        for name, _ in node.names:
            if name != "*":
                self._add_imported_module(node, f"{imported_module.name}.{name}")
            else:
                self._add_imported_module(node, imported_module.name)

    @check_messages(*MSGS)
    def leave_module(self, node: nodes.Module) -> None:
        # Check imports are grouped by category (standard, 3rd party, local)
        std_imports, ext_imports, loc_imports = self._check_imports_order(node)

        # Check that imports are grouped by package within a given category
        met_import: Set[str] = set()  # set for 'import x' style
        met_from: Set[str] = set()  # set for 'from x import y' style
        current_package = None
        for import_node, import_name in std_imports + ext_imports + loc_imports:
            if not self.linter.is_message_enabled(
                "ungrouped-imports", import_node.fromlineno
            ):
                continue
            met = met_from if isinstance(import_node, nodes.ImportFrom) else met_import
            package, _, _ = import_name.partition(".")
            if (
                current_package
                and current_package != package
                and package in met
                and is_node_in_guarded_import_block(import_node) is False
            ):
                self.add_message("ungrouped-imports", node=import_node, args=package)
            current_package = package
            met.add(package)

        self._imports_stack = []
        self._first_non_import_node = None

    def compute_first_non_import_node(self, node):
        if not self.linter.is_message_enabled("wrong-import-position", node.fromlineno):
            return
        # if the node does not contain an import instruction, and if it is the
        # first node of the module, keep a track of it (all the import positions
        # of the module will be compared to the position of this first
        # instruction)
        if self._first_non_import_node:
            return
        if not isinstance(node.parent, nodes.Module):
            return
        nested_allowed = [nodes.TryExcept, nodes.TryFinally]
        is_nested_allowed = [
            allowed for allowed in nested_allowed if isinstance(node, allowed)
        ]
        if is_nested_allowed and any(
            node.nodes_of_class((nodes.Import, nodes.ImportFrom))
        ):
            return
        if isinstance(node, nodes.Assign):
            # Add compatibility for module level dunder names
            # https://www.python.org/dev/peps/pep-0008/#module-level-dunder-names
            valid_targets = [
                isinstance(target, nodes.AssignName)
                and target.name.startswith("__")
                and target.name.endswith("__")
                for target in node.targets
            ]
            if all(valid_targets):
                return
        self._first_non_import_node = node

    visit_tryfinally = (
        visit_tryexcept
    ) = (
        visit_assignattr
    ) = (
        visit_assign
    ) = (
        visit_ifexp
    ) = visit_comprehension = visit_expr = visit_if = compute_first_non_import_node

    def visit_functiondef(self, node: nodes.FunctionDef) -> None:
        if not self.linter.is_message_enabled("wrong-import-position", node.fromlineno):
            return
        # If it is the first non import instruction of the module, record it.
        if self._first_non_import_node:
            return

        # Check if the node belongs to an `If` or a `Try` block. If they
        # contain imports, skip recording this node.
        if not isinstance(node.parent.scope(), nodes.Module):
            return

        root = node
        while not isinstance(root.parent, nodes.Module):
            root = root.parent

        if isinstance(root, (nodes.If, nodes.TryFinally, nodes.TryExcept)):
            if any(root.nodes_of_class((nodes.Import, nodes.ImportFrom))):
                return

        self._first_non_import_node = node

    visit_classdef = visit_for = visit_while = visit_functiondef

    def _check_misplaced_future(self, node):
        basename = node.modname
        if basename == "__future__":
            # check if this is the first non-docstring statement in the module
            prev = node.previous_sibling()
            if prev:
                # consecutive future statements are possible
                if not (
                    isinstance(prev, nodes.ImportFrom) and prev.modname == "__future__"
                ):
                    self.add_message("misplaced-future", node=node)
            return

    def _check_same_line_imports(self, node):
        # Detect duplicate imports on the same line.
        names = (name for name, _ in node.names)
        counter = collections.Counter(names)
        for name, count in counter.items():
            if count > 1:
                self.add_message("reimported", node=node, args=(name, node.fromlineno))

    def _check_position(self, node):
        """Check `node` import or importfrom node position is correct

        Send a message  if `node` comes before another instruction
        """
        # if a first non-import instruction has already been encountered,
        # it means the import comes after it and therefore is not well placed
        if self._first_non_import_node:
            self.add_message("wrong-import-position", node=node, args=node.as_string())

    def _record_import(self, node, importedmodnode):
        """Record the package `node` imports from"""
        if isinstance(node, nodes.ImportFrom):
            importedname = node.modname
        else:
            importedname = importedmodnode.name if importedmodnode else None
        if not importedname:
            importedname = node.names[0][0].split(".")[0]

        if isinstance(node, nodes.ImportFrom) and (node.level or 0) >= 1:
            # We need the importedname with first point to detect local package
            # Example of node:
            #  'from .my_package1 import MyClass1'
            #  the output should be '.my_package1' instead of 'my_package1'
            # Example of node:
            #  'from . import my_package2'
            #  the output should be '.my_package2' instead of '{pyfile}'
            importedname = "." + importedname

        self._imports_stack.append((node, importedname))

    @staticmethod
    def _is_fallback_import(node, imports):
        imports = [import_node for (import_node, _) in imports]
        return any(astroid.are_exclusive(import_node, node) for import_node in imports)

    def _check_imports_order(self, _module_node):
        """Checks imports of module `node` are grouped by category

        Imports must follow this order: standard, 3rd party, local
        """
        std_imports = []
        third_party_imports = []
        first_party_imports = []
        # need of a list that holds third or first party ordered import
        external_imports = []
        local_imports = []
        third_party_not_ignored = []
        first_party_not_ignored = []
        local_not_ignored = []
        isort_driver = IsortDriver(self.config)
        for node, modname in self._imports_stack:
            if modname.startswith("."):
                package = "." + modname.split(".")[1]
            else:
                package = modname.split(".")[0]
            nested = not isinstance(node.parent, nodes.Module)
            ignore_for_import_order = not self.linter.is_message_enabled(
                "wrong-import-order", node.fromlineno
            )
            import_category = isort_driver.place_module(package)
            node_and_package_import = (node, package)
            if import_category in {"FUTURE", "STDLIB"}:
                std_imports.append(node_and_package_import)
                wrong_import = (
                    third_party_not_ignored
                    or first_party_not_ignored
                    or local_not_ignored
                )
                if self._is_fallback_import(node, wrong_import):
                    continue
                if wrong_import and not nested:
                    self.add_message(
                        "wrong-import-order",
                        node=node,
                        args=(
                            f'standard import "{node.as_string()}"',
                            f'"{wrong_import[0][0].as_string()}"',
                        ),
                    )
            elif import_category == "THIRDPARTY":
                third_party_imports.append(node_and_package_import)
                external_imports.append(node_and_package_import)
                if not nested:
                    if not ignore_for_import_order:
                        third_party_not_ignored.append(node_and_package_import)
                    else:
                        self.linter.add_ignored_message(
                            "wrong-import-order", node.fromlineno, node
                        )
                wrong_import = first_party_not_ignored or local_not_ignored
                if wrong_import and not nested:
                    self.add_message(
                        "wrong-import-order",
                        node=node,
                        args=(
                            f'third party import "{node.as_string()}"',
                            f'"{wrong_import[0][0].as_string()}"',
                        ),
                    )
            elif import_category == "FIRSTPARTY":
                first_party_imports.append(node_and_package_import)
                external_imports.append(node_and_package_import)
                if not nested:
                    if not ignore_for_import_order:
                        first_party_not_ignored.append(node_and_package_import)
                    else:
                        self.linter.add_ignored_message(
                            "wrong-import-order", node.fromlineno, node
                        )
                wrong_import = local_not_ignored
                if wrong_import and not nested:
                    self.add_message(
                        "wrong-import-order",
                        node=node,
                        args=(
                            f'first party import "{node.as_string()}"',
                            f'"{wrong_import[0][0].as_string()}"',
                        ),
                    )
            elif import_category == "LOCALFOLDER":
                local_imports.append((node, package))
                if not nested:
                    if not ignore_for_import_order:
                        local_not_ignored.append((node, package))
                    else:
                        self.linter.add_ignored_message(
                            "wrong-import-order", node.fromlineno, node
                        )
        return std_imports, external_imports, local_imports

    def _get_imported_module(self, importnode, modname):
        try:
            return importnode.do_import_module(modname)
        except astroid.TooManyLevelsError:
            if _ignore_import_failure(importnode, modname, self._ignored_modules):
                return None
            self.add_message("relative-beyond-top-level", node=importnode)
        except astroid.AstroidSyntaxError as exc:
            message = f"Cannot import {modname!r} due to syntax error {str(exc.error)!r}"  # pylint: disable=no-member; false positive
            self.add_message("syntax-error", line=importnode.lineno, args=message)

        except astroid.AstroidBuildingException:
            if not self.linter.is_message_enabled("import-error"):
                return None
            if _ignore_import_failure(importnode, modname, self._ignored_modules):
                return None
            if not self.config.analyse_fallback_blocks and is_from_fallback_block(
                importnode
            ):
                return None

            dotted_modname = get_import_name(importnode, modname)
            self.add_message("import-error", args=repr(dotted_modname), node=importnode)
        return None

    def _add_imported_module(
        self, node: Union[nodes.Import, nodes.ImportFrom], importedmodname: str
    ) -> None:
        """notify an imported module, used to analyze dependencies"""
        module_file = node.root().file
        context_name = node.root().name
        base = os.path.splitext(os.path.basename(module_file))[0]

        try:
            importedmodname = astroid.modutils.get_module_part(
                importedmodname, module_file
            )
        except ImportError:
            pass

        in_type_checking_block = isinstance(node.parent, nodes.If) and is_typing_guard(
            node.parent
        )

        if context_name == importedmodname:
            self.add_message("import-self", node=node)

        elif not astroid.modutils.is_standard_module(importedmodname):
            # if this is not a package __init__ module
            if base != "__init__" and context_name not in self._module_pkg:
                # record the module's parent, or the module itself if this is
                # a top level module, as the package it belongs to
                self._module_pkg[context_name] = context_name.rsplit(".", 1)[0]

            # handle dependencies
            dependencies_stat: Dict[str, Union[Set]] = self.linter.stats.dependencies
            importedmodnames = dependencies_stat.setdefault(importedmodname, set())
            if context_name not in importedmodnames:
                importedmodnames.add(context_name)

            # update import graph
            self.import_graph[context_name].add(importedmodname)
            if (
                not self.linter.is_message_enabled("cyclic-import", line=node.lineno)
                or in_type_checking_block
            ):
                self._excluded_edges[context_name].add(importedmodname)

    def _check_preferred_module(self, node, mod_path):
        """check if the module has a preferred replacement"""
        if mod_path in self.preferred_modules:
            self.add_message(
                "preferred-module",
                node=node,
                args=(self.preferred_modules[mod_path], mod_path),
            )

    def _check_import_as_rename(
        self, node: Union[nodes.Import, nodes.ImportFrom]
    ) -> None:
        names = node.names
        for name in names:
            if not all(name):
                return

            splitted_packages = name[0].rsplit(".", maxsplit=1)
            import_name = splitted_packages[-1]
            aliased_name = name[1]
            if import_name != aliased_name:
                continue

            if len(splitted_packages) == 1:
                self.add_message("useless-import-alias", node=node)
            elif len(splitted_packages) == 2:
                self.add_message(
                    "consider-using-from-import",
                    node=node,
                    args=(splitted_packages[0], import_name),
                )

    def _check_reimport(self, node, basename=None, level=None):
        """check if the import is necessary (i.e. not already done)"""
        if not self.linter.is_message_enabled("reimported"):
            return

        frame = node.frame()
        root = node.root()
        contexts = [(frame, level)]
        if root is not frame:
            contexts.append((root, None))

        for known_context, known_level in contexts:
            for name, alias in node.names:
                first = _get_first_import(
                    node, known_context, name, basename, known_level, alias
                )
                if first is not None:
                    self.add_message(
                        "reimported", node=node, args=(name, first.fromlineno)
                    )

    def _report_external_dependencies(self, sect, _, _dummy):
        """return a verbatim layout for displaying dependencies"""
        dep_info = _make_tree_defs(self._external_dependencies_info().items())
        if not dep_info:
            raise EmptyReportError()
        tree_str = _repr_tree_defs(dep_info)
        sect.append(VerbatimText(tree_str))

    def _report_dependencies_graph(self, sect, _, _dummy):
        """write dependencies as a dot (graphviz) file"""
        dep_info = self.linter.stats.dependencies
        if not dep_info or not (
            self.config.import_graph
            or self.config.ext_import_graph
            or self.config.int_import_graph
        ):
            raise EmptyReportError()
        filename = self.config.import_graph
        if filename:
            _make_graph(filename, dep_info, sect, "")
        filename = self.config.ext_import_graph
        if filename:
            _make_graph(filename, self._external_dependencies_info(), sect, "external ")
        filename = self.config.int_import_graph
        if filename:
            _make_graph(filename, self._internal_dependencies_info(), sect, "internal ")

    def _filter_dependencies_graph(self, internal):
        """build the internal or the external dependency graph"""
        graph = collections.defaultdict(set)
        for importee, importers in self.linter.stats.dependencies.items():
            for importer in importers:
                package = self._module_pkg.get(importer, importer)
                is_inside = importee.startswith(package)
                if is_inside and internal or not is_inside and not internal:
                    graph[importee].add(importer)
        return graph

    @astroid.decorators.cached
    def _external_dependencies_info(self):
        """return cached external dependencies information or build and
        cache them
        """
        return self._filter_dependencies_graph(internal=False)

    @astroid.decorators.cached
    def _internal_dependencies_info(self):
        """return cached internal dependencies information or build and
        cache them
        """
        return self._filter_dependencies_graph(internal=True)

    def _check_wildcard_imports(self, node, imported_module):
        if node.root().package:
            # Skip the check if in __init__.py issue #2026
            return

        wildcard_import_is_allowed = self._wildcard_import_is_allowed(imported_module)
        for name, _ in node.names:
            if name == "*" and not wildcard_import_is_allowed:
                self.add_message("wildcard-import", args=node.modname, node=node)

    def _wildcard_import_is_allowed(self, imported_module):
        return (
            self.config.allow_wildcard_with_all
            and imported_module is not None
            and "__all__" in imported_module.locals
        )

    def _check_toplevel(self, node):
        """Check whether the import is made outside the module toplevel."""
        # If the scope of the import is a module, then obviously it is
        # not outside the module toplevel.
        if isinstance(node.scope(), nodes.Module):
            return

        module_names = [
            f"{node.modname}.{name[0]}"
            if isinstance(node, nodes.ImportFrom)
            else name[0]
            for name in node.names
        ]

        # Get the full names of all the imports that are only allowed at the module level
        scoped_imports = [
            name for name in module_names if name not in self._allow_any_import_level
        ]

        if scoped_imports:
            self.add_message(
                "import-outside-toplevel", args=", ".join(scoped_imports), node=node
            )


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(ImportsChecker(linter))
