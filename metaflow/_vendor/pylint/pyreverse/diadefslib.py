# Copyright (c) 2006, 2008-2010, 2013-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 bot <bot@noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""handle diagram generation options for class diagram or default diagrams
"""

from typing import Any, Optional

from metaflow._vendor import astroid
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.pyreverse.diagrams import ClassDiagram, PackageDiagram
from metaflow._vendor.pylint.pyreverse.inspector import Project
from metaflow._vendor.pylint.pyreverse.utils import LocalsVisitor

# diagram generators ##########################################################


class DiaDefGenerator:
    """handle diagram generation options"""

    def __init__(self, linker, handler):
        """common Diagram Handler initialization"""
        self.config = handler.config
        self._set_default_options()
        self.linker = linker
        self.classdiagram = None  # defined by subclasses

    def get_title(self, node):
        """get title for objects"""
        title = node.name
        if self.module_names:
            title = f"{node.root().name}.{title}"
        return title

    def _set_option(self, option):
        """activate some options if not explicitly deactivated"""
        # if we have a class diagram, we want more information by default;
        # so if the option is None, we return True
        if option is None:
            return bool(self.config.classes)
        return option

    def _set_default_options(self):
        """set different default options with _default dictionary"""
        self.module_names = self._set_option(self.config.module_names)
        all_ancestors = self._set_option(self.config.all_ancestors)
        all_associated = self._set_option(self.config.all_associated)
        anc_level, association_level = (0, 0)
        if all_ancestors:
            anc_level = -1
        if all_associated:
            association_level = -1
        if self.config.show_ancestors is not None:
            anc_level = self.config.show_ancestors
        if self.config.show_associated is not None:
            association_level = self.config.show_associated
        self.anc_level, self.association_level = anc_level, association_level

    def _get_levels(self):
        """help function for search levels"""
        return self.anc_level, self.association_level

    def show_node(self, node):
        """true if builtins and not show_builtins"""
        if self.config.show_builtin:
            return True
        return node.root().name != "builtins"

    def add_class(self, node):
        """visit one class and add it to diagram"""
        self.linker.visit(node)
        self.classdiagram.add_object(self.get_title(node), node)

    def get_ancestors(self, node, level):
        """return ancestor nodes of a class node"""
        if level == 0:
            return
        for ancestor in node.ancestors(recurs=False):
            if not self.show_node(ancestor):
                continue
            yield ancestor

    def get_associated(self, klass_node, level):
        """return associated nodes of a class node"""
        if level == 0:
            return
        for association_nodes in list(klass_node.instance_attrs_type.values()) + list(
            klass_node.locals_type.values()
        ):
            for node in association_nodes:
                if isinstance(node, astroid.Instance):
                    node = node._proxied
                if not (isinstance(node, nodes.ClassDef) and self.show_node(node)):
                    continue
                yield node

    def extract_classes(self, klass_node, anc_level, association_level):
        """extract recursively classes related to klass_node"""
        if self.classdiagram.has_node(klass_node) or not self.show_node(klass_node):
            return
        self.add_class(klass_node)

        for ancestor in self.get_ancestors(klass_node, anc_level):
            self.extract_classes(ancestor, anc_level - 1, association_level)

        for node in self.get_associated(klass_node, association_level):
            self.extract_classes(node, anc_level, association_level - 1)


class DefaultDiadefGenerator(LocalsVisitor, DiaDefGenerator):
    """generate minimum diagram definition for the project :

    * a package diagram including project's modules
    * a class diagram including project's classes
    """

    def __init__(self, linker, handler):
        DiaDefGenerator.__init__(self, linker, handler)
        LocalsVisitor.__init__(self)

    def visit_project(self, node: Project) -> None:
        """visit a pyreverse.utils.Project node

        create a diagram definition for packages
        """
        mode = self.config.mode
        if len(node.modules) > 1:
            self.pkgdiagram: Optional[PackageDiagram] = PackageDiagram(
                f"packages {node.name}", mode
            )
        else:
            self.pkgdiagram = None
        self.classdiagram = ClassDiagram(f"classes {node.name}", mode)

    def leave_project(self, _: Project) -> Any:
        """leave the pyreverse.utils.Project node

        return the generated diagram definition
        """
        if self.pkgdiagram:
            return self.pkgdiagram, self.classdiagram
        return (self.classdiagram,)

    def visit_module(self, node: nodes.Module) -> None:
        """visit an astroid.Module node

        add this class to the package diagram definition
        """
        if self.pkgdiagram:
            self.linker.visit(node)
            self.pkgdiagram.add_object(node.name, node)

    def visit_classdef(self, node: nodes.ClassDef) -> None:
        """visit an astroid.Class node

        add this class to the class diagram definition
        """
        anc_level, association_level = self._get_levels()
        self.extract_classes(node, anc_level, association_level)

    def visit_importfrom(self, node: nodes.ImportFrom) -> None:
        """visit astroid.ImportFrom  and catch modules for package diagram"""
        if self.pkgdiagram:
            self.pkgdiagram.add_from_depend(node, node.modname)


class ClassDiadefGenerator(DiaDefGenerator):
    """generate a class diagram definition including all classes related to a
    given class
    """

    def class_diagram(self, project, klass):
        """return a class diagram definition for the given klass and its
        related klasses
        """

        self.classdiagram = ClassDiagram(klass, self.config.mode)
        if len(project.modules) > 1:
            module, klass = klass.rsplit(".", 1)
            module = project.get_module(module)
        else:
            module = project.modules[0]
            klass = klass.split(".")[-1]
        klass = next(module.ilookup(klass))

        anc_level, association_level = self._get_levels()
        self.extract_classes(klass, anc_level, association_level)
        return self.classdiagram


# diagram handler #############################################################


class DiadefsHandler:
    """handle diagram definitions :

    get it from user (i.e. xml files) or generate them
    """

    def __init__(self, config):
        self.config = config

    def get_diadefs(self, project, linker):
        """Get the diagrams configuration data

        :param project:The pyreverse project
        :type project: pyreverse.utils.Project
        :param linker: The linker
        :type linker: pyreverse.inspector.Linker(IdGeneratorMixIn, LocalsVisitor)

        :returns: The list of diagram definitions
        :rtype: list(:class:`pylint.pyreverse.diagrams.ClassDiagram`)
        """

        #  read and interpret diagram definitions (Diadefs)
        diagrams = []
        generator = ClassDiadefGenerator(linker, self)
        for klass in self.config.classes:
            diagrams.append(generator.class_diagram(project, klass))
        if not diagrams:
            diagrams = DefaultDiadefGenerator(linker, self).visit(project)
        for diagram in diagrams:
            diagram.extract_relationships()
        return diagrams
