# Copyright (c) 2009-2010, 2012-2013 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2013-2014 Google, Inc.
# Copyright (c) 2014 Michal Nowikowski <godfryd@gmail.com>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2020-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Interfaces for Pylint objects"""
from collections import namedtuple
from typing import TYPE_CHECKING, Tuple

from metaflow._vendor.astroid import nodes

if TYPE_CHECKING:
    from metaflow._vendor.pylint.reporters.ureports.nodes import Section

Confidence = namedtuple("Confidence", ["name", "description"])
# Warning Certainties
HIGH = Confidence("HIGH", "No false positive possible.")
INFERENCE = Confidence("INFERENCE", "Warning based on inference result.")
INFERENCE_FAILURE = Confidence(
    "INFERENCE_FAILURE", "Warning based on inference with failures."
)
UNDEFINED = Confidence("UNDEFINED", "Warning without any associated confidence level.")

CONFIDENCE_LEVELS = [HIGH, INFERENCE, INFERENCE_FAILURE, UNDEFINED]


class Interface:
    """Base class for interfaces."""

    @classmethod
    def is_implemented_by(cls, instance):
        return implements(instance, cls)


def implements(obj: "Interface", interface: Tuple[type, type]) -> bool:
    """Return whether the given object (maybe an instance or class) implements
    the interface.
    """
    kimplements = getattr(obj, "__implements__", ())
    if not isinstance(kimplements, (list, tuple)):
        kimplements = (kimplements,)
    return any(issubclass(i, interface) for i in kimplements)


class IChecker(Interface):
    """This is a base interface, not designed to be used elsewhere than for
    sub interfaces definition.
    """

    def open(self):
        """called before visiting project (i.e set of modules)"""

    def close(self):
        """called after visiting project (i.e set of modules)"""


class IRawChecker(IChecker):
    """interface for checker which need to parse the raw file"""

    def process_module(self, node: nodes.Module) -> None:
        """process a module

        the module's content is accessible via astroid.stream
        """


class ITokenChecker(IChecker):
    """Interface for checkers that need access to the token list."""

    def process_tokens(self, tokens):
        """Process a module.

        tokens is a list of all source code tokens in the file.
        """


class IAstroidChecker(IChecker):
    """interface for checker which prefers receive events according to
    statement type
    """


class IReporter(Interface):
    """reporter collect messages and display results encapsulated in a layout"""

    def handle_message(self, msg) -> None:
        """Handle the given message object."""

    def display_reports(self, layout: "Section") -> None:
        """display results encapsulated in the layout tree"""


__all__ = ("IRawChecker", "IAstroidChecker", "ITokenChecker", "IReporter", "IChecker")
