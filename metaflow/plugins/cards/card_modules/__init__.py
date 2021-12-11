import sys
import pkgutil
import importlib
import re

import traceback

# todo : create common import for this later.

from ..exception import (
    IncorrectCardModuleAttributeTypeException,
    TYPE_CHECK_REGEX,
    CARD_ID_PATTERN,
)
from .card import MetaflowCard, MetaflowCardComponent


# Code from https://packaging.python.org/guides/creating-and-discovering-plugins/#using-namespace-packages
def iter_namespace(ns_pkg):
    import pkgutil

    # Specifying the second argument (prefix) to iter_modules makes the
    # returned name an absolute name instead of a relative one. This allows
    # import_module to work without having to do additional modification to
    # the name.
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


# Loading modules related to Metaflow Cards here
MF_CARDS_EXTERNAL_MODULES = []
try:
    import metaflow_cards as _card_modules
except ImportError as e:
    ver = sys.version_info[0] * 10 + sys.version_info[1]
    if ver >= 36:
        # e.name is set to the name of the package that fails to load
        # so don't error ONLY IF the error is importing this module (but do
        # error if there is a transitive import error)
        if not (isinstance(e, ModuleNotFoundError) and e.name == "metaflow_cards"):
            print(
                "Cannot load metaflow_cards -- "
                "if you want to ignore, uninstall metaflow_cards package"
            )
            raise
else:
    all_cards = []
    for finder, name, ispkg in iter_namespace(_card_modules):
        # todo : find a better way to handle errors over here
        # In the case of an import failure of a certain module,
        # we ignore the import and just loudly print the errors.
        # TODO :
        # Should this have an associated variable like METAFLOW_DEBUG_CARDS
        # to print the exit message?
        try:
            card_module = importlib.import_module(name)
        except:
            stack_trace = traceback.format_exc()
            print("Ignoring module %s due to error in importing." % (name))
            continue
        try:
            # Register the cards here
            # Inside metaflow_cards.custom_package.__init__ add
            # from .some_card_module import SomeCard
            # CARDS = [SomeCard]
            if not hasattr(card_module, "CARDS"):
                raise AttributeError()
            if not isinstance(card_module.CARDS, list):
                raise IncorrectCardModuleAttributeTypeException(name)
            # todo: Check if types need to be validated;
            # todo : check if the registrations are happening in a clean way
            all_cards.extend(card_module.CARDS)
        except AttributeError as e:
            print(
                "Ignoring import of module %s since "
                "it lacks an associated CARDS attribute." % (name)
            )
        except IncorrectCardModuleAttributeTypeException as e:
            print(e.headline + "\n\t" + e.message)

    for card in all_cards:

        if not hasattr(card, "type") or card.type is None:
            print(
                "Ignoring import of module %s since "
                "it lacks an associated `type` property "
                "or the property is set as None." % (card.__name__)
            )
            continue
        regex_match = re.match(CARD_ID_PATTERN, card.type)
        if regex_match is None:
            print(
                "Ignoring import of MetaflowCard %s since "
                "the `type` doesn't follow regex patterns. MetaflowCard.type "
                "should follow this regex pattern %s" % (card.type, TYPE_CHECK_REGEX)
            )
            continue
        MF_CARDS_EXTERNAL_MODULES.append(card)
