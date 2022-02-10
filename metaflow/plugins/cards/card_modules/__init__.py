import os
import traceback
from .card import MetaflowCard, MetaflowCardComponent
from metaflow.extension_support import get_modules, EXT_PKG, _ext_debug

_CARD_MODULES = []


def iter_namespace(ns_pkg):
    # Specifying the second argument (prefix) to iter_modules makes the
    # returned name an absolute name instead of a relative one. This allows
    # import_module to work without having to do additional modification to
    # the name.
    import pkgutil

    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


def _get_external_card_packages():
    """
    Safely extract all external card modules
    Returns:
        `list` of `ModuleType`
    """
    import importlib

    # Caching card related modules.
    global _CARD_MODULES
    if len(_CARD_MODULES) > 0:
        return _CARD_MODULES
    for m in get_modules("plugins.cards"):
        # Iterate submodules of metaflow_extensions.X.plugins.cards
        # For example metaflow_extensions.X.plugins.cards.monitoring
        card_packages = []
        if getattr(m, "__path__", None):
            card_packages.append(m)
        for _, card_mod, ispkg_c in iter_namespace(m.module):
            try:
                if not ispkg_c:
                    continue
                cm = importlib.import_module(card_mod)
                _ext_debug("Importing card package %s" % card_mod)
                card_packages.append(cm)
            except Exception as e:
                _ext_debug(
                    "External Card Module Import Exception \n\n %s \n\n %s"
                    % (str(e), traceback.format_exc())
                )

        _CARD_MODULES.extend(card_packages)
    return _CARD_MODULES


def _load_external_cards():
    # Load external card packages
    card_packages = _get_external_card_packages()
    if not card_packages:
        return []
    external_cards = {}
    card_arr = []
    # Load cards from all external packages.
    for package in card_packages:
        try:
            cards = package.CARDS
            # Ensure that types match.
            if not type(cards) == list:
                continue
        except AttributeError:
            continue
        else:
            for c in cards:
                if not isinstance(c, type) or not issubclass(c, MetaflowCard):
                    # every card should only be inheriting a MetaflowCard
                    continue
                if not getattr(c, "type", None):
                    # todo Warn user of non existant `type` in MetaflowCard
                    continue
                if c.type in external_cards:
                    # todo Warn user of duplicate card
                    continue
                # external_cards[c.type] = c
                card_arr.append(c)
    return card_arr


MF_EXTERNAL_CARDS = _load_external_cards()
