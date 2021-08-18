import sys
import pkgutil
import importlib


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
        if not (isinstance(e, ModuleNotFoundError) and \
                e.name == 'metaflow_cards'):
            print(
                "Cannot load metaflow_cards -- "
                "if you want to ignore, uninstall metaflow_cards package")
            raise
else:
    lazy_load_card_modules = {}
    for finder, name, ispkg in iter_namespace(_card_modules):
        card_module = importlib.import_module(name)
        try:
            # Register the rules here
            # Inside metaflow_cards.custom_package.__init__ add 
                # from .some_card_module import SomeCard 
                # CARDS = [SomeCard]
            assert card_module.CARDS is not None
            lazy_load_card_modules[name] = card_module.CARDS
            assert isinstance(card_module.CARDS,list)
            # todo: Check if types need to be validated; 
            # todo : check if the registrations are happening in a clean way
            MF_CARDS_EXTERNAL_MODULES.extend(card_module.CARDS)
            # 
        except AttributeError as e:
            print(
                "Ignoring import of module %s since "\
                "it lacks an associated CARDS attribute." % (name)
            )
        except AssertionError as e:
            print(
                "Ignoring import of module %s since the CARDS attribute "\
                "is not a `list`." % (name)
            )