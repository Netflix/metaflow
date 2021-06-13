import sys
import types

from .s3 import MetaflowS3Exception, S3

# Import any additional datatools defined by a Metaflow custom package
try:
    import metaflow_custom.datatools as extension_module
except ImportError as e:
    ver = sys.version_info[0] * 10 + sys.version_info[1]
    if ver >= 36:
        # e.name is set to the name of the package that fails to load
        # so don't error ONLY IF the error is importing this module (but do
        # error if there is a transitive import error)
        if not (isinstance(e, ModuleNotFoundError) and \
                e.name in ['metaflow_custom', 'metaflow_custom.datatools']):
            print(
                "Cannot load metaflow_custom exceptions -- "
                "if you want to ignore, uninstall metaflow_custom package")
            raise
else:
    # We load into globals whatever we have in extension_module
    # We specifically exclude any modules that may be included (like sys, os, etc)
    # *except* for ones that are part of metaflow_custom (basically providing
    # an aliasing mechanism)
    lazy_load_custom_modules = {}
    addl_modules = extension_module.__dict__.get('__mf_promote_submodules__')
    if addl_modules:
        # We make an alias for these modules which the metaflow_custom author
        # wants to expose but that may not be loaded yet
        lazy_load_custom_modules = {
            'metaflow.datatools.%s' % k: 'metaflow_custom.datatools.%s' % k
            for k in addl_modules}
    for n, o in extension_module.__dict__.items():
        if not n.startswith('__') and not isinstance(o, types.ModuleType):
            globals()[n] = o
        elif isinstance(o, types.ModuleType) and o.__package__ and \
                o.__package__.startswith('metaflow_custom'):
            lazy_load_custom_modules['metaflow.datatools.%s' % n] = o
    if lazy_load_custom_modules:
        from metaflow import _LazyLoader
        sys.meta_path = [_LazyLoader(lazy_load_custom_modules)] + sys.meta_path
finally:
    # Erase all temporary names to avoid leaking things
    for _n in ['ver', 'n', 'o', 'e', 'lazy_load_custom_modules',
               'extension_module', '_LazyLoader', 'addl_modules']:
        try:
            del globals()[_n]
        except KeyError:
            pass
    del globals()['_n']
