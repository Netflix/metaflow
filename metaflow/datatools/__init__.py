import sys
import types

ver = sys.version_info[0] * 10 + sys.version_info[1]

# Read an AWS source in a chunked manner.
# We read in chunks (at most 2GB -- here this is passed via max_chunk_size)
# because of https://bugs.python.org/issue42853 (Py3 bug); this also helps
# keep memory consumption lower
# NOTE: For some weird reason, if you pass a large value to
# read, it delays the call so we always pass it either what
# remains or 2GB, whichever is smallest.
def read_in_chunks(dst, src, src_sz, max_chunk_size):
    remaining = src_sz
    while remaining > 0:
        buf = src.read(min(remaining, max_chunk_size))
        # Py2 doesn't return the number of bytes written so calculate size
        # separately
        dst.write(buf)
        remaining -= len(buf)


from .s3 import MetaflowS3Exception, S3

# Import any additional datatools defined by a Metaflow extensions package
try:
    import metaflow_extensions.datatools as extension_module
except ImportError as e:
    if ver >= 36:
        # e.name is set to the name of the package that fails to load
        # so don't error ONLY IF the error is importing this module (but do
        # error if there is a transitive import error)
        if not (
            isinstance(e, ModuleNotFoundError)
            and e.name in ["metaflow_extensions", "metaflow_extensions.datatools"]
        ):
            print(
                "Cannot load metaflow_extensions exceptions -- "
                "if you want to ignore, uninstall metaflow_extensions package"
            )
            raise
else:
    # We load into globals whatever we have in extension_module
    # We specifically exclude any modules that may be included (like sys, os, etc)
    # *except* for ones that are part of metaflow_extensions (basically providing
    # an aliasing mechanism)
    lazy_load_custom_modules = {}
    addl_modules = extension_module.__dict__.get("__mf_promote_submodules__")
    if addl_modules:
        # We make an alias for these modules which the metaflow_extensions author
        # wants to expose but that may not be loaded yet
        lazy_load_custom_modules = {
            "metaflow.datatools.%s" % k: "metaflow_extensions.datatools.%s" % k
            for k in addl_modules
        }
    for n, o in extension_module.__dict__.items():
        if not n.startswith("__") and not isinstance(o, types.ModuleType):
            globals()[n] = o
        elif (
            isinstance(o, types.ModuleType)
            and o.__package__
            and o.__package__.startswith("metaflow_extensions")
        ):
            lazy_load_custom_modules["metaflow.datatools.%s" % n] = o
    if lazy_load_custom_modules:
        from metaflow import _LazyLoader

        sys.meta_path = [_LazyLoader(lazy_load_custom_modules)] + sys.meta_path
finally:
    # Erase all temporary names to avoid leaking things
    for _n in [
        "ver",
        "n",
        "o",
        "e",
        "lazy_load_custom_modules",
        "extension_module",
        "_LazyLoader",
        "addl_modules",
    ]:
        try:
            del globals()[_n]
        except KeyError:
            pass
    del globals()["_n"]
