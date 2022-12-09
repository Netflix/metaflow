# Read an AWS source in a chunked manner.
# We read in chunks (at most 2GB -- here this is passed via max_chunk_size)
# because of https://bugs.python.org/issue42853 (Py3 bug); this also helps
# keep memory consumption lower
# NOTE: For some weird reason, if you pass a large value to
# read it delays the call, so we always pass it either what
# remains or 2GB, whichever is smallest.
def read_in_chunks(dst, src, src_sz, max_chunk_size):
    remaining = src_sz
    while remaining > 0:
        buf = src.read(min(remaining, max_chunk_size))
        # Py2 doesn't return the number of bytes written so calculate size
        # separately
        dst.write(buf)
        remaining -= len(buf)


from .local import MetaflowLocalNotFound, MetaflowLocalURLException, Local
from .s3 import MetaflowS3Exception, S3

# Import any additional datatools defined by a Metaflow extensions package
try:
    from metaflow.extension_support import get_modules, multiload_all

    multiload_all(get_modules("plugins.datatools"), "plugins.datatools", globals())
finally:
    # Erase all temporary names to avoid leaking things
    for _n in ["get_modules", "multiload_all"]:
        try:
            del globals()[_n]
        except KeyError:
            pass
    del globals()["_n"]
