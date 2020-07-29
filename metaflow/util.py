import os
import shutil
import sys
import tempfile
import zlib
import base64
from functools import wraps
from itertools import takewhile
import re

from metaflow.exception import MetaflowUnknownUser, MetaflowInternalError

try:
    # python2
    import cStringIO
    BytesIO = cStringIO.StringIO
    unicode_type = unicode
    bytes_type = str
    from urllib import quote, unquote

    # unquote_bytes should be a function that takes a urlencoded byte
    # string, encoded in UTF-8, url-decodes it and returns it as a
    # unicode object. Confusingly, how to accomplish this differs
    # between Python2 and Python3.
    #
    # Test with this input URL:
    # b'crazypath/%01%C3%B'
    # it should produce
    # u'crazypath/\x01\xff'
    def unquote_bytes(x):
        return to_unicode(unquote(to_bytes(x)))

except:
    # python3
    import io
    BytesIO = io.BytesIO
    unicode_type = str
    bytes_type = bytes
    from urllib.parse import quote, unquote

    def unquote_bytes(x):
        return unquote(to_unicode(x))


class TempDir(object):
    # Provide a temporary directory since Python 2.7 does not have it inbuilt
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)


def cached_property(getter):
    @wraps(getter)
    def exec_once(self):
        saved_name = '__%s' % getter.__name__
        if not hasattr(self, saved_name):
            setattr(self, saved_name, getter(self))
        return getattr(self, saved_name)
    return property(exec_once)


def all_equal(it):
    """
    Return True if all elements of the given iterator are equal.
    """
    it = iter(it)
    try:
        first = next(it)
    except StopIteration:
        return True
    for x in it:
        if x != first:
            return False
    return True


def url_quote(url):
    """
    Encode a unicode URL to a safe byte string
    """
    # quote() works reliably only with (byte)strings in Python2,
    # hence we need to .encode('utf-8') first. To see by yourself,
    # try quote(u'\xff') in python2. Python3 converts the output
    # always to Unicode, hence we need the outer to_bytes() too.
    #
    # We mark colon as a safe character to keep simple ASCII urls
    # nice looking, e.g. "http://google.com"
    return to_bytes(quote(to_bytes(url), safe='/:'))

def url_unquote(url_bytes):
    """
    Decode a byte string encoded with url_quote to a unicode URL
    """
    return unquote_bytes(url_bytes)

def is_stringish(x):
    """
    Returns true if the object is a unicode or a bytes object
    """
    return isinstance(x, bytes_type) or isinstance(x, unicode_type)


def to_fileobj(x):
    """
    Convert any string-line object to a byte-returning fileobj
    """
    return BytesIO(to_bytes(x))


def to_unicode(x):
    """
    Convert any object to a unicode object
    """
    if isinstance(x, bytes_type):
        return x.decode('utf-8')
    else:
        return unicode_type(x)


def to_bytes(x):
    """
    Convert any object to a byte string
    """
    if isinstance(x, unicode_type):
        return x.encode('utf-8')
    elif isinstance(x, bytes_type):
        return x
    elif isinstance(x, float):
        return repr(x).encode('utf-8')
    else:
        return str(x).encode('utf-8')


def get_username():
    """
    Return the name of the current user, or None if the current user
    could not be determined.
    """
    # note: the order of the list matters
    ENVVARS = ['METAFLOW_USER', 'SUDO_USER', 'USERNAME', 'USER']
    for var in ENVVARS:
        user = os.environ.get(var)
        if user and user != 'root':
            return user
    return None


def resolve_identity():
    prod_token = os.environ.get('METAFLOW_PRODUCTION_TOKEN')
    if prod_token:
        return 'production:%s' % prod_token
    user = get_username()
    if user and user != 'root':
        return 'user:%s' % user
    else:
        raise MetaflowUnknownUser()


def get_latest_run_id(echo, flow_name):
    from metaflow.datastore.local import LocalDataStore
    local_root = LocalDataStore.datastore_root
    if local_root is None:
        v = LocalDataStore.get_datastore_root_from_config(echo, create_on_absent=False)
        LocalDataStore.datastore_root = local_root = v
    if local_root:
        path = os.path.join(local_root, flow_name, 'latest_run')
        if os.path.exists(path):
            with open(path) as f:
                return f.read()
    return None


def write_latest_run_id(obj, run_id):
    from metaflow.datastore.local import LocalDataStore
    if LocalDataStore.datastore_root is None:
        LocalDataStore.datastore_root = LocalDataStore.get_datastore_root_from_config(obj.echo)
    path = os.path.join(LocalDataStore.datastore_root, obj.flow.name)
    try:
        os.makedirs(path)
    except OSError as x:
        if x.errno != 17:
            # Directories exists in other casewhich is fine
            raise
    with open(os.path.join(path, 'latest_run'), 'w') as f:
        f.write(str(run_id))


def get_object_package_version(obj):
    """    
    Return the top level package name and package version that defines the
    class of the given object.
    """
    try:
        module_name = obj.__class__.__module__

        if '.' in module_name:
            top_package_name = module_name.split('.')[0]
        else:
            top_package_name = module_name

    except AttributeError:
        return None, None

    try:
        top_package_version = sys.modules[top_package_name].__version__
        return top_package_name, top_package_version

    except AttributeError:
        return top_package_name, None


def compress_list(lst,
                  separator=',',
                  rangedelim=':',
                  zlibmarker='!',
                  zlibmin=500):

    bad_items = [x for x in lst
                 if separator in x or rangedelim in x or zlibmarker in x]
    if bad_items:
        raise MetaflowInternalError("Item '%s' includes a delimiter character "
                                    "so it can't be compressed" % bad_items[0])
    # Three output modes:
    lcp = longest_common_prefix(lst)
    if len(lst) < 2 or not lcp:
        # 1. Just a comma-separated list
        res = separator.join(lst)
    else:
        # 2. Prefix and a comma-separated list of suffixes
        lcplen = len(lcp)
        residuals = [e[lcplen:] for e in lst]
        res = rangedelim.join((lcp, separator.join(residuals)))
    if len(res) < zlibmin:
        return res
    else:
        # 3. zlib-compressed, base64-encoded, prefix-encoded list

        # interestingly, a typical zlib-encoded list of suffixes
        # has plenty of redundancy. Decoding the data *twice* helps a
        # lot
        compressed = zlib.compress(zlib.compress(to_bytes(res)))
        return zlibmarker + base64.b64encode(compressed).decode('utf-8')

def decompress_list(lststr, separator=',', rangedelim=':', zlibmarker='!'):
    # Three input modes:
    if lststr[0] == zlibmarker:
        # 3. zlib-compressed, base64-encoded
        lstbytes = base64.b64decode(lststr[1:])
        decoded = zlib.decompress(zlib.decompress(lstbytes)).decode('utf-8')
    else:
        decoded = lststr

    if rangedelim in decoded:
        prefix, suffixes = decoded.split(rangedelim)
        # 2. Prefix and a comma-separated list of suffixes
        return [prefix + suffix for suffix in suffixes.split(separator)]
    else:
        # 1. Just a comma-separated list
        return decoded.split(separator)


def longest_common_prefix(lst):
    if lst:
        return ''.join(a for a, _ in takewhile(lambda t: t[0] == t[1],
                                               zip(min(lst), max(lst))))
    else:
        return ''


def get_metaflow_root():
    return os.path.dirname(os.path.dirname(__file__))

def dict_to_cli_options(params):
    for k, v in params.items():
        if v:
            # we need special handling for 'with' since it is a reserved
            # keyword in Python, so we call it 'decospecs' in click args
            if k == 'decospecs':
                k = 'with'
            k = k.replace('_', '-')
            if not isinstance(v, tuple):
                v = [v]
            for value in v:
                yield '--%s' % k
                if not isinstance(value, bool):
                    value = to_unicode(value)
                    if ' ' in value:
                        yield '\'%s\'' % value
                    else:
                        yield value


# This function is imported from https://github.com/cookiecutter/whichcraft
def which(cmd, mode=os.F_OK | os.X_OK, path=None):
    """Given a command, mode, and a PATH string, return the path which
    conforms to the given mode on the PATH, or None if there is no such
    file.
    `mode` defaults to os.F_OK | os.X_OK. `path` defaults to the result
    of os.environ.get("PATH"), or can be overridden with a custom search
    path.
    Note: This function was backported from the Python 3 source code.
    """
    # Check that a given file can be accessed with the correct mode.
    # Additionally check that `file` is not a directory, as on Windows
    # directories pass the os.access check.
    try:  # Forced testing
        from shutil import which as w
        return w(cmd, mode, path)
    except ImportError: 
        def _access_check(fn, mode):
            return os.path.exists(fn) and os.access(fn, mode) and not os.path.isdir(fn)

        # If we're given a path with a directory part, look it up directly
        # rather than referring to PATH directories. This includes checking
        # relative to the current directory, e.g. ./script
        if os.path.dirname(cmd):
            if _access_check(cmd, mode):
                return cmd
            return None

        if path is None:
            path = os.environ.get("PATH", os.defpath)
        if not path:
            return None

        path = path.split(os.pathsep)

        files = [cmd]
        seen = set()
        for dir in path:
            normdir = os.path.normcase(dir)
            if normdir not in seen:
                seen.add(normdir)
                for thefile in files:
                    name = os.path.join(dir, thefile)
                    if _access_check(name, mode):
                        return name

        return None


def to_pascalcase(obj):
    """
    Convert all keys of a json to pascal case.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        res = obj.__class__()
        for k in obj:
            res[re.sub('([a-zA-Z])', 
                lambda x: x.groups()[0].upper(), k, 1)] = \
                to_pascalcase(obj[k])
    elif isinstance(obj, (list, set, tuple)):
        res = obj.__class__(to_pascalcase(v) for v in obj)
    else:
        return obj
    return res
