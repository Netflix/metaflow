import gzip

import io
import json
import os
import pickle
import tempfile

from hashlib import sha1
from shutil import move

import click

from .datastore.datastore import TransformableObject
from .exception import MetaflowException
from .metaflow_config import DATATOOLS_LOCALROOT, DATATOOLS_SUFFIX
from .parameters import context_proto, DeployTimeField, Parameter
from .util import to_unicode

try:
    # python2
    import cStringIO
    BytesIO = cStringIO.StringIO

    from urlparse import urlparse
except:
    # python3
    BytesIO = io.BytesIO

    from urllib.parse import urlparse

# TODO: This local "client" and the general notion of dataclients should probably
# be moved somewhere else. Putting here to keep this change compact for now
class MetaflowLocalURLException(MetaflowException):
    headline = 'Invalid path'

class MetaflowLocalNotFound(MetaflowException):
    headline = 'Local object not found'

class LocalObject(object):
    """
    This object represents a local object. It is a very thin wrapper
    to allow it to be used in the same way as the S3Object (only as needed
    in this usecase)

    Get or list calls return one or more of LocalObjects.
    """

    def __init__(self, url, path):

        # all fields of S3Object should return a unicode object
        def ensure_unicode(x):
            return None if x is None else to_unicode(x)
        path = ensure_unicode(path)

        self._path = path
        self._url = url

        if self._path:
            try:
                os.stat(self._path)
            except FileNotFoundError:
                self._path = None

    @property
    def exists(self):
        """
        Does this key correspond to an actual file?
        """
        return self._path is not None and os.path.isfile(self._path)

    @property
    def url(self):
        """
        Local location of the object; this is the path prefixed with local://
        """
        return self._url

    @property
    def path(self):
        """
        Path to the local file
        """
        return self._path


class Local(object):
    """
    This class allows you to access the local filesystem in a way similar to the S3 datatools
    client. It is a stripped down version for now and only implements the functionality needed
    for this use case.

    In the future, we may want to allow it to be used in a way similar to the S3() client.
    """

    @staticmethod
    def _makedirs(path):
        try:
            os.makedirs(path)
        except OSError as x:
            if x.errno == 17:
                return
            else:
                raise

    @classmethod
    def get_root_from_config(cls, echo, create_on_absent=True):
        result = DATATOOLS_LOCALROOT
        if result is None:
            from .datastore.local import LocalDataStore
            result = LocalDataStore.get_datastore_root_from_config(echo, create_on_absent)
            result = os.path.join(result, DATATOOLS_SUFFIX)
            if create_on_absent and not os.path.exists(result):
                os.mkdir(result)
        return result

    def __init__(self):
        """
        Initialize a new context for Local file operations. This object is based used as
        a context manager for a with statement.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _path(self, key):
        key = to_unicode(key)
        if key.startswith(u'local://'):
            return key[8:]
        elif key[0] != u'/':
            if current.is_running_flow:
                raise MetaflowLocalURLException(
                    "Specify Local(run=self) when you use Local inside a running "
                    "flow. Otherwise you have to use Local with full "
                    "local:// urls or absolute paths.")
            else:
                raise MetaflowLocalURLException(
                    "Initialize Local with an 'localroot' or 'run' if you don't "
                    "want to specify full local:// urls or absolute paths.")
        else:
            return key

    def get(self, key=None, return_missing=False):
        p = self._path(key)
        url = u'local://%s' % p
        if not os.path.isfile(p):
            if return_missing:
                p = None
            else:
                raise MetaflowLocalNotFound("Local URL %s not found" % url)
        return LocalObject(url, p)

    def put(self, key, obj, overwrite=True):
        p = self._path(key)
        if overwrite or (not os.path.exists(p)):
            Local._makedirs(os.path.dirname(p))
            with open(p, 'wb') as f:
                f.write(obj)
        return u'local://%s' % p


# From here on out, this is the IncludeFile implementation.
from .datatools import S3

DATACLIENTS = {'local': Local,
               's3': S3}

class LocalFile():
    def __init__(self, is_text, encoding, path):
        self._is_text = is_text
        self._encoding = encoding
        self._path = path

    @classmethod
    def is_file_handled(cls, path):
        if path:
            path = Uploader.decode_value(to_unicode(path))['url']
        for prefix, handler in DATACLIENTS.items():
            if path.startswith(u"%s://" % prefix):
                return True, Uploader(handler), None
        try:
            with open(path, mode='r') as _:
                pass
        except OSError:
            return False, None, "Could not open file '%s'" % path
        return True, None, None

    def __str__(self):
        return self._path

    def __repr__(self):
        return self._path

    def __call__(self, ctx):
        # We check again if this is a local file that exists. We do this here because
        # we always convert local files to DeployTimeFields irrespective of whether
        # the file exists.
        ok, _, err = LocalFile.is_file_handled(self._path)
        if not ok:
            raise MetaflowException("Cannot load file %s: %s" % (self._path, err))
        client = DATACLIENTS.get(ctx.ds_type)
        if client:
            return Uploader(client).store(
                ctx.flow_name, self._path, self._is_text, self._encoding, ctx.logger)
        raise MetaflowException("No client found for datastore type %s" % ctx.ds_type)


class FilePathClass(click.ParamType):
    name = 'FilePath'
    # The logic for this class is as follows:
    #  - It will always return a path that indicates the persisted path of the file.
    #    + If the value is already such a string, nothing happens and it returns that same value
    #    + If the value is a LocalFile, it will persist the local file and return the path
    #      of the persisted file
    #  - The artifact will be persisted prior to any run (for non-scheduled runs through persist_parameters)
    #    + This will therefore persist a simple string
    #  - When the parameter is loaded again, the load_parameter in the IncludeFile class will get called
    #    which will download and return the bytes of the persisted file.
    def __init__(self, is_text, encoding):
        self._is_text = is_text
        self._encoding = encoding

    def convert(self, value, param, ctx):
        value = os.path.expanduser(value)
        ok, file_type, err = LocalFile.is_file_handled(value)
        if not ok:
            self.fail(err)
        if file_type is None:
            # Here, we need to store the file
            param_ctx = context_proto._replace(parameter_name=self.parameter_name)
            return LocalFile(self._is_text, self._encoding, value)(param_ctx)
        else:
            # We will just store the URL in the datastore along with text/encoding info
            return Uploader.encode_url(
                'external', value, is_text=self._is_text, encoding=self._encoding)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'FilePath'


class IncludeFile(Parameter):

    def __init__(
            self, name, required=False, is_text=True, encoding=None, help=None, **kwargs):
        # Defaults are DeployTimeField
        v = kwargs.get('default')
        if v is not None:
            _, file_type, _ = LocalFile.is_file_handled(v)
            # Ignore error because we may never use the default
            if file_type is None:
                l = LocalFile(is_text, encoding, v)
                kwargs['default'] = DeployTimeField(name, str, 'default', l, print_representation=str(l))
            else:
                kwargs['default'] = DeployTimeField(
                    name,
                    str,
                    'default',
                    lambda _, is_text=is_text, encoding=encoding, v=v: Uploader.encode_url(
                        'external-default', v, is_text=is_text, encoding=encoding),
                    print_representation=v)

        super(IncludeFile, self).__init__(
            name, required=required, help=help,
            type=FilePathClass(is_text, encoding), **kwargs)

    def load_parameter(self, val):
        ok, file_type, err = LocalFile.is_file_handled(val)
        if not ok:
            raise MetaflowException("Parameter '%s' could not be loaded: %s" % (self.name, err))
        if file_type is None:
            raise MetaflowException("Parameter '%s' was not properly converted" % self.name)
        return file_type.load(val)


class Uploader():

    file_type = 'uploader-v1'

    def __init__(self, client_class):
        self._client_class = client_class

    @staticmethod
    def encode_url(url_type, url, **kwargs):
        # Avoid encoding twice (default -> URL -> _convert method of FilePath for example)
        if url is None or len(url) == 0 or url[0] == '{':
            return url
        return_value = {'type': url_type, 'url': url}
        return_value.update(kwargs)
        return json.dumps(return_value)

    @staticmethod
    def decode_value(value):
        if value is None or len(value) == 0 or value[0] != '{':
            return {'type': 'base', 'url': value}
        return json.loads(value)

    def store(self, flow_name, path, is_text, encoding, logger):
        sz = os.path.getsize(path)
        unit = ['B', 'KB', 'MB', 'GB', 'TB']
        pos = 0
        while pos < len(unit) and sz >= 1024:
            sz = sz // 1024
            pos += 1
        if pos >= 3:
            extra = '(this may take a while)'
        else:
            extra = ''
        logger(
            'Including file %s of size %d%s %s' % (path, sz, unit[pos], extra))
        try:
            cur_obj = TransformableObject(io.open(path, mode='rb').read())
        except IOError:
            # If we get an error here, since we know that the file exists already,
            # it means that read failed which happens with Python 2.7 for large files
            raise MetaflowException('Cannot read file at %s -- this is likely because it is too '
                                    'large to be properly handled by Python 2.7' % path)
        sha = sha1(cur_obj.current()).hexdigest()
        path = os.path.join(self._client_class.get_root_from_config(logger, True), flow_name, sha)
        buf = BytesIO()
        with gzip.GzipFile(
                fileobj=buf, mode='wb', compresslevel=3) as f:
            f.write(cur_obj.current())
        cur_obj.transform(lambda _: buf)
        buf.seek(0)
        with self._client_class() as client:
            url = client.put(path, buf.getvalue(), overwrite=False)
            logger('File persisted at %s' % url)
            return Uploader.encode_url(Uploader.file_type, url, is_text=is_text, encoding=encoding)

    def load(self, value):
        value_info = Uploader.decode_value(value)
        with self._client_class() as client:
            obj = client.get(value_info['url'], return_missing=True)
            if obj.exists:
                if value_info['type'] == Uploader.file_type:
                    # We saved this file directly so we know how to read it out
                    with gzip.GzipFile(filename=obj.path, mode='rb') as f:
                        if value_info['is_text']:
                            return io.TextIOWrapper(f, encoding=value_info.get('encoding')).read()
                        return f.read()
                else:
                    # We open this file according to the is_text and encoding information
                    if value_info['is_text']:
                        return io.open(obj.path, mode='rt', encoding=value_info.get('encoding')).read()
                    else:
                        return io.open(obj.path, mode='rb').read()
            raise FileNotFoundError("File at %s does not exist" % value_info['url'])
