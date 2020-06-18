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
from .datatools import DATACLIENTS
from .exception import MetaflowException
from .parameters import DeployTimeField, Parameter
from .util import to_unicode

try:
    # python2
    import cStringIO
    BytesIO = cStringIO.StringIO
except:
    # python3
    BytesIO = io.BytesIO

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
        client = DATACLIENTS.get(ctx.obj.datastore.TYPE)
        if client:
            return Uploader(client).store(self._path, self._is_text, self._encoding, ctx.obj.logger)
        raise MetaflowException("No client found for datastore type %s" % ctx.obj.datastore.TYPE)


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
            return LocalFile(self._is_text, self._encoding, value)(ctx)
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
        # We make any defaults DeployTimeFields if needed
        for k, v in kwargs.items():
            if k.startswith('default') and v is not None:
                _, file_type, _ = LocalFile.is_file_handled(v)
                # Ignore error because we may never use the default
                if file_type is None:
                    l = LocalFile(is_text, encoding, v)
                    kwargs[k] = DeployTimeField(name, str, k, l, print_representation=str(l))
                else:
                    kwargs[k] = DeployTimeField(
                        name,
                        str,
                        k,
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

    encodings = ['gzip+pickle-v2', 'gzip+pickle-v4']

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

    def store(self, path, is_text, encoding, logger):
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
            if is_text:
                cur_obj = TransformableObject(io.open(path, mode='rt', encoding=encoding).read())
            else:
                cur_obj = TransformableObject(io.open(path, mode='rb').read())
        except IOError:
            # If we get an error here, since we know that the file exists already,
            # it means that read failed which happens with Python 2.7 for large files
            raise MetaflowException('Cannot read file at %s -- this is likely because it is too '
                                    'large to be properly handled by Python 2.7' % path)
        url_type = Uploader.encodings[0]
        try:
            if pos > 2:
                url_type = Uploader.encodings[1]
                cur_obj.transform(lambda x: pickle.dumps(x, protocol=4))
            else:
                cur_obj.transform(lambda x: pickle.dumps(x, protocol=2))
        except (SystemError, OverflowError):
            raise MetaflowException('Large include files require Python 3.4 or newer for %s' % path)
        sha = sha1(cur_obj.current()).hexdigest()
        path = os.path.join(self._client_class.get_root_from_config(logger, True), sha)
        buf = BytesIO()
        with gzip.GzipFile(
                fileobj=buf, mode='wb', compresslevel=3) as f:
            f.write(cur_obj.current())
        cur_obj.transform(lambda _: buf)
        buf.seek(0)
        with self._client_class() as client:
            url = client.put(path, buf.getvalue(), overwrite=False)
            logger('File persisted at %s' % url)
            return Uploader.encode_url(url_type, url)

    def load(self, value):
        value_info = Uploader.decode_value(value)
        with self._client_class() as client:
            obj = client.get(value_info['url'], return_missing=True)
            if obj.exists:
                if value_info['type'] in Uploader.encodings:
                    # We saved this file directly so we know how to read it out
                    with gzip.GzipFile(filename=obj.path, mode='rb') as f:
                        return pickle.loads(f.read())
                else:
                    # We open this file according to the is_text and encoding information
                    if value_info['is_text']:
                        return io.open(obj.path, mode='rt', encoding=value_info.get('encoding')).read()
                    else:
                        return io.open(obj.path, mode='rb').read()
            raise FileNotFoundError("File at %s does not exist" % self._url)
