import io
import os
import click

from metaflow.exception import MetaflowException
from metaflow.parameters import Parameter


class InternalFile():
    def __init__(self, logger, is_text, encoding, path):
        self._logger = logger
        self._is_text = is_text
        self._encoding = encoding
        self._path = path
        self._size = os.path.getsize(self._path)

    def __call__(self):
        unit = ['B', 'KB', 'MB', 'GB', 'TB']
        sz = self._size
        pos = 0
        while pos < len(unit) and sz >= 1024:
            sz = sz // 1024
            pos += 1
        if pos >= 3:
            extra = '(this may take a while)'
        else:
            extra = ''
        self._logger(
            'Including file %s of size %d%s %s' % (self._path, sz, unit[pos], extra))
        if self._is_text:
            return io.open(self._path, mode='rt', encoding=self._encoding).read()
        try:
            return io.open(self._path, mode='rb').read()
        except IOError:
            # If we get an error here, since we know that the file exists already,
            # it means that read failed which happens with Python 2.7 for large files
            raise MetaflowException('Cannot read file at %s -- this is likely because it is too '
                                    'large to be properly handled by Python 2.7' % self._path)

    def name(self):
        return self._path

    def size(self):
        return self._size


class FilePathClass(click.ParamType):
    name = 'FilePath'

    def __init__(self, is_text, encoding):
        self._is_text = is_text
        self._encoding = encoding

    def convert(self, value, param, ctx):
        try:
            with open(value, mode='r') as _:
                pass
        except OSError:
            self.fail("Could not open file '%s'" % value)

        return InternalFile(ctx.obj.logger, self._is_text, self._encoding, value)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'FilePath'


class IncludeFile(Parameter):

    def __init__(
            self, name, required=False, is_text=True, encoding=None, help=None, default=None):
        super(IncludeFile, self).__init__(
            name, required=required, help=help, default=default,
            type=FilePathClass(is_text, encoding))
