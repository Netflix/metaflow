import os
import sys
import time
import shutil
import random
import subprocess
from itertools import starmap
from tempfile import mkdtemp, NamedTemporaryFile

from .. import FlowSpec
from ..current import current
from ..metaflow_config import DATATOOLS_S3ROOT
from ..util import is_stringish,\
                   to_bytes,\
                   to_unicode,\
                   to_fileobj,\
                   url_quote,\
                   url_unquote
from ..exception import MetaflowException
from ..debug import debug
from . import s3op

try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse

from metaflow.datastore.util.s3util import get_s3_client

try:
    import boto3
    from botocore.exceptions import ClientError
    boto_found = True
except:
    boto_found = False

NUM_S3OP_RETRIES = 8

class MetaflowS3InvalidObject(MetaflowException):
    headline = 'Not a string-like object'

class MetaflowS3URLException(MetaflowException):
    headline = 'Invalid address'

class MetaflowS3Exception(MetaflowException):
    headline = 'S3 access failed'

class MetaflowS3NotFound(MetaflowException):
    headline = 'S3 object not found'

class MetaflowS3AccessDenied(MetaflowException):
    headline = 'S3 access denied'

class S3Object(object):
    """
    This object represents a path or an object in S3,
    with an optional local copy.

    Get or list calls return one or more of S3Objects.
    """

    def __init__(self, prefix, url, path, size=None):

        # all fields of S3Object should return a unicode object
        def ensure_unicode(x):
            return None if x is None else to_unicode(x)
        prefix, url, path = map(ensure_unicode, (prefix, url, path))

        self._size = size
        self._url = url
        self._path = path
        self._key = None

        if path:
            self._size = os.stat(self._path).st_size

        if prefix is None or prefix == url:
            self._key = url
            self._prefix = None
        else:
            self._key = url[len(prefix.rstrip('/')) + 1:].rstrip('/')
            self._prefix = prefix

    @property
    def exists(self):
        """
        Does this key correspond to an object in S3?
        """
        return self._size is not None

    @property
    def downloaded(self):
        """
        Has this object been downloaded?
        """
        return bool(self._path)

    @property
    def url(self):
        """
        S3 location of the object
        """
        return self._url

    @property
    def prefix(self):
        """
        Prefix requested that matches the object.
        """
        return self._prefix

    @property
    def key(self):
        """
        Key corresponds to the key given to the get call that produced
        this object. This may be a full S3 URL or a suffix based on what
        was requested.
        """
        return self._key

    @property
    def path(self):
        """
        Path to the local file corresponding to the object downloaded.
        This file gets deleted automatically when a S3 scope exits.

        Returns None if this S3Object has not been downloaded.
        """
        return self._path

    @property
    def blob(self):
        """
        Contents of the object as a byte string.

        Returns None if this S3Object has not been downloaded.
        """
        if self._path:
            with open(self._path, 'rb') as f:
                return f.read()

    @property
    def text(self):
        """
        Contents of the object as a Unicode string.

        Returns None if this S3Object has not been downloaded.
        """
        if self._path:
            return self.blob.decode('utf-8', errors='replace')

    @property
    def size(self):
        """
        Size of the object in bytes.

        Returns None if the key does not correspond to an object in S3.
        """
        return self._size

    def __str__(self):
        if self._path:
            return '<S3Object %s (%d bytes, local)>' % (self._url, self._size)
        elif self._size:
            return '<S3Object %s (%d bytes, in S3)>' % (self._url, self._size)
        else:
            return '<S3Object %s (object does not exist)>' % self._url

    def __repr__(self):
        return str(self)

class S3(object):

    @classmethod
    def get_root_from_config(cls, echo, create_on_absent=True):
        return DATATOOLS_S3ROOT

    def __init__(self,
                 tmproot='.',
                 bucket=None,
                 prefix=None,
                 run=None,
                 s3root=None):
        """
        Initialize a new context for S3 operations. This object is based used as
        a context manager for a with statement.

        There are two ways to initialize this object depending whether you want
        to bind paths to a Metaflow run or not.

        1. With a run object:

            run: (required) Either a FlowSpec object (typically 'self') or a
                 Run object corresponding to an existing Metaflow run. These
                 are used to add a version suffix in the S3 path.
            bucket: (optional) S3 bucket.
            prefix: (optional) S3 prefix.

        2. Without a run object:

            s3root: (optional) An S3 root URL for all operations. If this is
                    not specified, all operations require a full S3 URL.

        These options are supported in both the modes:

            tmproot: (optional) Root path for temporary files (default: '.')
        """

        if not boto_found:
            raise MetaflowException("You need to install 'boto3' in order to use S3.")

        if run:
            # 1. use a (current) run ID with optional customizations
            parsed = urlparse(DATATOOLS_S3ROOT)
            if not bucket:
                bucket = parsed.netloc
            if not prefix:
                prefix = parsed.path
            if isinstance(run, FlowSpec):
                if current.is_running_flow:
                    prefix = os.path.join(prefix,
                                          current.flow_name,
                                          current.run_id)
                else:
                    raise MetaflowS3URLException(\
                        "Initializing S3 with a FlowSpec outside of a running "
                        "flow is not supported.")
            else:
                prefix = os.path.join(prefix, run.parent.id, run.id)

            self._s3root = u's3://%s' % os.path.join(bucket, prefix.strip('/'))
        elif s3root:
            # 2. use an explicit S3 prefix
            parsed = urlparse(to_unicode(s3root))
            if parsed.scheme != 's3':
                raise MetaflowS3URLException(\
                    "s3root needs to be an S3 URL prefxied with s3://.")
            self._s3root = s3root.rstrip('/')
        else:
            # 3. use the client only with full URLs
            self._s3root = None

        self._tmpdir = mkdtemp(dir=tmproot, prefix='metaflow.s3.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        """
        Delete all temporary files downloaded in this context.
        """
        try:
            if not debug.s3client:
                shutil.rmtree(self._tmpdir)
        except:
            pass

    def _url(self, key):
        # NOTE: All URLs are handled as Unicode objects (unicde in py2,
        # string in py3) internally. We expect that all URLs passed to this
        # class as either Unicode or UTF-8 encoded byte strings. All URLs
        # returned are Unicode.
        if self._s3root is None:
            parsed = urlparse(to_unicode(key))
            if parsed.scheme == 's3' and parsed.path:
                return key
            else:
                if current.is_running_flow:
                    raise MetaflowS3URLException(\
                        "Specify S3(run=self) when you use S3 inside a running "
                        "flow. Otherwise you have to use S3 with full "
                        "s3:// urls.")
                else:
                    raise MetaflowS3URLException(\
                        "Initialize S3 with an 's3root' or 'run' if you don't "
                        "want to specify full s3:// urls.")
        elif key:
            if key.startswith('s3://'):
                raise MetaflowS3URLException(\
                    "Don't use absolute S3 URLs when the S3 client is "
                    "initialized with a prefix. URL: %s" % key)
            return os.path.join(self._s3root, key)
        else:
            return self._s3root

    def list_paths(self, keys=None):
        """
        List the next level of paths in S3. If multiple keys are
        specified, listings are done in parallel. The returned
        S3Objects have .exists == False if the url refers to a
        prefix, not an existing S3 object.

        Args:
            keys: (required) a list of suffixes for paths to list.

        Returns:
            a list of S3Objects (not downloaded)

        Example:

        Consider the following paths in S3:

        A/B/C
        D/E

        In this case, list_paths(['A', 'D']), returns ['A/B', 'D/E']. The
        first S3Object has .exists == False, since it does not refer to an
        object in S3. It is just a prefix.
        """
        def _list(keys):
            if keys is None:
                keys = [None]
            urls = (self._url(key).rstrip('/') + '/' for key in keys)
            res = self._read_many_files('list', urls)
            for s3prefix, s3url, size in res:
                if size:
                    yield s3prefix, s3url, None, int(size)
                else:
                    yield s3prefix, s3url, None, None

        return list(starmap(S3Object, _list(keys)))

    def list_recursive(self, keys=None):
        """
        List objects in S3 recursively. If multiple keys are
        specified, listings are done in parallel. The returned
        S3Objects have always .exists == True, since they refer
        to existing objects in S3.

        Args:
            keys: (required) a list of suffixes for paths to list.

        Returns:
            a list of S3Objects (not downloaded)

        Example:

        Consider the following paths in S3:

        A/B/C
        D/E

        In this case, list_recursive(['A', 'D']), returns ['A/B/C', 'D/E'].
        """
        def _list(keys):
            if keys is None:
                keys = [None]
            res = self._read_many_files('list',
                                        map(self._url, keys),
                                        recursive=True)
            for s3prefix, s3url, size in res:
                yield s3prefix, s3url, None, int(size)
        return list(starmap(S3Object, _list(keys)))

    def get(self, key=None, return_missing=False):
        """
        Get a single object from S3.

        Args:
            key: (optional) a suffix identifying the object.
            return_missing: (optional, default False) if set to True, do
                            not raise an exception for a missing key but
                            return it as an S3Object with .exists == False.

        Returns:
            an S3Object corresponding to the object requested.
        """
        url = self._url(key)
        src = urlparse(url)

        def _download(s3, tmp):
            s3.download_file(src.netloc, src.path.lstrip('/'), tmp)
            return url

        try:
            path = self._one_boto_op(_download, url)
        except MetaflowS3NotFound:
            if return_missing:
                path = None
            else:
                raise

        return S3Object(self._s3root, url, path)

    def get_many(self, keys, return_missing=False):
        """
        Get many objects from S3 in parallel.

        Args:
            keys: (required) a list of suffixes identifying the objects.
            return_missing: (optional, default False) if set to True, do
                            not raise an exception for a missing key but
                            return it as an S3Object with .exists == False.

        Returns:
            a list of S3Objects corresponding to the objects requested.
        """
        def _get():
            res = self._read_many_files('get',
                                        map(self._url, keys),
                                        allow_missing=return_missing,
                                        verify=True,
                                        verbose=False,
                                        listing=True)

            for s3prefix, s3url, fname in res:
                if fname:
                    yield self._s3root, s3url, os.path.join(self._tmpdir, fname)
                else:
                    # missing entries per return_missing=True
                    yield self._s3root, s3prefix, None, None
        return list(starmap(S3Object, _get()))

    def get_recursive(self, keys):
        """
        Get many objects from S3 recursively in parallel.

        Args:
            keys: (required) a list of suffixes for paths to download
                  recursively.

        Returns:
            a list of S3Objects corresponding to the objects requested.
        """
        def _get():
            res = self._read_many_files('get',
                                        map(self._url, keys),
                                        recursive=True,
                                        verify=True,
                                        verbose=False,
                                        listing=True)

            for s3prefix, s3url, fname in res:
                yield s3prefix, s3url, os.path.join(self._tmpdir, fname)
        return list(starmap(S3Object, _get()))

    def get_all(self):
        """
        Get all objects from S3 recursively (in parallel). This request
        only works if S3 is initialized with a run or a s3root prefix.

        Returns:
            a list of S3Objects corresponding to the objects requested.
        """
        if self._s3root is None:
            raise MetaflowS3URLException(\
                "Can't get_all() when S3 is initialized without a prefix")
        else:
            return self.get_recursive([None])

    def put(self, key, obj, overwrite=True):
        """
        Put an object to S3.

        Args:
            key:       (required) suffix for the object.
            obj:       (required) a bytes, string, or a unicode object to 
                       be stored in S3.
            overwrite: (optional) overwrites the key with obj, if it exists

        Returns:
            an S3 URL corresponding to the object stored.
        """

        if not is_stringish(obj):
            raise MetaflowS3InvalidObject(\
                "Object corresponding to the key '%s' is not a string "
                "or a bytes object." % key)

        url = self._url(key)
        src = urlparse(url)

        def _upload(s3, tmp):
            # we need to recreate the StringIO object for retries since
            # apparently upload_fileobj will/may close() it
            blob = to_fileobj(obj)
            s3.upload_fileobj(blob, src.netloc, src.path.lstrip('/'))

        if overwrite:
            self._one_boto_op(_upload, url)
            return url
        else:
            def _head(s3, tmp):
                s3.head_object(Bucket=src.netloc, Key=src.path.lstrip('/'))

            try:
                self._one_boto_op(_head, url)
            except MetaflowS3NotFound as err:
                self._one_boto_op(_upload, url)    
            return url

    def put_many(self, key_objs, overwrite=True):
        """
        Put objects to S3 in parallel.

        Args:
            key_objs:  (required) an iterator of (key, value) tuples. Value must
                       be a string, bytes, or a unicode object.
            overwrite: (optional) overwrites the key with obj, if it exists

        Returns:
            a list of (key, S3 URL) tuples corresponding to the files sent.
        """
        def _store():
            for key, obj in key_objs:
                if is_stringish(obj):
                    with NamedTemporaryFile(dir=self._tmpdir,
                                            delete=False,
                                            mode='wb',
                                            prefix='metaflow.s3.put_many.') as tmp:
                        tmp.write(to_bytes(obj))
                        tmp.close()
                        yield tmp.name, self._url(key), key
                else:
                    raise MetaflowS3InvalidObject(
                        "Object corresponding to the key '%s' is not a string "
                        "or a bytes object." % key)

        return self._put_many_files(_store(), overwrite)


    def put_files(self, key_paths, overwrite=True):
        """
        Put files to S3 in parallel.

        Args:
            key_paths: (required) an iterator of (key, path) tuples.
            overwrite: (optional) overwrites the key with obj, if it exists

        Returns:
            a list of (key, S3 URL) tuples corresponding to the files sent.
        """
        def _check():
            for key, path in key_paths:
                if not os.path.exists(path):
                    raise MetaflowS3NotFound("Local file not found: %s" % path)
                yield path, self._url(key), key

        return self._put_many_files(_check(), overwrite)

    def _one_boto_op(self, op, url):
        error = ''
        for i in range(NUM_S3OP_RETRIES):
            tmp = NamedTemporaryFile(dir=self._tmpdir,
                                     prefix='metaflow.s3.one_file.',
                                     delete=False)
            try:
                s3, _ = get_s3_client()
                op(s3, tmp.name)
                return tmp.name
            except ClientError as err:
                error_code = s3op.normalize_client_error(err)
                if error_code == 404:
                    raise MetaflowS3NotFound(url)
                elif error_code == 403:
                    raise MetaflowS3AccessDenied(url)
                elif error_code == 'NoSuchBucket':
                    raise MetaflowS3URLException("Specified S3 bucket doesn't exist.")
                error = str(err)
            except Exception as ex:
                # TODO specific error message for out of disk space
                error = str(ex)
            os.unlink(tmp.name)
            # add some jitter to make sure retries are not synchronized
            time.sleep(2**i + random.randint(0, 10))
        raise MetaflowS3Exception("S3 operation failed.\n"\
                                  "Key requested: %s\n"\
                                  "Error: %s" % (url, error))

    # NOTE: re: _read_many_files and _put_many_files
    # All file IO is through binary files - we write bytes, we read
    # bytes. All inputs and outputs from these functions are Unicode.
    # Conversion between bytes and unicode is done through url_quote
    # and url_unquote.
    def _read_many_files(self, op, prefixes, **options):
        prefixes = list(prefixes)
        with NamedTemporaryFile(dir=self._tmpdir,
                                mode='wb',
                                delete=not debug.s3client,
                                prefix='metaflow.s3.inputs.') as inputfile:
            inputfile.write(b'\n'.join(map(url_quote, prefixes)))
            inputfile.flush()
            stdout, stderr = self._s3op_with_retries(op,
                                                     inputs=inputfile.name,
                                                     **options)
            if stderr:
                raise MetaflowS3Exception("Getting S3 files failed.\n"\
                                          "First prefix requested: %s\n"\
                                          "Error: %s" % (prefixes[0], stderr))
            else:
                for line in stdout.splitlines():
                    yield tuple(map(url_unquote, line.strip(b'\n').split(b' ')))

    def _put_many_files(self, url_files, overwrite):
        url_files = list(url_files)
        with NamedTemporaryFile(dir=self._tmpdir,
                                mode='wb',
                                delete=not debug.s3client,
                                prefix='metaflow.s3.put_inputs.') as inputfile:
            lines = (b' '.join(map(url_quote, (os.path.realpath(local), url)))
                     for local, url, _ in url_files)
            inputfile.write(b'\n'.join(lines))
            inputfile.flush()
            stdout, stderr = self._s3op_with_retries('put',
                                                filelist=inputfile.name,
                                                verbose=False,
                                                overwrite=overwrite,
                                                listing=True)
            if stderr:
                raise MetaflowS3Exception("Uploading S3 files failed.\n"\
                                          "First key: %s\n"\
                                          "Error: %s" % (url_files[0][2],
                                                         stderr))
            else:
                urls = set()
                for line in stdout.splitlines():
                    url, _, _ = map(url_unquote, line.strip(b'\n').split(b' '))
                    urls.add(url)
                return [(key, url) for _, url, key in url_files if url in urls]

    def _s3op_with_retries(self, mode, **options):

        cmdline = [sys.executable, os.path.abspath(s3op.__file__), mode]
        for key, value in options.items():
            key = key.replace('_', '-')
            if isinstance(value, bool):
                if value:
                    cmdline.append('--%s' % key)
                else:
                    cmdline.append('--no-%s' % key)
            else:
                cmdline.extend(('--%s' % key, value))

        for i in range(NUM_S3OP_RETRIES):
            with NamedTemporaryFile(dir=self._tmpdir,
                                    mode='wb+',
                                    delete=not debug.s3client,
                                    prefix='metaflow.s3op.stderr') as stderr:
                try:
                    debug.s3client_exec(cmdline)
                    stdout = subprocess.check_output(cmdline,
                                                     cwd=self._tmpdir,
                                                     stderr=stderr.file)
                    return stdout, None
                except subprocess.CalledProcessError as ex:
                    stderr.seek(0)
                    err_out = stderr.read().decode('utf-8', errors='replace')
                    stderr.seek(0)
                    if ex.returncode == s3op.ERROR_URL_NOT_FOUND:
                        raise MetaflowS3NotFound(err_out)
                    elif ex.returncode == s3op.ERROR_URL_ACCESS_DENIED:
                        raise MetaflowS3AccessDenied(err_out)
                    time.sleep(2**i + random.randint(0, 10))

        return None, err_out
