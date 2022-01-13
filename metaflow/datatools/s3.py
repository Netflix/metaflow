import json
import os
import sys
import time
import shutil
import random
import subprocess
from io import RawIOBase, BytesIO, BufferedIOBase
from itertools import chain, starmap
from tempfile import mkdtemp, NamedTemporaryFile

from .. import FlowSpec
from ..current import current
from ..metaflow_config import DATATOOLS_S3ROOT, S3_RETRY_COUNT
from ..util import (
    namedtuple_with_defaults,
    is_stringish,
    to_bytes,
    to_unicode,
    to_fileobj,
    url_quote,
    url_unquote,
)
from ..exception import MetaflowException
from ..debug import debug

try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse

from .s3util import get_s3_client, read_in_chunks, get_timestamp

try:
    import boto3
    from boto3.s3.transfer import TransferConfig

    DOWNLOAD_FILE_THRESHOLD = 2 * TransferConfig().multipart_threshold
    DOWNLOAD_MAX_CHUNK = 2 * 1024 * 1024 * 1024 - 1
    boto_found = True
except:
    boto_found = False


def ensure_unicode(x):
    return None if x is None else to_unicode(x)


S3GetObject = namedtuple_with_defaults("S3GetObject", "key offset length")

S3PutObject = namedtuple_with_defaults(
    "S3PutObject",
    "key value path content_type metadata",
    defaults=(None, None, None, None),
)

RangeInfo = namedtuple_with_defaults(
    "RangeInfo", "total_size request_offset request_length", defaults=(0, -1)
)


class MetaflowS3InvalidObject(MetaflowException):
    headline = "Not a string-like object"


class MetaflowS3URLException(MetaflowException):
    headline = "Invalid address"


class MetaflowS3Exception(MetaflowException):
    headline = "S3 access failed"


class MetaflowS3NotFound(MetaflowException):
    headline = "S3 object not found"


class MetaflowS3AccessDenied(MetaflowException):
    headline = "S3 access denied"


class S3Object(object):
    """
    This object represents a path or an object in S3,
    with an optional local copy.
    Get or list calls return one or more of S3Objects.
    """

    def __init__(
        self,
        prefix,
        url,
        path,
        size=None,
        content_type=None,
        metadata=None,
        range_info=None,
        last_modified=None,
    ):

        # all fields of S3Object should return a unicode object
        prefix, url, path = map(ensure_unicode, (prefix, url, path))

        self._size = size
        self._url = url
        self._path = path
        self._key = None
        self._content_type = content_type
        self._last_modified = last_modified

        self._metadata = None
        if metadata is not None and "metaflow-user-attributes" in metadata:
            self._metadata = json.loads(metadata["metaflow-user-attributes"])

        if range_info and (
            range_info.request_length is None or range_info.request_length < 0
        ):
            self._range_info = RangeInfo(
                range_info.total_size, range_info.request_offset, range_info.total_size
            )
        else:
            self._range_info = range_info

        if path:
            self._size = os.stat(self._path).st_size

        if prefix is None or prefix == url:
            self._key = url
            self._prefix = None
        else:
            self._key = url[len(prefix.rstrip("/")) + 1 :].rstrip("/")
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
            with open(self._path, "rb") as f:
                return f.read()

    @property
    def text(self):
        """
        Contents of the object as a Unicode string.
        Returns None if this S3Object has not been downloaded.
        """
        if self._path:
            return self.blob.decode("utf-8", errors="replace")

    @property
    def size(self):
        """
        Size of the object in bytes.
        Returns None if the key does not correspond to an object in S3.
        """
        return self._size

    @property
    def has_info(self):
        """
        Returns true if this S3Object contains the content-type or user-metadata.
        If False, this means that content_type and range_info will not return the
        proper information
        """
        return self._content_type is not None or self._metadata is not None

    @property
    def metadata(self):
        """
        Returns a dictionary of user-defined metadata
        """
        return self._metadata

    @property
    def content_type(self):
        """
        Returns the content-type of the S3 object; if unknown, returns None
        """
        return self._content_type

    @property
    def range_info(self):
        """
        Returns a namedtuple containing the following fields:
            - total_size: size in S3 of the object
            - request_offset: the starting offset in this S3Object
            - request_length: the length in this S3Object
        """
        return self._range_info

    @property
    def last_modified(self):
        """
        Returns the last modified unix timestamp of the object, or None
        if not fetched.
        """
        return self._last_modified

    def __str__(self):
        if self._path:
            return "<S3Object %s (%d bytes, local)>" % (self._url, self._size)
        elif self._size:
            return "<S3Object %s (%d bytes, in S3)>" % (self._url, self._size)
        else:
            return "<S3Object %s (object does not exist)>" % self._url

    def __repr__(self):
        return str(self)


class S3Client(object):
    def __init__(self):
        self._s3_client = None
        self._s3_error = None

    @property
    def client(self):
        if self._s3_client is None:
            self.reset_client()
        return self._s3_client

    @property
    def error(self):
        if self._s3_error is None:
            self.reset_client()
        return self._s3_error

    def reset_client(self):
        self._s3_client, self._s3_error = get_s3_client()


class S3(object):
    @classmethod
    def get_root_from_config(cls, echo, create_on_absent=True):
        return DATATOOLS_S3ROOT

    def __init__(
        self, tmproot=".", bucket=None, prefix=None, run=None, s3root=None, **kwargs
    ):
        """
        Initialize a new context for S3 operations. This object is used as
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
                    prefix = os.path.join(prefix, current.flow_name, current.run_id)
                else:
                    raise MetaflowS3URLException(
                        "Initializing S3 with a FlowSpec outside of a running "
                        "flow is not supported."
                    )
            else:
                prefix = os.path.join(prefix, run.parent.id, run.id)

            self._s3root = u"s3://%s" % os.path.join(bucket, prefix.strip("/"))
        elif s3root:
            # 2. use an explicit S3 prefix
            parsed = urlparse(to_unicode(s3root))
            if parsed.scheme != "s3":
                raise MetaflowS3URLException(
                    "s3root needs to be an S3 URL prefxied with s3://."
                )
            self._s3root = s3root.rstrip("/")
        else:
            # 3. use the client only with full URLs
            self._s3root = None

        self._s3_client = kwargs.get("external_client", S3Client())
        self._tmpdir = mkdtemp(dir=tmproot, prefix="metaflow.s3.")

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
                if self._tmpdir:
                    shutil.rmtree(self._tmpdir)
                    self._tmpdir = None
        except:
            pass

    def _url(self, key_value):
        # NOTE: All URLs are handled as Unicode objects (unicode in py2,
        # string in py3) internally. We expect that all URLs passed to this
        # class as either Unicode or UTF-8 encoded byte strings. All URLs
        # returned are Unicode.
        key = getattr(key_value, "key", key_value)
        if self._s3root is None:
            parsed = urlparse(to_unicode(key))
            if parsed.scheme == "s3" and parsed.path:
                return key
            else:
                if current.is_running_flow:
                    raise MetaflowS3URLException(
                        "Specify S3(run=self) when you use S3 inside a running "
                        "flow. Otherwise you have to use S3 with full "
                        "s3:// urls."
                    )
                else:
                    raise MetaflowS3URLException(
                        "Initialize S3 with an 's3root' or 'run' if you don't "
                        "want to specify full s3:// urls."
                    )
        elif key:
            if key.startswith("s3://"):
                raise MetaflowS3URLException(
                    "Don't use absolute S3 URLs when the S3 client is "
                    "initialized with a prefix. URL: %s" % key
                )
            return os.path.join(self._s3root, key)
        else:
            return self._s3root

    def _url_and_range(self, key_value):
        url = self._url(key_value)
        start = getattr(key_value, "offset", None)
        length = getattr(key_value, "length", None)
        range_str = None
        # Range specification are inclusive so getting from offset 500 for 100
        # bytes will read as bytes=500-599
        if start is not None or length is not None:
            if start is None:
                start = 0
            if length is None:
                # Fetch from offset till the end of the file
                range_str = "bytes=%d-" % start
            elif length < 0:
                # Fetch from end; ignore start value here
                range_str = "bytes=-%d" % (-length)
            else:
                # Typical range fetch
                range_str = "bytes=%d-%d" % (start, start + length - 1)
        return url, range_str

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
            urls = ((self._url(key).rstrip("/") + "/", None) for key in keys)
            res = self._read_many_files("list", urls)
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
            res = self._read_many_files(
                "list", map(self._url_and_range, keys), recursive=True
            )
            for s3prefix, s3url, size in res:
                yield s3prefix, s3url, None, int(size)

        return list(starmap(S3Object, _list(keys)))

    def info(self, key=None, return_missing=False):
        """
        Get information about a single object from S3
        Args:
            key: (optional) a suffix identifying the object.
            return_missing: (optional, default False) if set to True, do
                            not raise an exception for a missing key but
                            return it as an S3Object with .exists == False.
        Returns:
            an S3Object containing information about the object. The
            downloaded property will be false and exists will indicate whether
            or not the file exists
        """
        url = self._url(key)
        src = urlparse(url)

        def _info(s3, tmp):
            resp = s3.head_object(Bucket=src.netloc, Key=src.path.lstrip('/"'))
            return {
                "content_type": resp["ContentType"],
                "metadata": resp["Metadata"],
                "size": resp["ContentLength"],
                "last_modified": get_timestamp(resp["LastModified"]),
            }

        info_results = None
        try:
            _, info_results = self._one_boto_op(_info, url, create_tmp_file=False)
        except MetaflowS3NotFound:
            if return_missing:
                info_results = None
            else:
                raise
        if info_results:
            return S3Object(
                self._s3root,
                url,
                path=None,
                size=info_results["size"],
                content_type=info_results["content_type"],
                metadata=info_results["metadata"],
                last_modified=info_results["last_modified"],
            )
        return S3Object(self._s3root, url, None)

    def info_many(self, keys, return_missing=False):
        """
        Get information about many objects from S3 in parallel.
        Args:
            keys: (required) a list of suffixes identifying the objects.
            return_missing: (optional, default False) if set to True, do
                            not raise an exception for a missing key but
                            return it as an S3Object with .exists == False.
        Returns:
            a list of S3Objects corresponding to the objects requested. The
            downloaded property will be false and exists will indicate whether
            or not the file exists.
        """

        def _head():
            from . import s3op

            res = self._read_many_files(
                "info", map(self._url_and_range, keys), verbose=False, listing=True
            )

            for s3prefix, s3url, fname in res:
                if fname:
                    # We have a metadata file to read from
                    with open(os.path.join(self._tmpdir, fname), "r") as f:
                        info = json.load(f)
                    if info["error"] is not None:
                        # We have an error, we check if it is a missing file
                        if info["error"] == s3op.ERROR_URL_NOT_FOUND:
                            if return_missing:
                                yield self._s3root, s3url, None
                            else:
                                raise MetaflowS3NotFound()
                        elif info["error"] == s3op.ERROR_URL_ACCESS_DENIED:
                            raise MetaflowS3AccessDenied()
                        else:
                            raise MetaflowS3Exception("Got error: %d" % info["error"])
                    else:
                        yield self._s3root, s3url, None, info["size"], info[
                            "content_type"
                        ], info["metadata"], None, info["last_modified"]
                else:
                    # This should not happen; we should always get a response
                    # even if it contains an error inside it
                    raise MetaflowS3Exception("Did not get a response to HEAD")

        return list(starmap(S3Object, _head()))

    def get(self, key=None, return_missing=False, return_info=True):
        """
        Get a single object from S3.
        Args:
            key: (optional) a suffix identifying the object. Can also be
                 an object containing the properties `key`, `offset` and
                 `length` to specify a range query. `S3GetObject` is such an object.
            return_missing: (optional, default False) if set to True, do
                            not raise an exception for a missing key but
                            return it as an S3Object with .exists == False.
            return_info: (optional, default True) if set to True, fetch the
                         content-type and user metadata associated with the object.
        Returns:
            an S3Object corresponding to the object requested.
        """
        url, r = self._url_and_range(key)
        src = urlparse(url)

        def _download(s3, tmp):
            if r:
                resp = s3.get_object(
                    Bucket=src.netloc, Key=src.path.lstrip("/"), Range=r
                )
            else:
                resp = s3.get_object(Bucket=src.netloc, Key=src.path.lstrip("/"))
            sz = resp["ContentLength"]
            if not r and sz > DOWNLOAD_FILE_THRESHOLD:
                # In this case, it is more efficient to use download_file as it
                # will download multiple parts in parallel (it does it after
                # multipart_threshold)
                s3.download_file(src.netloc, src.path.lstrip("/"), tmp)
            else:
                with open(tmp, mode="wb") as t:
                    read_in_chunks(t, resp["Body"], sz, DOWNLOAD_MAX_CHUNK)
            if return_info:
                return {
                    "content_type": resp["ContentType"],
                    "metadata": resp["Metadata"],
                    "last_modified": get_timestamp(resp["LastModified"]),
                }
            return None

        addl_info = None
        try:
            path, addl_info = self._one_boto_op(_download, url)
        except MetaflowS3NotFound:
            if return_missing:
                path = None
            else:
                raise
        if addl_info:
            return S3Object(
                self._s3root,
                url,
                path,
                content_type=addl_info["content_type"],
                metadata=addl_info["metadata"],
                last_modified=addl_info["last_modified"],
            )
        return S3Object(self._s3root, url, path)

    def get_many(self, keys, return_missing=False, return_info=True):
        """
        Get many objects from S3 in parallel.
        Args:
            keys: (required) a list of suffixes identifying the objects. Each
                  item in the list can also be an object containing the properties
                  `key`, `offset` and `length to specify a range query.
                  `S3GetObject` is such an object.
            return_missing: (optional, default False) if set to True, do
                            not raise an exception for a missing key but
                            return it as an S3Object with .exists == False.
            return_info: (optional, default True) if set to True, fetch the
                         content-type and user metadata associated with the object.
        Returns:
            a list of S3Objects corresponding to the objects requested.
        """

        def _get():
            res = self._read_many_files(
                "get",
                map(self._url_and_range, keys),
                allow_missing=return_missing,
                verify=True,
                verbose=False,
                info=return_info,
                listing=True,
            )

            for s3prefix, s3url, fname in res:
                if return_info:
                    if fname:
                        # We have a metadata file to read from
                        with open(
                            os.path.join(self._tmpdir, "%s_meta" % fname), "r"
                        ) as f:
                            info = json.load(f)
                        yield self._s3root, s3url, os.path.join(
                            self._tmpdir, fname
                        ), None, info["content_type"], info["metadata"], None, info[
                            "last_modified"
                        ]
                    else:
                        yield self._s3root, s3prefix, None
                else:
                    if fname:
                        yield self._s3root, s3url, os.path.join(self._tmpdir, fname)
                    else:
                        # missing entries per return_missing=True
                        yield self._s3root, s3prefix, None

        return list(starmap(S3Object, _get()))

    def get_recursive(self, keys, return_info=False):
        """
        Get many objects from S3 recursively in parallel.
        Args:
            keys: (required) a list of suffixes for paths to download
                  recursively.
            return_info: (optional, default False) if set to True, fetch the
                         content-type and user metadata associated with the object.
        Returns:
            a list of S3Objects corresponding to the objects requested.
        """

        def _get():
            res = self._read_many_files(
                "get",
                map(self._url_and_range, keys),
                recursive=True,
                verify=True,
                verbose=False,
                info=return_info,
                listing=True,
            )

            for s3prefix, s3url, fname in res:
                if return_info:
                    # We have a metadata file to read from
                    with open(os.path.join(self._tmpdir, "%s_meta" % fname), "r") as f:
                        info = json.load(f)
                    yield self._s3root, s3url, os.path.join(
                        self._tmpdir, fname
                    ), None, info["content_type"], info["metadata"], None, info[
                        "last_modified"
                    ]
                else:
                    yield s3prefix, s3url, os.path.join(self._tmpdir, fname)

        return list(starmap(S3Object, _get()))

    def get_all(self, return_info=False):
        """
        Get all objects from S3 recursively (in parallel). This request
        only works if S3 is initialized with a run or a s3root prefix.
        Args:
            return_info: (optional, default False) if set to True, fetch the
                         content-type and user metadata associated with the object.
        Returns:
            a list of S3Objects corresponding to the objects requested.
        """
        if self._s3root is None:
            raise MetaflowS3URLException(
                "Can't get_all() when S3 is initialized without a prefix"
            )
        else:
            return self.get_recursive([None], return_info)

    def put(self, key, obj, overwrite=True, content_type=None, metadata=None):
        """
        Put an object to S3.
        Args:
            key:           (required) suffix for the object.
            obj:           (required) a bytes, string, or a unicode object to
                           be stored in S3.
            overwrite:     (optional) overwrites the key with obj, if it exists
            content_type:  (optional) string representing the MIME type of the
                           object
            metadata:      (optional) User metadata to store alongside the object
        Returns:
            an S3 URL corresponding to the object stored.
        """
        if isinstance(obj, (RawIOBase, BufferedIOBase)):
            if not obj.readable() or not obj.seekable():
                raise MetaflowS3InvalidObject(
                    "Object corresponding to the key '%s' is not readable or seekable"
                    % key
                )
            blob = obj
        else:
            if not is_stringish(obj):
                raise MetaflowS3InvalidObject(
                    "Object corresponding to the key '%s' is not a string "
                    "or a bytes object." % key
                )
            blob = to_fileobj(obj)
        # We override the close functionality to prevent closing of the
        # file if it is used multiple times when uploading (since upload_fileobj
        # will/may close it on failure)
        real_close = blob.close
        blob.close = lambda: None

        url = self._url(key)
        src = urlparse(url)
        extra_args = None
        if content_type or metadata:
            extra_args = {}
            if content_type:
                extra_args["ContentType"] = content_type
            if metadata:
                extra_args["Metadata"] = {
                    "metaflow-user-attributes": json.dumps(metadata)
                }

        def _upload(s3, _):
            # We make sure we are at the beginning in case we are retrying
            blob.seek(0)
            s3.upload_fileobj(
                blob, src.netloc, src.path.lstrip("/"), ExtraArgs=extra_args
            )

        if overwrite:
            self._one_boto_op(_upload, url, create_tmp_file=False)
            real_close()
            return url
        else:

            def _head(s3, _):
                s3.head_object(Bucket=src.netloc, Key=src.path.lstrip("/"))

            try:
                self._one_boto_op(_head, url, create_tmp_file=False)
            except MetaflowS3NotFound:
                self._one_boto_op(_upload, url, create_tmp_file=False)
            finally:
                real_close()
            return url

    def put_many(self, key_objs, overwrite=True):
        """
        Put objects to S3 in parallel.
        Args:
            key_objs:  (required) an iterator of (key, value) tuples. Value must
                       be a string, bytes, or a unicode object. Instead of
                       (key, value) tuples, you can also pass any object that
                       has the following properties 'key', 'value', 'content_type',
                       'metadata' like the S3PutObject for example. 'key' and
                       'value' are required but others are optional.
            overwrite: (optional) overwrites the key with obj, if it exists
        Returns:
            a list of (key, S3 URL) tuples corresponding to the files sent.
        """

        def _store():
            for key_obj in key_objs:
                if isinstance(key_obj, tuple):
                    key = key_obj[0]
                    obj = key_obj[1]
                else:
                    key = key_obj.key
                    obj = key_obj.value
                store_info = {
                    "key": key,
                    "content_type": getattr(key_obj, "content_type", None),
                }
                metadata = getattr(key_obj, "metadata", None)
                if metadata:
                    store_info["metadata"] = {
                        "metaflow-user-attributes": json.dumps(metadata)
                    }
                if isinstance(obj, (RawIOBase, BufferedIOBase)):
                    if not obj.readable() or not obj.seekable():
                        raise MetaflowS3InvalidObject(
                            "Object corresponding to the key '%s' is not readable or seekable"
                            % key
                        )
                else:
                    if not is_stringish(obj):
                        raise MetaflowS3InvalidObject(
                            "Object corresponding to the key '%s' is not a string "
                            "or a bytes object." % key
                        )
                    obj = to_fileobj(obj)
                with NamedTemporaryFile(
                    dir=self._tmpdir,
                    delete=False,
                    mode="wb",
                    prefix="metaflow.s3.put_many.",
                ) as tmp:
                    tmp.write(obj.read())
                    tmp.close()
                    yield tmp.name, self._url(key), store_info

        return self._put_many_files(_store(), overwrite)

    def put_files(self, key_paths, overwrite=True):
        """
        Put files to S3 in parallel.
        Args:
            key_paths: (required) an iterator of (key, path) tuples. Instead of
                       (key, path) tuples, you can also pass any object that
                       has the following properties 'key', 'path', 'content_type',
                       'metadata' like the S3PutObject for example. 'key' and
                       'path' are required but others are optional.
            overwrite: (optional) overwrites the key with obj, if it exists
        Returns:
            a list of (key, S3 URL) tuples corresponding to the files sent.
        """

        def _check():
            for key_path in key_paths:
                if isinstance(key_path, tuple):
                    key = key_path[0]
                    path = key_path[1]
                else:
                    key = key_path.key
                    path = key_path.path
                store_info = {
                    "key": key,
                    "content_type": getattr(key_path, "content_type", None),
                }
                metadata = getattr(key_path, "metadata", None)
                if metadata:
                    store_info["metadata"] = {
                        "metaflow-user-attributes": json.dumps(metadata)
                    }
                if not os.path.exists(path):
                    raise MetaflowS3NotFound("Local file not found: %s" % path)
                yield path, self._url(key), store_info

        return self._put_many_files(_check(), overwrite)

    def _one_boto_op(self, op, url, create_tmp_file=True):
        error = ""
        for i in range(S3_RETRY_COUNT + 1):
            tmp = None
            if create_tmp_file:
                tmp = NamedTemporaryFile(
                    dir=self._tmpdir, prefix="metaflow.s3.one_file.", delete=False
                )
            try:
                side_results = op(self._s3_client.client, tmp.name if tmp else None)
                return tmp.name if tmp else None, side_results
            except self._s3_client.error as err:
                from . import s3op

                error_code = s3op.normalize_client_error(err)
                if error_code == 404:
                    raise MetaflowS3NotFound(url)
                elif error_code == 403:
                    raise MetaflowS3AccessDenied(url)
                elif error_code == "NoSuchBucket":
                    raise MetaflowS3URLException("Specified S3 bucket doesn't exist.")
                error = str(err)
            except Exception as ex:
                # TODO specific error message for out of disk space
                error = str(ex)
            if tmp:
                os.unlink(tmp.name)
            self._s3_client.reset_client()
            # add some jitter to make sure retries are not synchronized
            time.sleep(2 ** i + random.randint(0, 10))
        raise MetaflowS3Exception(
            "S3 operation failed.\n" "Key requested: %s\n" "Error: %s" % (url, error)
        )

    # NOTE: re: _read_many_files and _put_many_files
    # All file IO is through binary files - we write bytes, we read
    # bytes. All inputs and outputs from these functions are Unicode.
    # Conversion between bytes and unicode is done through
    # and url_unquote.
    def _read_many_files(self, op, prefixes_and_ranges, **options):
        prefixes_and_ranges = list(prefixes_and_ranges)
        with NamedTemporaryFile(
            dir=self._tmpdir,
            mode="wb",
            delete=not debug.s3client,
            prefix="metaflow.s3.inputs.",
        ) as inputfile:
            inputfile.write(
                b"\n".join(
                    [
                        b" ".join([url_quote(prefix)] + ([url_quote(r)] if r else []))
                        for prefix, r in prefixes_and_ranges
                    ]
                )
            )
            inputfile.flush()
            stdout, stderr = self._s3op_with_retries(
                op, inputs=inputfile.name, **options
            )
            if stderr:
                raise MetaflowS3Exception(
                    "Getting S3 files failed.\n"
                    "First prefix requested: %s\n"
                    "Error: %s" % (prefixes_and_ranges[0], stderr)
                )
            else:
                for line in stdout.splitlines():
                    yield tuple(map(url_unquote, line.strip(b"\n").split(b" ")))

    def _put_many_files(self, url_info, overwrite):
        url_info = list(url_info)
        url_dicts = [
            dict(
                chain([("local", os.path.realpath(local)), ("url", url)], info.items())
            )
            for local, url, info in url_info
        ]

        with NamedTemporaryFile(
            dir=self._tmpdir,
            mode="wb",
            delete=not debug.s3client,
            prefix="metaflow.s3.put_inputs.",
        ) as inputfile:
            lines = [to_bytes(json.dumps(x)) for x in url_dicts]
            inputfile.write(b"\n".join(lines))
            inputfile.flush()
            stdout, stderr = self._s3op_with_retries(
                "put",
                filelist=inputfile.name,
                verbose=False,
                overwrite=overwrite,
                listing=True,
            )
            if stderr:
                raise MetaflowS3Exception(
                    "Uploading S3 files failed.\n"
                    "First key: %s\n"
                    "Error: %s" % (url_info[0][2]["key"], stderr)
                )
            else:
                urls = set()
                for line in stdout.splitlines():
                    url, _, _ = map(url_unquote, line.strip(b"\n").split(b" "))
                    urls.add(url)
                return [(info["key"], url) for _, url, info in url_info if url in urls]

    def _s3op_with_retries(self, mode, **options):
        from . import s3op

        cmdline = [sys.executable, os.path.abspath(s3op.__file__), mode]
        for key, value in options.items():
            key = key.replace("_", "-")
            if isinstance(value, bool):
                if value:
                    cmdline.append("--%s" % key)
                else:
                    cmdline.append("--no-%s" % key)
            else:
                cmdline.extend(("--%s" % key, value))

        for i in range(S3_RETRY_COUNT + 1):
            with NamedTemporaryFile(
                dir=self._tmpdir,
                mode="wb+",
                delete=not debug.s3client,
                prefix="metaflow.s3op.stderr",
            ) as stderr:
                try:
                    debug.s3client_exec(cmdline)
                    stdout = subprocess.check_output(
                        cmdline, cwd=self._tmpdir, stderr=stderr.file
                    )
                    return stdout, None
                except subprocess.CalledProcessError as ex:
                    stderr.seek(0)
                    err_out = stderr.read().decode("utf-8", errors="replace")
                    stderr.seek(0)
                    if ex.returncode == s3op.ERROR_URL_NOT_FOUND:
                        raise MetaflowS3NotFound(err_out)
                    elif ex.returncode == s3op.ERROR_URL_ACCESS_DENIED:
                        raise MetaflowS3AccessDenied(err_out)
                    print("Error with S3 operation:", err_out)
                    time.sleep(2 ** i + random.randint(0, 10))

        return None, err_out
