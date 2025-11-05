import errno
import json
import os
import re
import sys
import time
import shutil
import random
import subprocess
from io import RawIOBase, BufferedIOBase
from itertools import chain, starmap
from tempfile import mkdtemp, NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Tuple, Union, TYPE_CHECKING

from metaflow import FlowSpec
from metaflow.metaflow_current import current
from metaflow.metaflow_config import (
    DATATOOLS_S3ROOT,
    S3_RETRY_COUNT,
    S3_TRANSIENT_RETRY_COUNT,
    S3_LOG_TRANSIENT_RETRIES,
    S3_SERVER_SIDE_ENCRYPTION,
    S3_WORKER_COUNT,
    TEMPDIR,
)
from metaflow.util import (
    is_stringish,
    to_bytes,
    to_unicode,
    to_fileobj,
    url_quote,
    url_unquote,
)
from metaflow.tuple_util import namedtuple_with_defaults
from metaflow.exception import MetaflowException
from metaflow.debug import debug
import metaflow.tracing as tracing

try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse

from .s3util import (
    get_s3_client,
    read_in_chunks,
    get_timestamp,
    TRANSIENT_RETRY_START_LINE,
    TRANSIENT_RETRY_LINE_CONTENT,
)

if TYPE_CHECKING:
    import metaflow


def _check_and_init_s3_deps():
    try:
        import boto3
        from boto3.s3.transfer import TransferConfig
    except (ImportError, ModuleNotFoundError):
        raise MetaflowException("You need to install 'boto3' in order to use S3.")


def check_s3_deps(func):
    """The decorated function checks S3 dependencies (as needed for AWS S3 storage backend).
    This includes boto3.
    """

    def _inner_func(*args, **kwargs):
        _check_and_init_s3_deps()
        return func(*args, **kwargs)

    return _inner_func


TEST_INJECT_RETRYABLE_FAILURES = int(
    os.environ.get("METAFLOW_S3_TEST_RETRYABLE_FAILURES", 0)
)


def ensure_unicode(x):
    return None if x is None else to_unicode(x)


PutValue = Union[RawIOBase, BufferedIOBase, str, bytes]

S3GetObject = namedtuple_with_defaults(
    "S3GetObject", [("key", str), ("offset", int), ("length", int)]
)
S3GetObject.__module__ = __name__

S3PutObject = namedtuple_with_defaults(
    "S3PutObject",
    [
        ("key", str),
        ("value", Optional[PutValue]),
        ("path", Optional[str]),
        ("content_type", Optional[str]),
        ("encryption", Optional[str]),
        ("metadata", Optional[Dict[str, str]]),
    ],
    defaults=(None, None, None, None, None),
)
S3PutObject.__module__ = __name__

RangeInfo = namedtuple_with_defaults(
    "RangeInfo",
    [("total_size", int), ("request_offset", int), ("request_length", int)],
    defaults=(0, -1),
)
RangeInfo.__module__ = __name__

RANGE_MATCH = re.compile(r"bytes (?P<start>[0-9]+)-(?P<end>[0-9]+)/(?P<total>[0-9]+)")


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


class MetaflowS3InvalidRange(MetaflowException):
    headline = "S3 invalid range"


class MetaflowS3InsufficientDiskSpace(MetaflowException):
    headline = "Insufficient disk space"


class S3Object(object):
    """
    This object represents a path or an object in S3,
    with an optional local copy.

    `S3Object`s are not instantiated directly, but they are returned
    by many methods of the `S3` client.
    """

    def __init__(
        self,
        prefix: str,
        url: str,
        path: str,
        size: Optional[int] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        range_info: Optional[RangeInfo] = None,
        last_modified: Optional[int] = None,
        encryption: Optional[str] = None,
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

        self._encryption = encryption

    @property
    def exists(self) -> bool:
        """
        Does this key correspond to an object in S3?

        Returns
        -------
        bool
            True if this object points at an existing object (file) in S3.
        """
        return self._size is not None

    @property
    def downloaded(self) -> bool:
        """
        Has this object been downloaded?

        If True, the contents can be accessed through `path`, `blob`,
        and `text` properties.

        Returns
        -------
        bool
            True if the contents of this object have been downloaded.
        """
        return bool(self._path)

    @property
    def url(self) -> str:
        """
        S3 location of the object

        Returns
        -------
        str
            The S3 location of this object.
        """
        return self._url

    @property
    def prefix(self) -> str:
        """
        Prefix requested that matches this object.

        Returns
        -------
        str
            Requested prefix
        """
        return self._prefix

    @property
    def key(self) -> str:
        """
        Key corresponds to the key given to the get call that produced
        this object.

        This may be a full S3 URL or a suffix based on what
        was requested.

        Returns
        -------
        str
            Key requested.
        """
        return self._key

    @property
    def path(self) -> Optional[str]:
        """
        Path to a local temporary file corresponding to the object downloaded.

        This file gets deleted automatically when a S3 scope exits.
        Returns None if this S3Object has not been downloaded.

        Returns
        -------
        str
            Local path, if the object has been downloaded.
        """
        return self._path

    @property
    def blob(self) -> Optional[bytes]:
        """
        Contents of the object as a byte string or None if the
        object hasn't been downloaded.

        Returns
        -------
        bytes
            Contents of the object as bytes.
        """
        if self._path:
            with open(self._path, "rb") as f:
                return f.read()

    @property
    def text(self) -> Optional[str]:
        """
        Contents of the object as a string or None if the
        object hasn't been downloaded.

        The object is assumed to contain UTF-8 encoded data.

        Returns
        -------
        str
            Contents of the object as text.
        """
        if self._path:
            return self.blob.decode("utf-8", errors="replace")

    @property
    def size(self) -> Optional[int]:
        """
        Size of the object in bytes.

        Returns None if the key does not correspond to an object in S3.

        Returns
        -------
        int
            Size of the object in bytes, if the object exists.
        """
        return self._size

    @property
    def has_info(self) -> bool:
        """
        Returns true if this `S3Object` contains the content-type MIME header or
        user-defined metadata.

        If False, this means that `content_type`, `metadata`, `range_info` and
        `last_modified` will return None.

        Returns
        -------
        bool
            True if additional metadata is available.
        """
        return (
            self._content_type is not None
            or self._metadata is not None
            or self._range_info is not None
            or self._encryption is not None
        )

    @property
    def metadata(self) -> Optional[Dict[str, str]]:
        """
        Returns a dictionary of user-defined metadata, or None if no metadata
        is defined.

        Returns
        -------
        Dict
            User-defined metadata.
        """
        return self._metadata

    @property
    def content_type(self) -> Optional[str]:
        """
        Returns the content-type of the S3 object or None if it is not defined.

        Returns
        -------
        str
            Content type or None if the content type is undefined.
        """
        return self._content_type

    @property
    def encryption(self) -> Optional[str]:
        """
        Returns the encryption type of the S3 object or None if it is not defined.

        Returns
        -------
        str
            Server-side-encryption type or None if parameter is not set.
        """
        return self._encryption

    @property
    def range_info(self) -> Optional[RangeInfo]:
        """
        If the object corresponds to a partially downloaded object, returns
        information of what was downloaded.

        The returned object has the following fields:
        - `total_size`: Size of the object in S3.
        - `request_offset`: The starting offset.
        - `request_length`: The number of bytes downloaded.

        Returns
        -------
        namedtuple
            An object containing information about the partial download. If
            the `S3Object` doesn't correspond to a partially downloaded file,
            returns None.
        """
        return self._range_info

    @property
    def last_modified(self) -> Optional[int]:
        """
        Returns the last modified unix timestamp of the object.

        Returns
        -------
        int
            Unix timestamp corresponding to the last modified time.
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
    def __init__(self, s3_role_arn=None, s3_session_vars=None, s3_client_params=None):
        self._s3_client = None
        self._s3_error = None
        self._s3_role = s3_role_arn
        self._s3_session_vars = s3_session_vars
        self._s3_client_params = s3_client_params

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
        self._s3_client, self._s3_error = get_s3_client(
            s3_role_arn=self._s3_role,
            s3_session_vars=self._s3_session_vars,
            s3_client_params=self._s3_client_params,
        )


class S3(object):
    """
    The Metaflow S3 client.

    This object manages the connection to S3 and a temporary diretory that is used
    to download objects. Note that in most cases when the data fits in memory, no local
    disk IO is needed as operations are cached by the operating system, which makes
    operations fast as long as there is enough memory available.

    The easiest way is to use this object as a context manager:
    ```
    with S3() as s3:
        data = [obj.blob for obj in s3.get_many(urls)]
    print(data)
    ```
    The context manager takes care of creating and deleting a temporary directory
    automatically. Without a context manager, you must call `.close()` to delete
    the directory explicitly:
    ```
    s3 = S3()
    data = [obj.blob for obj in s3.get_many(urls)]
    s3.close()
    ```
    You can customize the location of the temporary directory with `tmproot`. It
    defaults to the current working directory.

    To make it easier to deal with object locations, the client can be initialized
    with an S3 path prefix. There are three ways to handle locations:

    1. Use a `metaflow.Run` object or `self`, e.g. `S3(run=self)` which
       initializes the prefix with the global `DATATOOLS_S3ROOT` path, combined
       with the current run ID. This mode makes it easy to version data based
       on the run ID consistently. You can use the `bucket` and `prefix` to
       override parts of `DATATOOLS_S3ROOT`.

    2. Specify an S3 prefix explicitly with `s3root`,
       e.g. `S3(s3root='s3://mybucket/some/path')`.

    3. Specify nothing, i.e. `S3()`, in which case all operations require
       a full S3 url prefixed with `s3://`.

    Parameters
    ----------
    tmproot : str, default '.'
        Where to store the temporary directory.
    bucket : str, optional, default None
        Override the bucket from `DATATOOLS_S3ROOT` when `run` is specified.
    prefix : str, optional, default None
        Override the path from `DATATOOLS_S3ROOT` when `run` is specified.
    run : FlowSpec or Run, optional, default None
        Derive path prefix from the current or a past run ID, e.g. S3(run=self).
    s3root : str, optional, default None
        If `run` is not specified, use this as the S3 prefix.
    encryption : str, optional, default None
        Server-side encryption to use when uploading objects to S3.
    """

    TYPE = "s3"

    @classmethod
    def get_root_from_config(cls, echo, create_on_absent=True):
        return DATATOOLS_S3ROOT

    @check_s3_deps
    def __init__(
        self,
        tmproot: str = TEMPDIR,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        run: Optional[Union[FlowSpec, "metaflow.Run"]] = None,
        s3root: Optional[str] = None,
        encryption: Optional[str] = S3_SERVER_SIDE_ENCRYPTION,
        **kwargs
    ):
        if run:
            # 1. use a (current) run ID with optional customizations
            if DATATOOLS_S3ROOT is None:
                raise MetaflowS3URLException(
                    "DATATOOLS_S3ROOT is not configured when trying to use S3 storage"
                )
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

            self._s3root = "s3://%s" % os.path.join(bucket, prefix.strip("/"))
        elif s3root:
            # 2. use an explicit S3 prefix
            parsed = urlparse(to_unicode(s3root))
            if parsed.scheme != "s3":
                raise MetaflowS3URLException(
                    "s3root needs to be an S3 URL prefixed with s3://."
                )
            self._s3root = s3root.rstrip("/")
        else:
            # 3. use the client only with full URLs
            self._s3root = None

        # Note that providing a role, session vars or client params and a client
        # will result in the role/session vars/client params being ignored
        self._s3_role = kwargs.get("role", None)
        self._s3_session_vars = kwargs.get("session_vars", None)
        self._s3_client_params = kwargs.get("client_params", None)
        self._s3_client = kwargs.get(
            "external_client",
            S3Client(
                s3_role_arn=self._s3_role,
                s3_session_vars=self._s3_session_vars,
                s3_client_params=self._s3_client_params,
            ),
        )
        self._s3_inject_failures = kwargs.get(
            "inject_failure_rate", TEST_INJECT_RETRYABLE_FAILURES
        )
        # Storing tmproot, bucket, ... as members to allow easier reconstruction
        # during JSON deserialization
        self._tmproot = tmproot
        self._bucket = bucket
        self._prefix = prefix
        self._run = run
        self._tmpdir = mkdtemp(dir=self._tmproot, prefix="metaflow.s3.")
        self._encryption = encryption

    def __enter__(self) -> "S3":
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
            # NOTE: S3 allows fragments as part of object names, e.g. /dataset #1/data.txt
            # Without allow_fragments=False the parsed.path for an object name with fragments is incomplete.
            parsed = urlparse(to_unicode(key), allow_fragments=False)
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
            # Strip leading slashes to ensure os.path.join works correctly
            # os.path.join discards the first argument if the second starts with '/'
            return os.path.join(self._s3root, key.lstrip("/"))
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

    def list_paths(self, keys: Optional[Iterable[str]] = None) -> List[S3Object]:
        """
        List the next level of paths in S3.

        If multiple keys are specified, listings are done in parallel. The returned
        S3Objects have `.exists == False` if the path refers to a prefix, not an
        existing S3 object.

        For instance, if the directory hierarchy is
        ```
        a/0.txt
        a/b/1.txt
        a/c/2.txt
        a/d/e/3.txt
        f/4.txt
        ```
        The `list_paths(['a', 'f'])` call returns
        ```
        a/0.txt (exists == True)
        a/b/ (exists == False)
        a/c/ (exists == False)
        a/d/ (exists == False)
        f/4.txt (exists == True)
        ```

        Parameters
        ----------
        keys : Iterable[str], optional, default None
            List of paths.

        Returns
        -------
        List[S3Object]
            S3Objects under the given paths, including prefixes (directories) that
            do not correspond to leaf objects.
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

    def list_recursive(self, keys: Optional[Iterable[str]] = None) -> List[S3Object]:
        """
        List all objects recursively under the given prefixes.

        If multiple keys are specified, listings are done in parallel. All objects
        returned have `.exists == True` as this call always returns leaf objects.

        For instance, if the directory hierarchy is
        ```
        a/0.txt
        a/b/1.txt
        a/c/2.txt
        a/d/e/3.txt
        f/4.txt
        ```
        The `list_paths(['a', 'f'])` call returns
        ```
        a/0.txt (exists == True)
        a/b/1.txt (exists == True)
        a/c/2.txt (exists == True)
        a/d/e/3.txt (exists == True)
        f/4.txt (exists == True)
        ```

        Parameters
        ----------
        keys : Iterable[str], optional, default None
            List of paths.

        Returns
        -------
        List[S3Object]
            S3Objects under the given paths.
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

    def info(self, key: Optional[str] = None, return_missing: bool = False) -> S3Object:
        """
        Get metadata about a single object in S3.

        This call makes a single `HEAD` request to S3 which can be
        much faster than downloading all data with `get`.

        Parameters
        ----------
        key : str, optional, default None
            Object to query. It can be an S3 url or a path suffix.
        return_missing : bool, default False
            If set to True, do not raise an exception for a missing key but
            return it as an `S3Object` with `.exists == False`.

        Returns
        -------
        S3Object
            An S3Object corresponding to the object requested. The object
            will have `.downloaded == False`.
        """

        url = self._url(key)
        # NOTE: S3 allows fragments as part of object names, e.g. /dataset #1/data.txt
        # Without allow_fragments=False the parsed src.path for an object name with fragments is incomplete.
        src = urlparse(url, allow_fragments=False)

        def _info(s3, tmp):
            resp = s3.head_object(Bucket=src.netloc, Key=src.path.lstrip('/"'))
            return {
                "content_type": resp["ContentType"],
                "metadata": resp["Metadata"],
                "size": resp["ContentLength"],
                "last_modified": get_timestamp(resp["LastModified"]),
                "encryption": resp.get("ServerSideEncryption"),
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
                encryption=info_results["encryption"],
            )
        return S3Object(self._s3root, url, None)

    def info_many(
        self, keys: Iterable[str], return_missing: bool = False
    ) -> List[S3Object]:
        """
        Get metadata about many objects in S3 in parallel.

        This call makes a single `HEAD` request to S3 which can be
        much faster than downloading all data with `get`.

        Parameters
        ----------
        keys : Iterable[str]
            Objects to query. Each key can be an S3 url or a path suffix.
        return_missing : bool, default False
            If set to True, do not raise an exception for a missing key but
            return it as an `S3Object` with `.exists == False`.

        Returns
        -------
        List[S3Object]
            A list of S3Objects corresponding to the paths requested. The
            objects will have `.downloaded == False`.
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
                        ], info["metadata"], None, info["last_modified"], info[
                            "encryption"
                        ]
                else:
                    # This should not happen; we should always get a response
                    # even if it contains an error inside it
                    raise MetaflowS3Exception("Did not get a response to HEAD")

        return list(starmap(S3Object, _head()))

    def get(
        self,
        key: Optional[Union[str, S3GetObject]] = None,
        return_missing: bool = False,
        return_info: bool = True,
    ) -> S3Object:
        """
        Get a single object from S3.

        Parameters
        ----------
        key : Union[str, S3GetObject], optional, default None
            Object to download. It can be an S3 url, a path suffix, or
            an S3GetObject that defines a range of data to download. If None, or
            not provided, gets the S3 root.
        return_missing : bool, default False
            If set to True, do not raise an exception for a missing key but
            return it as an `S3Object` with `.exists == False`.
        return_info : bool, default True
            If set to True, fetch the content-type and user metadata associated
            with the object at no extra cost, included for symmetry with `get_many`

        Returns
        -------
        S3Object
            An S3Object corresponding to the object requested.
        """
        from boto3.s3.transfer import TransferConfig

        DOWNLOAD_FILE_THRESHOLD = 2 * TransferConfig().multipart_threshold
        DOWNLOAD_MAX_CHUNK = 2 * 1024 * 1024 * 1024 - 1

        url, r = self._url_and_range(key)
        # NOTE: S3 allows fragments as part of object names, e.g. /dataset #1/data.txt
        # Without allow_fragments=False the parsed src.path for an object name with fragments is incomplete.
        src = urlparse(url, allow_fragments=False)

        def _download(s3, tmp):
            if r:
                resp = s3.get_object(
                    Bucket=src.netloc, Key=src.path.lstrip("/"), Range=r
                )
                # Format is bytes start-end/total; both start and end are inclusive so
                # a 500 bytes file will be `bytes 0-499/500` for the entire file.
                range_result = resp["ContentRange"]
                range_result_match = RANGE_MATCH.match(range_result)
                if range_result_match is None:
                    raise RuntimeError(
                        "Wrong format for ContentRange: %s" % str(range_result)
                    )
                range_result = RangeInfo(
                    int(range_result_match.group("total")),
                    request_offset=int(range_result_match.group("start")),
                    request_length=int(range_result_match.group("end"))
                    - int(range_result_match.group("start"))
                    + 1,
                )
            else:
                resp = s3.get_object(Bucket=src.netloc, Key=src.path.lstrip("/"))
                range_result = None
            sz = resp["ContentLength"]
            if range_result is None:
                range_result = RangeInfo(sz, request_offset=0, request_length=sz)
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
                    # Since Metaflow can also use S3-compatible storage like MinIO,
                    # there maybe some keys missing in the responses given by different S3-compatible object stores.
                    # MinIO is generally accessed via HTTPS, and so it's encrpytion scheme is
                    # TLS/SSL. This is why the `ServerSideEncryption` key is not present
                    # in the response from MinIO.
                    "encryption": resp.get("ServerSideEncryption"),
                    "metadata": resp["Metadata"],
                    "range_result": range_result,
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
                encryption=addl_info["encryption"],
                metadata=addl_info["metadata"],
                range_info=addl_info["range_result"],
                last_modified=addl_info["last_modified"],
            )
        return S3Object(self._s3root, url, path)

    def get_many(
        self,
        keys: Iterable[Union[str, S3GetObject]],
        return_missing: bool = False,
        return_info: bool = True,
    ) -> List[S3Object]:
        """
        Get many objects from S3 in parallel.

        Parameters
        ----------
        keys : Iterable[Union[str, S3GetObject]]
            Objects to download. Each object can be an S3 url, a path suffix, or
            an S3GetObject that defines a range of data to download.
        return_missing : bool, default False
            If set to True, do not raise an exception for a missing key but
            return it as an `S3Object` with `.exists == False`.
        return_info : bool, default True
            If set to True, fetch the content-type and user metadata associated
            with the object at no extra cost, included for symmetry with `get_many`.

        Returns
        -------
        List[S3Object]
            S3Objects corresponding to the objects requested.
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
                        range_info = info.get("range_result")
                        if range_info:
                            range_info = RangeInfo(
                                range_info["total"],
                                request_offset=range_info["start"],
                                request_length=range_info["end"]
                                - range_info["start"]
                                + 1,
                            )
                            yield self._s3root, s3url, os.path.join(
                                self._tmpdir, fname
                            ), None, info["content_type"], info[
                                "metadata"
                            ], range_info, info[
                                "last_modified"
                            ], info.get(
                                "encryption"
                            )
                    else:
                        yield self._s3root, s3prefix, None
                else:
                    if fname:
                        yield self._s3root, s3url, os.path.join(self._tmpdir, fname)
                    else:
                        # missing entries per return_missing=True
                        yield self._s3root, s3prefix, None

        return list(starmap(S3Object, _get()))

    def get_recursive(
        self, keys: Iterable[str], return_info: bool = False
    ) -> List[S3Object]:
        """
        Get many objects from S3 recursively in parallel.

        Parameters
        ----------
        keys : Iterable[str]
            Prefixes to download recursively. Each prefix can be an S3 url or a path suffix
            which define the root prefix under which all objects are downloaded.
        return_info : bool, default False
            If set to True, fetch the content-type and user metadata associated
            with the object.

        Returns
        -------
        List[S3Object]
            S3Objects stored under the given prefixes.
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
                    range_info = info.get("range_result")
                    if range_info:
                        range_info = RangeInfo(
                            range_info["total"],
                            request_offset=range_info["start"],
                            request_length=range_info["end"] - range_info["start"] + 1,
                        )
                    yield self._s3root, s3url, os.path.join(
                        self._tmpdir, fname
                    ), None, info["content_type"], info["metadata"], range_info, info[
                        "last_modified"
                    ], info.get(
                        "encryption"
                    )
                else:
                    yield s3prefix, s3url, os.path.join(self._tmpdir, fname)

        return list(starmap(S3Object, _get()))

    def get_all(self, return_info: bool = False) -> List[S3Object]:
        """
        Get all objects under the prefix set in the `S3` constructor.

        This method requires that the `S3` object is initialized either with `run` or
        `s3root`.

        Parameters
        ----------
        return_info : bool, default False
            If set to True, fetch the content-type and user metadata associated
            with the object.

        Returns
        -------
        Iterable[S3Object]
            S3Objects stored under the main prefix.
        """

        if self._s3root is None:
            raise MetaflowS3URLException(
                "Can't get_all() when S3 is initialized without a prefix"
            )
        else:
            return self.get_recursive([None], return_info)

    def put(
        self,
        key: Union[str, S3PutObject],
        obj: PutValue,
        overwrite: bool = True,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Upload a single object to S3.

        Parameters
        ----------
        key : Union[str, S3PutObject]
            Object path. It can be an S3 url or a path suffix.
        obj : PutValue
            An object to store in S3. Strings are converted to UTF-8 encoding.
        overwrite : bool, default True
            Overwrite the object if it exists. If set to False, the operation
            succeeds without uploading anything if the key already exists.
        content_type : str, optional, default None
            Optional MIME type for the object.
        metadata : Dict[str, str], optional, default None
            A JSON-encodable dictionary of additional headers to be stored
            as metadata with the object.

        Returns
        -------
        str
            URL of the object stored.
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
        # NOTE: S3 allows fragments as part of object names, e.g. /dataset #1/data.txt
        # Without allow_fragments=False the parsed src.path for an object name with fragments is incomplete.
        src = urlparse(url, allow_fragments=False)
        extra_args = None
        if content_type or metadata or self._encryption:
            extra_args = {}
            if content_type:
                extra_args["ContentType"] = content_type
            if metadata:
                extra_args["Metadata"] = {
                    "metaflow-user-attributes": json.dumps(metadata)
                }
            if self._encryption:
                extra_args["ServerSideEncryption"] = self._encryption

        def _upload(s3, _):
            # We make sure we are at the beginning in case we are retrying
            blob.seek(0)

            # We use manual tracing here because boto3 instrumentation
            # has an issue with upload_fileobj losing track of tracing context
            # https://github.com/open-telemetry/opentelemetry-python-contrib/issues/298
            with tracing.traced("s3.upload_fileobj", {"path": src.path}):
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

    def put_many(
        self,
        key_objs: List[Union[Tuple[str, PutValue], S3PutObject]],
        overwrite: bool = True,
    ) -> List[Tuple[str, str]]:
        """
        Upload many objects to S3.

        Each object to be uploaded can be specified in two ways:

        1. As a `(key, obj)` tuple where `key` is a string specifying
           the path and `obj` is a string or a bytes object.

        2. As a `S3PutObject` which contains additional metadata to be
           stored with the object.

        Parameters
        ----------
        key_objs : List[Union[Tuple[str, PutValue], S3PutObject]]
            List of key-object pairs to upload.
        overwrite : bool, default True
            Overwrite the object if it exists. If set to False, the operation
            succeeds without uploading anything if the key already exists.

        Returns
        -------
        List[Tuple[str, str]]
            List of `(key, url)` pairs corresponding to the objects uploaded.
        """

        def _store():
            for key_obj in key_objs:
                if isinstance(key_obj, S3PutObject):
                    key = key_obj.key
                    obj = key_obj.value
                else:
                    key = key_obj[0]
                    obj = key_obj[1]
                store_info = {
                    "key": key,
                    "content_type": getattr(key_obj, "content_type", None),
                }
                metadata = getattr(key_obj, "metadata", None)
                if metadata:
                    store_info["metadata"] = {
                        "metaflow-user-attributes": json.dumps(metadata)
                    }
                if self._encryption:
                    store_info["encryption"] = self._encryption
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

    def put_files(
        self,
        key_paths: List[Union[Tuple[str, PutValue], S3PutObject]],
        overwrite: bool = True,
    ) -> List[Tuple[str, str]]:
        """
        Upload many local files to S3.

        Each file to be uploaded can be specified in two ways:

        1. As a `(key, path)` tuple where `key` is a string specifying
           the S3 path and `path` is the path to a local file.

        2. As a `S3PutObject` which contains additional metadata to be
           stored with the file.

        Parameters
        ----------
        key_paths :  List[Union[Tuple[str, PutValue], S3PutObject]]
            List of files to upload.
        overwrite : bool, default True
            Overwrite the object if it exists. If set to False, the operation
            succeeds without uploading anything if the key already exists.

        Returns
        -------
        List[Tuple[str, str]]
            List of `(key, url)` pairs corresponding to the files uploaded.
        """

        def _check():
            for key_path in key_paths:
                if isinstance(key_path, S3PutObject):
                    key = key_path.key
                    path = key_path.path
                else:
                    key = key_path[0]
                    path = key_path[1]
                store_info = {
                    "key": key,
                    "content_type": getattr(key_path, "content_type", None),
                }
                metadata = getattr(key_path, "metadata", None)
                if metadata:
                    store_info["metadata"] = {
                        "metaflow-user-attributes": json.dumps(metadata)
                    }
                if self._encryption:
                    store_info["encryption"] = self._encryption
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
                elif error_code == 416:
                    raise MetaflowS3InvalidRange(err)
                elif error_code == "NoSuchBucket":
                    raise MetaflowS3URLException("Specified S3 bucket doesn't exist.")
                error = str(err)
            except OSError as e:
                if e.errno == errno.ENOSPC:
                    raise MetaflowS3InsufficientDiskSpace(str(e))
            except MetaflowException as ex:
                # Re-raise Metaflow exceptions (including TimeoutException)
                raise
            except Exception as ex:
                error = str(ex)
            if tmp:
                os.unlink(tmp.name)
            self._s3_client.reset_client()
            # only sleep if retries > 0
            if S3_RETRY_COUNT > 0:
                self._jitter_sleep(i)
        raise MetaflowS3Exception(
            "S3 operation failed.\n" "Key requested: %s\n" "Error: %s" % (url, error)
        )

    # add some jitter to make sure retries are not synchronized
    def _jitter_sleep(
        self, trynum: int, base: int = 2, cap: int = 360, jitter: float = 0.1
    ) -> None:
        """
        Sleep for an exponentially increasing interval with added jitter.

        Parameters
        ----------
        trynum: The current retry attempt number.
        base: The base multiplier for the exponential backoff.
        cap: The maximum interval to sleep.
        jitter: The maximum jitter percentage to add to the interval.
        """
        # Calculate the exponential backoff interval
        interval = min(cap, base**trynum)

        # Add random jitter
        jitter_value = interval * jitter * random.uniform(-1, 1)
        interval_with_jitter = interval + jitter_value

        # Ensure the interval is not negative
        interval_with_jitter = max(0, interval_with_jitter)

        # Sleep for the calculated interval
        time.sleep(interval_with_jitter)

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
            stdout_lines, stderr = self._s3op_with_retries(
                op, inputs=inputfile.name, **options
            )
            if stderr:
                raise MetaflowS3Exception(
                    "Getting S3 files failed.\n"
                    "First prefix requested: %s\n"
                    "Error: %s" % (prefixes_and_ranges[0], stderr)
                )
            else:
                for line in stdout_lines:
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
            stdout_lines, stderr = self._s3op_with_retries(
                "put",
                inputs=inputfile.name,
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
                for line in stdout_lines:
                    url, _, _ = map(url_unquote, line.strip(b"\n").split(b" "))
                    urls.add(url)
                return [(info["key"], url) for _, url, info in url_info if url in urls]

    def _s3op_with_retries(self, mode, **options):
        from . import s3op

        # High level note on what this function does:
        #  - perform s3op (which calls s3op.py in a subprocess to parallelize the
        #    operation). Typically this operation has several inputs (for example,
        #    multiple files to get or put)
        #  - the result of this operation can be either:
        #    - a known permanent failure (access denied for example) in which case we
        #      return this failure.
        #    - a known transient failure (SlowDown for example) in which case we will
        #      retry *only* the inputs that have this transient failure.
        #    - an unknown failure (something went wrong but we cannot say if it was
        #      a known permanent failure or something else). In this case, we assume
        #      it's a transient failure and retry only those inputs (same as above).
        #
        # NOTES(npow): 2025-05-13
        # Previously, this code would also retry the fatal failures, including no_progress
        # and unknown failures, from the beginning. This is not ideal because:
        # 1. Fatal errors are not supposed to be retried.
        # 2. Retrying from the beginning does not improve the situation, and is
        #    wasteful since we have already uploaded some files.
        # 3. The number of transient errors is far more than fatal errors, so we
        #    can be optimistic and assume the unknown errors are transient.
        cmdline = [sys.executable, os.path.abspath(s3op.__file__), mode]
        recursive_get = False
        for key, value in options.items():
            key = key.replace("_", "-")
            if isinstance(value, bool):
                if value:
                    if mode == "get" and key == "recursive":
                        # We make a note of this because for transient retries, we
                        # don't pass the recursive flag since we already did all the
                        # listing we needed
                        recursive_get = True
                    else:
                        cmdline.append("--%s" % key)
                else:
                    cmdline.append("--no-%s" % key)
            elif key == "inputs":
                base_input_filename = value
            else:
                cmdline.extend(("--%s" % key, value))
        if self._s3_role is not None:
            cmdline.extend(("--s3role", self._s3_role))
        if self._s3_session_vars is not None:
            cmdline.extend(("--s3sessionvars", json.dumps(self._s3_session_vars)))
        if self._s3_client_params is not None:
            cmdline.extend(("--s3clientparams", json.dumps(self._s3_client_params)))

        def _inject_failure_rate():
            # list mode does not do retries on transient failures (there is no
            # SlowDown handling) so we never inject a failure rate
            if mode == "list":
                return 0
            # Otherwise, we cap the failure rate at 90%
            return min(90, self._s3_inject_failures)

        transient_retry_count = 0  # Number of transient retries (per top-level retry)
        inject_failures = _inject_failure_rate()
        out_lines = []  # List to contain the lines returned by _s3op_with_retries
        pending_retries = (
            []
        )  # Inputs that need to be retried due to a transient failure
        loop_count = 0
        last_ok_count = 0  # Number of inputs that were successful in the last try
        total_ok_count = 0  # Total number of OK inputs

        def _reset():
            nonlocal transient_retry_count, inject_failures, out_lines, pending_retries
            nonlocal loop_count, last_ok_count, total_ok_count
            transient_retry_count = 0
            inject_failures = _inject_failure_rate()
            if mode != "put":
                # For put, even after retries, we keep around whatever we already
                # uploaded. This is because uploading with overwrite=False is not
                # an idempotent operation and so some files could be uploaded during
                # the first try which we should report back.
                out_lines = []
            pending_retries = []
            loop_count = 0
            last_ok_count = 0
            total_ok_count = 0  # Reset to zero even if we keep out_lines

        def _update_out_lines(out_lines, ok_lines, resize=False):
            if resize:
                # This is the first time around; we make the list big enough. Typically,
                # there is nothing in out_lines but in some cases (a retry after a
                # partial result), there may be stuff in it
                out_lines.extend([None] * (len(ok_lines) - len(out_lines)))
            for l in ok_lines:
                idx, rest = l.split(b" ", maxsplit=1)
                if rest.decode(encoding="utf-8") != TRANSIENT_RETRY_LINE_CONTENT:
                    # Update the proper location in the out_lines array; we maintain
                    # position as if transient retries did not exist. This
                    # makes sure that order is respected even in the presence of
                    # transient retries.
                    out_lines[int(idx.decode(encoding="utf-8"))] = rest

        def try_s3_op(last_ok_count, pending_retries, out_lines, inject_failures):
            # NOTE: Make sure to update pending_retries and out_lines in place
            addl_cmdline = []
            if len(pending_retries) == 0 and recursive_get:
                # First time around (or after a fatal failure)
                addl_cmdline = ["--recursive"]
            with NamedTemporaryFile(
                dir=self._tmpdir,
                mode="wb+",
                delete=not debug.s3client,
                prefix="metaflow.s3op.stderr.",
            ) as stderr:
                with NamedTemporaryFile(
                    dir=self._tmpdir,
                    mode="wb",
                    delete=not debug.s3client,
                    prefix="metaflow.s3op.transientretry.",
                ) as tmp_input:
                    if len(pending_retries) > 0:
                        # We try a little bit more than the previous success (to still
                        # be aggressive but not too much). If there is a lot of
                        # transient errors and we are having issues pushing through
                        # things, this will shrink more and more until we are doing a
                        # single operation at a time. If things start going better, it
                        # will increase by 20% every round.
                        #
                        # If we made no progress (last_ok_count == 0) we retry at most
                        # 2*S3_WORKER_COUNT from whatever is left in `pending_retries`
                        max_count = min(
                            int(last_ok_count * 1.2), len(pending_retries)
                        ) or min(2 * S3_WORKER_COUNT, len(pending_retries))
                        tmp_input.writelines(pending_retries[:max_count])
                        tmp_input.flush()
                        debug.s3client_exec(
                            "Have %d pending; succeeded in %d => trying for %d and "
                            "leaving %d for the next round"
                            % (
                                len(pending_retries),
                                last_ok_count,
                                max_count,
                                len(pending_retries) - max_count,
                            )
                        )
                        del pending_retries[:max_count]

                        input_filename = tmp_input.name
                    else:
                        input_filename = base_input_filename

                    addl_cmdline.extend(["--inputs", input_filename])

                    # Check if we want to inject failures (for testing)
                    if inject_failures > 0:
                        addl_cmdline.extend(["--inject-failure", str(inject_failures)])
                        # Logic here is to have higher and lower failure rates to try to
                        # exercise as much of the code as possible. The failure rate
                        # trends towards 0.
                        if loop_count % 2 == 0:
                            inject_failures = int(inject_failures / 3)
                        else:
                            # We cap at 90 (and not 100) for injection of failures to
                            # reduce the likelihood of having flaky test. If the
                            # failure injection rate is too high, this can cause actual
                            # retries more often and then lead to too many actual
                            # retries
                            inject_failures = min(90, int(inject_failures * 1.5))
                    try:
                        debug.s3client_exec(cmdline + addl_cmdline)
                        # Run the operation.
                        env = os.environ.copy()
                        tracing.inject_tracing_vars(env)
                        env["METAFLOW_ESCAPE_HATCH_WARNING"] = "False"
                        stdout = subprocess.check_output(
                            cmdline + addl_cmdline,
                            cwd=self._tmpdir,
                            stderr=stderr.file,
                            env=env,
                        )
                        # Here we did not have any error -- transient or otherwise.
                        ok_lines = stdout.splitlines()
                        _update_out_lines(out_lines, ok_lines, resize=loop_count == 0)
                        return (len(ok_lines), 0, inject_failures, None)
                    except subprocess.CalledProcessError as ex:
                        if ex.returncode == s3op.ERROR_TRANSIENT:
                            # In this special case, we failed transiently on *some* of
                            # the files but not necessarily all. This is typically
                            # caused by limits on the number of operations that can
                            # occur per second or some other temporary limitation.
                            # We will retry only those that we failed on and we will not
                            # count this as a retry *unless* we are making no forward
                            # progress. In effect, we consider that as long as *some*
                            # operations are going through, we should just keep going as
                            # if it was a single operation.
                            ok_lines = ex.stdout.splitlines()
                            stderr.seek(0)
                            do_output = False
                            retry_lines = []
                            for l in stderr:
                                if do_output:
                                    retry_lines.append(l)
                                    continue
                                if (
                                    l.decode(encoding="utf-8")
                                    == "%s\n" % TRANSIENT_RETRY_START_LINE
                                ):
                                    # Look for a special marker as the start of the
                                    # "failed inputs that need to be retried"
                                    do_output = True
                            stderr.seek(0)
                            if do_output is False:
                                return (
                                    0,
                                    0,
                                    inject_failures,
                                    "Could not find inputs to retry",
                                )
                            else:
                                _update_out_lines(
                                    out_lines, ok_lines, resize=loop_count == 0
                                )
                                pending_retries.extend(retry_lines)

                                return (
                                    len(ok_lines),
                                    len(retry_lines),
                                    inject_failures,
                                    None,
                                )

                        # Here, this is a "normal" failure that we need to send back up.
                        # These failures are not retried.
                        stderr.seek(0)
                        err_out = stderr.read().decode("utf-8", errors="replace")
                        stderr.seek(0)
                        if ex.returncode == s3op.ERROR_URL_NOT_FOUND:
                            raise MetaflowS3NotFound(err_out)
                        elif ex.returncode == s3op.ERROR_URL_ACCESS_DENIED:
                            raise MetaflowS3AccessDenied(err_out)
                        elif ex.returncode == s3op.ERROR_INVALID_RANGE:
                            raise MetaflowS3InvalidRange(err_out)

                        # Here, this is some other error that we will retry. We still
                        # update the successful lines
                        ok_lines = ex.stdout.splitlines()
                        _update_out_lines(out_lines, ok_lines, resize=loop_count == 0)
                        return 0, 0, inject_failures, err_out

        while transient_retry_count <= S3_TRANSIENT_RETRY_COUNT:
            (
                last_ok_count,
                last_retry_count,
                inject_failures,
                err_out,
            ) = try_s3_op(last_ok_count, pending_retries, out_lines, inject_failures)
            if err_out:
                break
            if last_retry_count != 0:
                # During our last try, we did not manage to process everything we wanted
                # due to a transient failure so we try again.
                transient_retry_count += 1
                total_ok_count += last_ok_count

                if S3_LOG_TRANSIENT_RETRIES:
                    # Extract transient error type from pending retry lines
                    error_info = ""
                    if pending_retries:
                        try:
                            # Parse the first line to get transient error type
                            first_retry = json.loads(
                                pending_retries[0].decode("utf-8").strip()
                            )
                            if "transient_error_type" in first_retry:
                                error_info = (
                                    " (%s)" % first_retry["transient_error_type"]
                                )
                        except (json.JSONDecodeError, IndexError, KeyError):
                            pass

                    print(
                        "Transient S3 failure (attempt #%d) -- total success: %d, "
                        "last attempt %d/%d -- remaining: %d%s"
                        % (
                            transient_retry_count,
                            total_ok_count,
                            last_ok_count,
                            last_ok_count + last_retry_count,
                            len(pending_retries),
                            error_info,
                        )
                    )
                if inject_failures == 0:
                    # Don't sleep when we are "faking" the failures
                    self._jitter_sleep(transient_retry_count)

            loop_count += 1
            # If we have no more things to try, we break out of the loop.
            if len(pending_retries) == 0:
                break

        # At this point, we check out_lines; strip None which can happen for puts that
        # didn't upload files
        return [o for o in out_lines if o is not None], err_out
