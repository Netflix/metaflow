from metaflow.plugins.azure.azure_python_version_check import check_python_version

check_python_version()

import multiprocessing
import json
import os
import shutil
import uuid
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tempfile import mkdtemp

from metaflow import profile
from metaflow.datastore.datastore_storage import DataStoreStorage, CloseAfterUse
from metaflow.exception import MetaflowInternalError, MetaflowException
from metaflow.metaflow_config import (
    DATASTORE_SYSROOT_AZURE,
    AZURE_STORAGE_ACCOUNT_URL,
    ARTIFACT_LOCALROOT,
    AZURE_STORAGE_WORKLOAD_TYPE,
)


if sys.version_info[:2] < (3, 7):
    # in 3.6, Only BrokenProcessPool exists (there is no BrokenThreadPool)
    from concurrent.futures import BrokenProcessPool as BrokenExecutor
else:
    # in 3.7 and newer, BrokenExecutor is a base class that parents BrokenProcessPool AND BrokenThreadPool
    from concurrent.futures import BrokenExecutor

try:

    # Python 3.6 would print lots of warnings about deprecated cryptography usage when importing Azure modules
    import warnings

    warnings.filterwarnings("ignore")
    from azure.identity import DefaultAzureCredential
    from azure.core.credentials import TokenCredential

    from azure.core.exceptions import (
        ResourceNotFoundError,
        ResourceExistsError,
    )
    from metaflow.plugins.azure.azure_client import get_azure_blob_service
    from metaflow.plugins.azure.azure_utils import (
        parse_azure_sysroot,
        process_exception,
        handle_exceptions,
    )
except ImportError:
    raise MetaflowInternalError(
        msg="Please ensure azure-identity and azure-storage-blob Python packages are installed"
    )

# cut down on crazy logging from azure.identity..
# TODO but what if folks want to debug on occasion?
import logging

logging.getLogger("azure.identity").setLevel(logging.ERROR)
logging.getLogger("msrest.serialization").setLevel(logging.ERROR)


# TODO strip this out
def profile_dec(func):
    def inner_func(*args, **kwargs):
        with profile(func.__name__):
            return func(*args, **kwargs)

    return inner_func


# TODO keyboard interrupt crazy tracebacks in process pool
# TODO really understand how keyboardinterrupt works with processpool...

# TODO setup AKS cluster
# TODO run Metadata service (consider going to oleg2)
# TODO run PostgresDB (consider going to oleg2)
# TODO Send jobs to AKS @kubernetes, credentials (probably pull from provisioned environment)
# TODO Setup OB integration tests in BuildKite


def _noop_for_executor_warm_up():
    pass


class AzureStorageExecutor(object):
    """Thin wrapper around a ProcessPoolExecutor, or a ThreadPoolExecutor where
    the former may be unsafe.
    """

    def __init__(self, use_processes=False):
        if use_processes:
            mp_start_method = multiprocessing.get_start_method(allow_none=True)
            if mp_start_method == "spawn":
                self._executor = ProcessPoolExecutor()
                return
            if sys.version_info[:2] >= (3, 7):
                self._executor = ProcessPoolExecutor(
                    mp_context=multiprocessing.get_context("spawn"),
                )
                return
            raise MetaflowException(
                msg="Cannot use ProcessPoolExecutor because Python version is older than 3.7 and multiprocess start method has been set to something other than 'spawn'"
            )
        threadpool_max_workers = 1
        if os.cpu_count():
            # The Azure SDK internally also uses a thread pool.
            # In a thread pool of thread pool situation, let's be more
            # conservative than the default of (CPU_COUNT + 4).
            threadpool_max_workers = max(1, os.cpu_count() // 2)
        self._executor = ThreadPoolExecutor(max_workers=threadpool_max_workers)

    @profile_dec
    def warm_up(self):
        # warm up at least one process or thread in the pool.
        # we don't await future... just let it complete in background
        self._executor.submit(_noop_for_executor_warm_up)

    def submit(self, *args, **kwargs):
        return self._executor.submit(*args, **kwargs)


# How many threads / connections to use per upload or download operation
AZURE_STORAGE_DOWNLOAD_MAX_CONCURRENCY = 4
AZURE_STORAGE_UPLOAD_MAX_CONCURRENCY = 16

BYTES_IN_MB = 1024 * 1024

AZURE_STORAGE_DEFAULT_SCOPE = "https://storage.azure.com/.default"


# TODO this is still not a good name...  Work "root" into it?
class AzureClient(object):
    """
    This exists independently from AzureBlobStorage as a wrapper around SDK client object.
    It carries around parameters needed to construct Azure SDK objects on demand.

    We delay constructing Azure SDK objects because they are not picklable. It means they
    may not be serialized across proces boundaries in a multiprocessing context.

    Either token OR access_key may be provided as credential.
    """

    def __init__(
        self, storage_account_url=None, datastore_root=None, token=None, access_key=None
    ):
        if storage_account_url is None:
            raise MetaflowInternalError("storage_account_url must be set")
        if datastore_root is None:
            raise MetaflowInternalError("datastore_root must be set")
        if token is None and access_key is None:
            raise MetaflowInternalError("either access_key or token must be set")
        if token and access_key:
            raise MetaflowInternalError("cannot set both access_key and token")
        self._storage_account_url = storage_account_url
        self._datastore_root = datastore_root
        self._token = token
        self._access_key = access_key

    def get_datastore_root(self):
        return self._datastore_root

    def get_blob_container_client(self):
        if self._access_key:
            credential = self._access_key
            credential_is_hashable = True
        else:

            class CachedTokenCredential(TokenCredential):
                def __init__(self, token):
                    self._cached_token = token
                    self._credential = None

                def get_token(self, *_scopes, **_kwargs):
                    # We initialize with a fixed token (_cached_token).
                    #
                    # In most cases, we take a fast path - we always just return that fixed token.
                    # I.e. we generate the token once somewhere; subsequent operations use that same token.
                    #
                    # The fixed token can expire (defaults to several hours, but can be configured by an Azure admin)
                    #
                    # If we detect token expiration, we delegate all future token needs back to DefaultAzureCredential,
                    # which similarly supports token caching and regeneration of expired tokens.
                    #
                    # The net result is that we only generate new tokens when absolutely necessary.
                    #
                    # This dance is needed because DefaultAzureCredential is not thread or multiprocess safe
                    # and we want token reuse for performance reasons.  Token generation through DefaultAzureCredential
                    # may be slow, depending on the exact underlying method used (e.g. Azure CLI is particularly slow).
                    #
                    # https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python
                    if (self._cached_token.expires_on - time.time()) < 300:
                        self._credential = DefaultAzureCredential()
                    if self._credential:
                        return self._credential.get_token(*_scopes, **_kwargs)
                    return self._cached_token

                # Implement __hash__ and __eq__ so the service object becomes cacheable
                def __hash__(self):
                    return hash(self._cached_token)

                def __eq__(self, other):
                    return self._cached_token == other._cached_token

            credential = CachedTokenCredential(self._token)
            credential_is_hashable = True
        service = get_azure_blob_service(
            self._storage_account_url,
            credential=credential,
            credential_is_hashable=credential_is_hashable,
        )

        container_name, _ = parse_azure_sysroot(self._datastore_root)
        return service.get_container_client(container_name)

    def get_blob_client(self, path):
        container = self.get_blob_container_client()
        blob_full_path = self.get_blob_full_path(path)
        return container.get_blob_client(blob_full_path)

    def get_blob_full_path(self, path):
        _, blob_prefix = parse_azure_sysroot(self._datastore_root)
        if blob_prefix is None:
            return path
        path = path.lstrip("/")
        return "/".join([blob_prefix, path])


# Azure blob operations. These are meant to be single units of work
# to be performed by thread or process pool workers.
def _is_file_single(path, client=None):
    """Drives AzureStorage.is_file()"""
    with profile("SUB_is_file_single"):
        try:
            blob = client.get_blob_client(path)
            return blob.exists()
        except Exception as e:
            process_exception(e)


def _save_bytes_single(
    path_tmpfile_metadata_triple,
    overwrite=False,
    client=None,
):
    """Drives AzureStorage.save_bytes()"""
    with profile("SUB_save_bytes_single"):
        try:
            path, tmpfile, metadata = path_tmpfile_metadata_triple

            metadata_to_upload = None
            if metadata:
                metadata_to_upload = {
                    # Azure metadata rules:
                    # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-properties-metadata#set-and-retrieve-metadata
                    # https://docs.microsoft.com/en-us/rest/api/storageservices/setting-and-retrieving-properties-and-metadata-for-blob-resources#Subheading1
                    "metaflow_user_attributes": json.dumps(metadata),
                }
            blob = client.get_blob_client(path)
            with open(tmpfile, "rb") as byte_stream:
                try:
                    # This is a racy existence check worth doing.
                    # It is good enough 99.9% of the time.
                    # Depending on ResourceExistsError is more costly, though
                    # we are still going to handle it right.
                    if overwrite or not blob.exists():
                        blob.upload_blob(
                            byte_stream,
                            overwrite=overwrite,
                            metadata=metadata_to_upload,
                            max_concurrency=AZURE_STORAGE_UPLOAD_MAX_CONCURRENCY,
                        )
                except ResourceExistsError:
                    if overwrite:
                        # this is an unexpected condition - operation should not complain about
                        # resource exists if we already said it's fine to overwrite
                        raise
                    else:
                        # we did not want to overwrite. We swallow the exception because the behavior we
                        # want is "try to upload, but just no-op if already exists".
                        # this is consistent with S3 and Local implementations.
                        #
                        # Note: In other implementations, we may do a pre-upload object existence check.
                        # Race conditions are possible in those implementations - and that appears to
                        # be tolerated by our datastore usage patterns.
                        #
                        # For Azure, we let azure-storage-blob and underlying REST API handle this. It looks
                        # race free (as of 6/28/2022)
                        pass
        except Exception as e:
            process_exception(e)


def _load_bytes_single(tmpdir, key, client=None):
    """Drives AzureStorage.load_bytes()"""
    with profile("SUB_load_bytes_single"):

        try:
            blob = client.get_blob_client(key)
            try:
                blob_properties = blob.get_blob_properties()
            except ResourceNotFoundError:
                # load_bytes() needs to return None for keys that don't exist
                return key, None, None
            tmp_filename = os.path.join(tmpdir, str(uuid.uuid4()))
            try:
                with open(tmp_filename, "wb") as f:
                    blob.download_blob(
                        max_concurrency=AZURE_STORAGE_DOWNLOAD_MAX_CONCURRENCY
                    ).readinto(f)
                metaflow_user_attributes = None
                if (
                    blob_properties.metadata
                    and "metaflow_user_attributes" in blob_properties.metadata
                ):
                    metaflow_user_attributes = json.loads(
                        blob_properties.metadata["metaflow_user_attributes"]
                    )
            except Exception:
                # clean up the tmp file for the one specific failed load
                if os.path.exists(tmp_filename):
                    os.unlink(tmp_filename)
                raise
            return key, tmp_filename, metaflow_user_attributes
        except Exception as e:
            process_exception(e)


def _list_content_single(path, client=None):
    """Drives AzureStorage.list_content()"""
    with profile("SUB_list_content_single"):
        try:
            result = []
            # all query paths are assumed to be folders. This replicates S3 behavior.
            path = path.rstrip("/") + "/"
            full_path = client.get_blob_full_path(path)
            container = client.get_blob_container_client()
            # "file" blobs show up as BlobProperties. We assume we always have "blob_type" key
            # "directories" show up as BlobPrefix. We assume we never have "blob_type" key
            for blob_properties_or_prefix in container.walk_blobs(
                name_starts_with=full_path
            ):
                name = blob_properties_or_prefix.name
                # there are other ways. Like checking the returned name ends with slash.
                # But checking blob_type is more robust
                is_file = blob_properties_or_prefix.has_key("blob_type")
                if not is_file:
                    # for directories we don't want trailing slashes in results
                    name = name.rstrip("/")
                _, top_level_blob_prefix = parse_azure_sysroot(
                    client.get_datastore_root()
                )
                if (
                    top_level_blob_prefix is not None
                    and name[: len(top_level_blob_prefix)] == top_level_blob_prefix
                ):
                    name = name[len(top_level_blob_prefix) + 1 :]

                # DataStorage.list_content is not pickle-able, because it is defined inline as a class member.
                # So let's just return a regular tuple.
                # list_content() can pack it up later.
                # TODO(jackie) Why is it defined as a class member at all? Probably should not be.
                result.append((name, is_file))
            return result
        except Exception as e:
            process_exception(e)


def handle_executor_exceptions(func):
    """
    Decorator for handling errors that come from an Executor. This decorator should
    only be used on functions where executor errors are possible. I.e. the function
    uses AzureStorageExecutor.
    """

    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BrokenExecutor:
            # This is fatal. So we bail ASAP.
            # We also don't want to log, because KeyboardInterrupt on worker processes
            # also take us here, so it's going to be "normal" user operation most of the
            # time.
            # BrokenExecutor parents both BrokenThreadPool and BrokenProcessPool.
            sys.exit(1)

    return inner_function


class AzureStorage(DataStoreStorage):
    TYPE = "azure"

    def __init__(self, root=None):
        if not AZURE_STORAGE_ACCOUNT_URL:
            raise MetaflowException(
                msg="Must configure METAFLOW_AZURE_STORAGE_ACCOUNT_URL"
            )
        super(AzureStorage, self).__init__(root)
        self._tmproot = ARTIFACT_LOCALROOT
        self._default_scope_token = None

        # TODO test this again!
        self._use_processes = AZURE_STORAGE_WORKLOAD_TYPE == "high_throughput"
        self._executor = AzureStorageExecutor(use_processes=self._use_processes)
        self._executor.warm_up()

    @handle_exceptions
    @profile_dec
    def _get_default_token(self):
        # either we never got a default token, or the one we got is expiring in 5 min
        if not self._default_scope_token or (
            self._default_scope_token.expires_on - time.time() < 300
        ):
            with DefaultAzureCredential() as credential:
                self._default_scope_token = credential.get_token(
                    AZURE_STORAGE_DEFAULT_SCOPE
                )
        return self._default_scope_token

    def _get_client(self):
        # We take this only from environment, but not from metaflow JSON configs.
        # There is no precedent for storing secret information there.
        # This could be
        # - storage account access key
        # - shared access token ("SAS")
        # TODO do we need to broaden the name here?
        access_key = os.getenv("METAFLOW_AZURE_STORAGE_ACCESS_KEY", default=None)
        if access_key:
            return AzureClient(
                storage_account_url=AZURE_STORAGE_ACCOUNT_URL,
                datastore_root=self.datastore_root,
                access_key=access_key,
            )
        else:
            return AzureClient(
                storage_account_url=AZURE_STORAGE_ACCOUNT_URL,
                datastore_root=self.datastore_root,
                token=self._get_default_token(),
            )

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        # create_on_absent doesn't do anything.  This matches S3Storage
        return DATASTORE_SYSROOT_AZURE

    @handle_executor_exceptions
    @profile_dec
    def is_file(self, paths):
        # preserving order is important...
        futures = [
            self._executor.submit(
                _is_file_single,
                path,
                client=self._get_client(),
            )
            for path in paths
        ]
        # preserving order is important...
        with profile("is_file_result_wait[%d]" % len(futures)):
            return [future.result() for future in futures]

    def info_file(self, path):
        # not used anywhere... we should consider killing this on all data storage implementations
        raise NotImplementedError()

    @profile_dec
    def size_file(self, path):
        try:
            client = self._get_client()
            return client.get_blob_client(path).get_blob_properties().size
        except Exception as e:
            process_exception(e)

    @handle_executor_exceptions
    @profile_dec
    def list_content(self, paths):
        futures = [
            self._executor.submit(_list_content_single, path, client=self._get_client())
            for path in paths
        ]
        result = []
        for future in as_completed(futures):
            result.extend(self.list_content_result(*x) for x in future.result())
        return result

    @handle_executor_exceptions
    @profile_dec
    def save_bytes(self, path_and_bytes_iter, overwrite=False, len_hint=0):
        tmpdir = None
        try:
            tmpdir = mkdtemp(
                dir=ARTIFACT_LOCALROOT, prefix="metaflow.azure.save_bytes."
            )
            futures = []
            for path, byte_stream in path_and_bytes_iter:
                metadata = None
                # bytes_stream could actually be (bytes_stream, metadata) instead.
                # Read the small print on DatastoreStorage.save_bytes()
                if isinstance(byte_stream, tuple):
                    byte_stream, metadata = byte_stream
                tmp_filename = os.path.join(tmpdir, str(uuid.uuid4()))
                with profile("save_bytes_write_tmp"):
                    with open(tmp_filename, "wb") as f:
                        f.write(byte_stream.read())
                # Fully finish writing the file, before submitting work. Careful with indentation.

                with profile("save_bytes_submit"):
                    futures.append(
                        self._executor.submit(
                            _save_bytes_single,
                            (path, tmp_filename, metadata),
                            overwrite=overwrite,
                            client=self._get_client(),
                        )
                    )
            with profile("save_bytes_result_wait[%d]" % len(futures)):
                for future in as_completed(futures):
                    future.result()
        finally:
            # *Future* improvement: We could clean up individual tmp files as each future completes
            if tmpdir and os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)

    @handle_executor_exceptions
    @profile_dec
    def load_bytes(self, keys):

        tmpdir = mkdtemp(dir=self._tmproot, prefix="metaflow.azure.load_bytes.")
        try:
            futures = [
                self._executor.submit(
                    _load_bytes_single,
                    tmpdir,
                    key,
                    client=self._get_client(),
                )
                for key in keys
            ]

            # Let's detect any failures fast. Stop and cleanup ASAP.
            # Note that messing up return order is beneficial re: Hyrum's law too.
            with profile("load_bytes_result_wait[%d]" % len(futures)):
                items = [future.result() for future in as_completed(futures)]
        except Exception:
            # Other BaseExceptions will skip clean up here.
            # We let this go - smooth exit in those circumstances is more important than cleaning up a few tmp files
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
            raise

        class _Closer(object):
            @staticmethod
            def close():
                if os.path.isdir(tmpdir):
                    shutil.rmtree(tmpdir)

        return CloseAfterUse(iter(items), closer=_Closer)
