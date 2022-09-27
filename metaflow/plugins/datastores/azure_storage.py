import math
import platform

import multiprocessing
import json
import os
import shutil
import uuid
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tempfile import mkdtemp

from metaflow.datastore.datastore_storage import DataStoreStorage, CloseAfterUse
from metaflow.exception import MetaflowInternalError, MetaflowException
from metaflow.metaflow_config import (
    DATASTORE_SYSROOT_AZURE,
    ARTIFACT_LOCALROOT,
    AZURE_STORAGE_WORKLOAD_TYPE,
)
from metaflow.plugins.azure.azure_utils import (
    check_azure_deps,
    process_exception,
    handle_exceptions,
    create_static_token_credential,
    parse_azure_full_path,
)

from metaflow.plugins.azure.blob_service_client_factory import (
    get_azure_blob_service_client,
)

if sys.version_info[:2] < (3, 7):
    # in 3.6, Only BrokenProcessPool exists (there is no BrokenThreadPool)
    from concurrent.futures.process import BrokenProcessPool as BrokenExecutor
else:
    # in 3.7 and newer, BrokenExecutor is a base class that parents BrokenProcessPool AND BrokenThreadPool
    from concurrent.futures import BrokenExecutor

# TODO keyboard interrupt crazy tracebacks in process pool


def _determine_effective_cpu_limit():
    """Calculate CPU limit (in number of cores) based on:

    - /sys/fs/cgroup/cpu/cpu.max (if available, cgroup 2)
    OR
    - /sys/fs/cgroup/cpu/cpu.cfs_quota_us
    - /sys/fs/cgroup/cpu/cpu.cfs_period_us

    Returns:
        > 0 if limit was successfully calculated
        = 0 if we determined that there is no limit
        -1 if we failed to determine the limit
    """
    try:
        if platform.system() == "Darwin":
            # On MacOS, and not in a container
            # We are assuming it is extremely out-of-the-way to run a Darwin container
            return 0
        elif platform.system() == "Linux":
            # Bare metal Linux, or a Linux container running on either Linux or MacOS system
            # Linux containers on MacOS have this cpu.max file
            with open("/sys/fs/cgroup/cpu.max", "rb") as f:
                parts = f.read().decode("utf-8").split(" ")
                if len(parts) == 2:
                    if parts[0] == "max":
                        return 0
                    return int(parts[0]) / int(parts[1])

            with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "rb") as f:
                quota = int(f.read())
                # this file shows -1 for no limit
                if quota == -1:
                    return 0
                # some other negative number - this is weird...return "undetermined"
                if quota < 0:
                    return -1
            with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "rb") as f:
                period = int(f.read())
                # should be positive. we don't want div by zero errors.
                if period <= 0:
                    return -1
                return quota / period
        else:
            # Not Linux or MacOS...give up and return "undetermined"
            return -1
    except Exception:
        return -1


def _noop_for_executor_warm_up():
    pass


def _compute_executor_max_workers():
    # For processes, let's start conservative. Let's restrict to 4-18 always. Can be configurable one day.
    min_processes = 4
    max_processes = 18
    # make an effort to get cgroup cpu limits
    effective_cpu_limit = _determine_effective_cpu_limit()

    def _bracket(min_v, v, max_v):
        assert min_v <= max_v
        if v < min_v:
            return min_v
        if v > max_v:
            return max_v
        return v

    if effective_cpu_limit < 0:
        # We don't know the limit, stick to min
        processpool_max_workers = min_processes
    elif effective_cpu_limit == 0:
        # There is (probably) no limit, use physical core count
        processpool_max_workers = _bracket(
            min_processes, os.cpu_count() or 1, max_processes
        )
    else:
        # There is a limit, so let's bracket it within min / max
        processpool_max_workers = _bracket(
            min_processes, math.ceil(effective_cpu_limit), max_processes
        )
    # Threads a lighter than processes... so just tack on a little more
    threadpool_max_workers = processpool_max_workers + 4
    return processpool_max_workers, threadpool_max_workers


class AzureStorageExecutor(object):
    """Thin wrapper around a ProcessPoolExecutor, or a ThreadPoolExecutor where
    the former may be unsafe.
    """

    def __init__(self, use_processes=False):
        (
            processpool_max_workers,
            threadpool_max_workers,
        ) = _compute_executor_max_workers()
        if use_processes:
            mp_start_method = multiprocessing.get_start_method(allow_none=True)
            if mp_start_method == "spawn":
                self._executor = ProcessPoolExecutor(
                    max_workers=processpool_max_workers
                )
            elif sys.version_info[:2] >= (3, 7):
                self._executor = ProcessPoolExecutor(
                    mp_context=multiprocessing.get_context("spawn"),
                    max_workers=processpool_max_workers,
                )
            else:
                raise MetaflowException(
                    msg="Cannot use ProcessPoolExecutor because Python version is older than 3.7 and multiprocess start method has been set to something other than 'spawn'"
                )
        else:
            self._executor = ThreadPoolExecutor(max_workers=threadpool_max_workers)

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


class _AzureRootClient(object):
    """
    This exists independently from AzureBlobStorage as a wrapper around SDK clients.
    It carries around parameters needed to construct Azure SDK clients on demand.

    _AzureRootClient objects will be passed from main to worker threads or processes. They
    must be picklable. We delay constructing Azure SDK objects because they are not picklable.

    For example, azure.core.TokenCredential objects are not picklable. Therefore we pass around an
    AccessToken (token) instead, and construct TokenCredential on demand in the target thread (or process).
    Note that we do this to amortize credential retrieval cost across threads (or processes). Depending on
    the credential methods available to DefaultAzureCredential, credential retrieval can be expensive.
    E.g. Azure CLI based credential may take 500-1000ms.

    _AzureRootClient  also carries around with it blob methods that operate relative to
    datastore_root.
    """

    def __init__(self, datastore_root=None, token=None, shared_access_signature=None):
        if datastore_root is None:
            raise MetaflowInternalError("datastore_root must be set")
        if token is None and shared_access_signature is None:
            raise MetaflowInternalError(
                "either shared_access_signature or token must be set"
            )
        if token and shared_access_signature:
            raise MetaflowInternalError(
                "cannot set both shared_access_signature and token"
            )
        self._datastore_root = datastore_root
        self._token = token
        self._shared_access_signature = shared_access_signature

    def get_datastore_root(self):
        return self._datastore_root

    def get_blob_container_client(self):
        if self._shared_access_signature:
            credential = self._shared_access_signature
            credential_is_cacheable = True
        else:
            credential = create_static_token_credential(self._token)
            credential_is_cacheable = True
        service = get_azure_blob_service_client(
            credential=credential,
            credential_is_cacheable=credential_is_cacheable,
        )
        # datastore_root is <container_name>/<blob_prefix>
        container_name, _ = parse_azure_full_path(self._datastore_root)
        return service.get_container_client(container_name)

    def get_blob_client(self, path):
        container = self.get_blob_container_client()
        blob_full_path = self.get_blob_full_path(path)
        return container.get_blob_client(blob_full_path)

    def get_blob_full_path(self, path):
        """
        Full path means <blob_prefix>/<path> where:
        datastore_root is <container_name>/<blob_prefix>
        """
        _, blob_prefix = parse_azure_full_path(self._datastore_root)
        if blob_prefix is None:
            return path
        path = path.lstrip("/")
        return "/".join([blob_prefix, path])

    # Azure blob operations. These are meant to be single units of work
    # to be performed by thread or process pool workers.
    def is_file_single(self, path):
        """Drives AzureStorage.is_file()"""
        try:
            blob = self.get_blob_client(path)
            return blob.exists()
        except Exception as e:
            process_exception(e)

    def save_bytes_single(
        self,
        path_tmpfile_metadata_triple,
        overwrite=False,
    ):
        """Drives AzureStorage.save_bytes()"""
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
            blob = self.get_blob_client(path)
            from azure.core.exceptions import ResourceExistsError

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

    def load_bytes_single(self, tmpdir, key):
        """Drives AzureStorage.load_bytes()"""
        from azure.core.exceptions import ResourceNotFoundError

        try:
            blob = self.get_blob_client(key)
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

    def list_content_single(self, path):
        """Drives AzureStorage.list_content()"""
        try:
            result = []
            # all query paths are assumed to be folders. This replicates S3 behavior.
            path = path.rstrip("/") + "/"
            full_path = self.get_blob_full_path(path)
            container = self.get_blob_container_client()
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

                # Now massage the resulting blob paths - we need to strip off the common blob prefix
                _, top_level_blob_prefix = parse_azure_full_path(
                    self.get_datastore_root()
                )
                if (
                    top_level_blob_prefix is not None
                    and name[: len(top_level_blob_prefix)] == top_level_blob_prefix
                ):
                    name = name[len(top_level_blob_prefix) + 1 :]

                # DataStorage.list_content_result is not pickle-able, because it is defined
                # inline as a class member. So let's just return a regular tuple. list_content()
                # can pack it up later.
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
        # cannot decorate __init__... invoke it with dummy decoratee
        check_azure_deps(lambda: 0)
        super(AzureStorage, self).__init__(root)
        self._tmproot = ARTIFACT_LOCALROOT
        self._default_scope_token = None
        self._root_client = None

        self._use_processes = AZURE_STORAGE_WORKLOAD_TYPE == "high_throughput"
        self._executor = AzureStorageExecutor(use_processes=self._use_processes)
        self._executor.warm_up()

    @handle_exceptions
    def _get_default_token(self):
        # either we never got a default token, or the one we got is expiring in 5 min
        if not self._default_scope_token or (
            self._default_scope_token.expires_on - time.time() < 300
        ):
            from azure.identity import DefaultAzureCredential

            with DefaultAzureCredential() as credential:
                self._default_scope_token = credential.get_token(
                    AZURE_STORAGE_DEFAULT_SCOPE
                )
        return self._default_scope_token

    @property
    def root_client(self):
        """Note this is for optimization only - it allows slow-initialization credentials to be
        reused across multiple threads and processes.

        Speed up applies mainly to the "no access key" path.
        """
        if self._root_client is None:
            self._root_client = _AzureRootClient(
                datastore_root=self.datastore_root,
                token=self._get_default_token(),
            )
        return self._root_client

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        # create_on_absent doesn't do anything.  This matches S3Storage
        return DATASTORE_SYSROOT_AZURE

    @handle_executor_exceptions
    def is_file(self, paths):
        # preserving order is important...
        futures = [
            self._executor.submit(
                self.root_client.is_file_single,
                path,
            )
            for path in paths
        ]
        # preserving order is important...
        return [future.result() for future in futures]

    def info_file(self, path):
        # not used anywhere... we should consider killing this on all data storage implementations
        raise NotImplementedError()

    def size_file(self, path):
        from azure.core.exceptions import ResourceNotFoundError

        try:
            return self.root_client.get_blob_client(path).get_blob_properties().size
        except ResourceNotFoundError:
            return None
        except Exception as e:
            process_exception(e)

    @handle_executor_exceptions
    def list_content(self, paths):
        futures = [
            self._executor.submit(self.root_client.list_content_single, path)
            for path in paths
        ]
        result = []
        for future in as_completed(futures):
            result.extend(self.list_content_result(*x) for x in future.result())
        return result

    @handle_executor_exceptions
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
                with open(tmp_filename, "wb") as f:
                    f.write(byte_stream.read())
                # Fully finish writing the file, before submitting work. Careful with indentation.

                futures.append(
                    self._executor.submit(
                        self.root_client.save_bytes_single,
                        (path, tmp_filename, metadata),
                        overwrite=overwrite,
                    )
                )
            for future in as_completed(futures):
                future.result()
        finally:
            # *Future* improvement: We could clean up individual tmp files as each future completes
            if tmpdir and os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)

    @handle_executor_exceptions
    def load_bytes(self, keys):

        tmpdir = mkdtemp(dir=self._tmproot, prefix="metaflow.azure.load_bytes.")
        try:
            futures = [
                self._executor.submit(
                    self.root_client.load_bytes_single,
                    tmpdir,
                    key,
                )
                for key in keys
            ]

            # Let's detect any failures fast. Stop and cleanup ASAP.
            # Note that messing up return order is beneficial re: Hyrum's law too.
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
