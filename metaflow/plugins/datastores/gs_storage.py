import json
import os
import shutil
import uuid
from concurrent.futures import as_completed
from tempfile import mkdtemp


from metaflow.datastore.datastore_storage import DataStoreStorage, CloseAfterUse
from metaflow.exception import MetaflowInternalError
from metaflow.metaflow_config import (
    DATASTORE_SYSROOT_GS,
    ARTIFACT_LOCALROOT,
    GS_STORAGE_WORKLOAD_TYPE,
)

from metaflow.plugins.gcp.gs_storage_client_factory import get_gs_storage_client
from metaflow.plugins.gcp.gs_utils import (
    check_gs_deps,
    parse_gs_full_path,
    process_gs_exception,
)
from metaflow.plugins.storage_executor import (
    StorageExecutor,
    handle_executor_exceptions,
)


class _GSRootClient(object):
    """
    datastore_root aware Google Cloud Storage client. I.e. blob operations are
    all done relative to datastore_root.

    This must be picklable, so methods may be passed across process boundaries.
    """

    def __init__(self, datastore_root):
        if datastore_root is None:
            raise MetaflowInternalError("datastore_root must be set")
        self._datastore_root = datastore_root

    def get_datastore_root(self):
        return self._datastore_root

    def get_blob_full_path(self, path):
        """
        Full path means <blob_prefix>/<path> where:
        datastore_root is gs://<bucket_name>/<blob_prefix>
        """
        _, blob_prefix = parse_gs_full_path(self._datastore_root)
        if blob_prefix is None:
            return path
        path = path.lstrip("/")
        return "/".join([blob_prefix, path])

    def get_bucket_client(self):
        bucket_name, _ = parse_gs_full_path(self._datastore_root)
        client = get_gs_storage_client()
        return client.bucket(bucket_name)

    def get_blob_client(self, path):
        bucket = self.get_bucket_client()
        blob_full_path = self.get_blob_full_path(path)
        blob = bucket.blob(blob_full_path)
        return blob

    # GS blob operations. These are meant to be single units of work
    # to be performed by thread or process pool workers.
    def is_file_single(self, path):
        """Drives GSStorage.is_file()"""
        try:
            blob = self.get_blob_client(path)
            result = blob.exists()

            return result
        except Exception as e:
            process_gs_exception(e)

    def list_content_single(self, path):
        """Drives GSStorage.list_content()"""

        def _trim_result(name, prefix):
            # Remove a prefix from the name, if present
            if prefix is not None and name[: len(prefix)] == prefix:
                name = name[len(prefix) + 1 :]
            return name

        try:
            path = path.rstrip("/") + "/"
            bucket_name, blob_prefix = parse_gs_full_path(self._datastore_root)
            full_path = self.get_blob_full_path(path)
            blobs = get_gs_storage_client().list_blobs(
                bucket_name,
                prefix=full_path,
                delimiter="/",
                include_trailing_delimiter=False,
            )
            result = []
            for b in blobs:
                result.append((_trim_result(b.name, blob_prefix), True))
            for p in blobs.prefixes:
                result.append((_trim_result(p, blob_prefix).rstrip("/"), False))
            return result
        except Exception as e:
            process_gs_exception(e)

    def save_bytes_single(
        self,
        path_tmpfile_metadata_triple,
        overwrite=False,
    ):
        try:
            path, tmpfile, metadata = path_tmpfile_metadata_triple
            blob = self.get_blob_client(path)
            if not overwrite:
                if blob.exists():
                    return
            if metadata is not None:
                blob.metadata = {"metaflow-user-attributes": json.dumps(metadata)}
            from google.cloud.storage.retry import DEFAULT_RETRY

            blob.upload_from_filename(
                tmpfile, retry=DEFAULT_RETRY, timeout=(14400, 60)
            )  # generous timeout for massive uploads. Use the same values as for Azure (connection_timeout, read_timeout)
        except Exception as e:
            process_gs_exception(e)

    def load_bytes_single(self, tmpdir, key):
        """Drives GSStorage.load_bytes()"""
        tmp_filename = os.path.join(tmpdir, str(uuid.uuid4()))
        blob = self.get_blob_client(key)
        metaflow_user_attributes = None
        import google.api_core.exceptions

        try:
            blob.reload()
            if blob.metadata and "metaflow-user-attributes" in blob.metadata:
                metaflow_user_attributes = json.loads(
                    blob.metadata["metaflow-user-attributes"]
                )
            blob.download_to_filename(tmp_filename)
        except google.api_core.exceptions.NotFound:
            tmp_filename = None
        return key, tmp_filename, metaflow_user_attributes


class GSStorage(DataStoreStorage):
    TYPE = "gs"

    @check_gs_deps
    def __init__(self, root=None):
        super(GSStorage, self).__init__(root)
        self._tmproot = ARTIFACT_LOCALROOT
        self._root_client = None

        self._use_processes = GS_STORAGE_WORKLOAD_TYPE == "high_throughput"
        self._executor = StorageExecutor(use_processes=self._use_processes)
        self._executor.warm_up()

    @property
    def root_client(self):
        """Root client is datastore_root aware. All blob operations go through root_client's
        methods.

        Method calls may run on the main thread, or be submitted to a process or thread pool.
        """
        if self._root_client is None:
            self._root_client = _GSRootClient(
                datastore_root=self.datastore_root,
            )
        return self._root_client

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        # create_on_absent doesn't do anything.  This matches S3Storage
        return DATASTORE_SYSROOT_GS

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
        import google.api_core.exceptions

        try:
            blob = self.root_client.get_blob_client(path)
            blob.reload()
            return blob.size
        except google.api_core.exceptions.NotFound:
            return None

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
            tmpdir = mkdtemp(dir=ARTIFACT_LOCALROOT, prefix="metaflow.gs.save_bytes.")
            futures = []
            for path, byte_stream in path_and_bytes_iter:
                metadata = None
                # bytes_stream could actually be (bytes_stream, metadata) instead.
                # Read the small print on DatastoreStorage.save_bytes()
                if isinstance(byte_stream, tuple):
                    byte_stream, metadata = byte_stream
                tmp_filename = os.path.join(tmpdir, str(uuid.uuid4()))
                with open(tmp_filename, "wb") as f:
                    # make sure to close the file handle after reading.
                    with byte_stream as bytes:
                        f.write(bytes.read())
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
        tmpdir = mkdtemp(dir=self._tmproot, prefix="metaflow.gs.load_bytes.")
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
