import os
import json
import gzip
import time
import random
import sys

try:
    # python2
    from urlparse import urlparse
    import cStringIO
    BytesIO = cStringIO.StringIO
except:
    # python3
    from urllib.parse import urlparse
    import io
    BytesIO = io.BytesIO
import itertools
from metaflow.metaflow_config import get_authenticated_gcs_client
from metaflow.exception import MetaflowException
from .datastore import MetaflowDataStore, only_if_not_done
from ..metadata import MetaDatum


GCS_NUM_RETRIES = 7


# decorator to retry functions that access GCS
def gcs_retry(f):
    def retry_wrapper(self, *args, **kwargs):
        last_exc = None
        for i in range(GCS_NUM_RETRIES):
            try:
                return f(self, *args, **kwargs)
            except MetaflowException as ex:
                # MetaflowExceptions are not related to GCS, don't retry
                raise
            except Exception as ex:
                try:
                    function_name = f.func_name
                except AttributeError:
                    function_name = f.__name__
                sys.stderr.write("GCS datastore operation %s failed (%s). "
                                 "Retrying %d more times..\n"
                                 % (function_name, ex, GCS_NUM_RETRIES - i))
                self.reset_client(hard_reset=True)
                last_exc = ex
                # exponential backoff
                time.sleep(2**i + random.randint(0, 5))
        raise last_exc
    return retry_wrapper


class GCSDataStore(MetaflowDataStore):
    TYPE='gcs'

    client = None

    def __init__(self, *args, **kwargs):
        self.reset_client()
        super(GCSDataStore, self).__init__(*args, **kwargs)

    @classmethod
    def reset_client(cls, hard_reset=False):
        if cls.client is None or hard_reset:
            cls.client = get_authenticated_gcs_client()

    @classmethod
    def filename_with_attempt_prefix(cls, name, attempt):
        """
        Return the equivalent filename for `name` depending
        whether an attempt prefix must be used, if `attempt` isn't None.
        """
        if attempt is None:
            return name
        else:
            return '%d.%s' % (attempt, name)

    @gcs_retry
    def _get_gcs_object(self, path, return_buf=False):
        url = urlparse(path)
        bucket_name, blob_name = url.netloc, url.path.lstrip('/')
        buf = BytesIO()
        self.client.bucket(bucket_name).blob(blob_name).download_to_file(buf)
        if return_buf:
            buf.seek(0)
            return buf
        return buf.getvalue()

    @gcs_retry
    def _put_gcs_object(self, path, blob=None, buf=None):
        url = urlparse(path)
        bucket_name, blob_name = url.netloc, url.path.lstrip('/')
        if buf is None:
            buf = BytesIO(blob)
        self.client.bucket(bucket_name).blob(blob_name).upload_from_file(buf)

    @gcs_retry
    def _exists_gcs_object(self, path):
        url = urlparse(path)
        bucket_name, blob_name = url.netloc, url.path.lstrip('/')
        return self.client.bucket(bucket_name).blob(blob_name).exists()

    @classmethod
    def get_bucket_and_base(cls, path):
        url = urlparse(path)
        bucket_name, base_path = url.netloc, url.path.lstrip('/')
        return bucket_name, base_path

    # Datastore needs to implement the methods below
    @only_if_not_done
    def save_metadata(self, name, data):
        """
        Save a task-specific metadata dictionary as JSON.
        """
        filename = self.filename_with_attempt_prefix('%s.json' % name, self.attempt)
        path = os.path.join(self.root, filename)
        self._put_gcs_object(path, blob=json.dumps(data).encode('utf-8'))

    def load_metadata(self, name):
        """
        Load a task-specific metadata dictionary as JSON.
        """
        filename = self.filename_with_attempt_prefix('%s.json' % name,
                                                     self.attempt)
        path = os.path.join(self.root, filename)
        byte_results = self._get_gcs_object(path)
        return json.loads(byte_results.decode('utf-8'))

    def has_metadata(self, name, with_attempt=True):
        """
        Return True if this metadata file exists.
        """
        attempt = self.attempt if with_attempt else None
        filename = self.filename_with_attempt_prefix('%s.json' % name,
                                                     attempt)
        path = os.path.join(self.root, filename)
        return self._exists_gcs_object(path)

    def object_path(self, sha):
        root = os.path.join(self.data_root, sha[:2])
        return os.path.join(root, sha)

    @only_if_not_done
    def save_data(self, sha, transformable_object):
        """
        Save a content-addressed data blob if it doesn't exist already.
        """
        path = self.object_path(sha)
        if not self._exists_gcs_object(path):
            buf = BytesIO()
            # NOTE compresslevel makes a huge difference. The default
            # level of 9 can be impossibly slow.
            with gzip.GzipFile(fileobj=buf,
                               mode='wb',
                               compresslevel=3) as f:
                f.write(transformable_object.current())
            transformable_object.transform(lambda _: buf)
            buf.seek(0)
            self._put_gcs_object(path, buf=buf)
        return path

    def load_data(self, sha):
        """
        Load a content-addressed data blob.
        """
        path = self.object_path(sha)
        buf = self._get_gcs_object(path, return_buf=True)
        return self.decode_gzip_data(None, buf) # filename=None

    @only_if_not_done
    def save_log(self, logtype, bytebuffer):
        """
        Save a log file represented as a bytes object.
        Returns path to log file.
        """
        path = self.get_log_location(logtype)
        self._put_gcs_object(path, blob=bytebuffer)
        return path

    def load_log(self, logtype, attempt_override=None):
        """
        Load a task-specific log file represented as a bytes object.
        """
        path = self.get_log_location(logtype, attempt_override)
        return self._get_gcs_object(path)

    @only_if_not_done
    def done(self):
        """
        Write a marker indicating that datastore has finished writing to
        this path.
        """
        filename = self.get_done_filename_for_attempt(self.attempt)
        path = os.path.join(self.root, filename)
        self._put_gcs_object(path)

        self.metadata.register_metadata(
            self.run_id, self.step_name, self.task_id,
            [MetaDatum(field='attempt-done', value=str(self.attempt), type='attempt-done')])

        self._is_done_set = True

    def is_done(self):
        """
        A flag indicating whether this datastore directory was closed
        succesfully with done().
        """
        filename = self.get_done_filename_for_attempt(self.attempt)
        path = os.path.join(self.root, filename)
        return self._exists_gcs_object(path)

    @classmethod
    def get_latest_tasks(cls,
                         flow_name,
                         run_id=None,
                         steps=None,
                         pathspecs=None):
        """
        Return a list of (step, task, attempt, metadata_blob) for a subset of
        the tasks (consider eventual consistency) for which the latest attempt
        is done for the input `flow_name, run_id`.
        We filter the list based on `steps` if non-None.
        Alternatively, `pathspecs` can contain the exact list of pathspec(s)
        (run_id/step_name/task_id) that should be filtered.
        Note: When `pathspecs` is specified, we expect strict consistency and
        not eventual consistency in contrast to other modes.
        """
        client = get_authenticated_gcs_client()

        def blob_for_path(path):
            url = urlparse(path)
            bucket_name, blob_name = url.netloc, url.path.lstrip('/')
            return client.bucket(bucket_name).blob(blob_name)

        def list_blobs_for_path(path):
            url = urlparse(path)
            bucket_name, prefix = url.netloc, url.path.lstrip('/')
            return client.list_blobs(bucket_name, prefix=prefix)

        run_prefix = cls.make_path(flow_name, run_id)
        if pathspecs:
            blobs = itertools.chain(*(list_blobs_for_path(cls.make_path(flow_name, pathspec=p)) for p in pathspecs))
        elif steps:
            blobs = itertools.chain(*(
                list_blobs_for_path(path)
                for path in [cls.make_path(flow_name, run_id, step) for step in steps]
            ))
        else:
            blobs = list_blobs_for_path(run_prefix)

        valid_endings = [ending for attempt in range(5) for ending in
                         [cls.get_metadata_filename_for_attempt(attempt),
                          cls.get_filename_for_attempt(attempt),
                          cls.get_done_filename_for_attempt(attempt)]]
        valid_blobs = [b for b in blobs if any([b.name.endswith(ending)
                       for ending in valid_endings])]

        all_data_blobs = {}
        latest_attempt = {}
        done_attempts = set()
        for b in valid_blobs:
            path = b.name
            step_name, task_id, fname = path.split('/')[-3:]
            _, attempt = cls.parse_filename(fname)
            if cls.is_done_filename(fname):
                done_attempts.add((step_name, task_id, attempt))
            elif cls.is_attempt_filename(fname):
                # files are in sorted order, so overwrite is ok???
                if (step_name, task_id) not in latest_attempt or attempt > latest_attempt[(step_name, task_id)]:
                    latest_attempt[(step_name, task_id)] = attempt
            else:
                # is_metadata_filename(fname) == True.
                all_data_blobs[(step_name, task_id, attempt)] = b.download_as_string()

        latest_attempts = set((step_name, task_id, attempt)
                               for (step_name, task_id), attempt
                                   in latest_attempt.items())
        latest_and_done = latest_attempts & done_attempts
        return [(step_name, task_id, attempt,
                 all_data_blobs[(step_name, task_id, attempt)])
                 for step_name, task_id, attempt in latest_and_done]

    @classmethod
    def get_artifacts(cls, artifacts_to_prefetch):
        """
        Return a list of (sha, obj_blob) for all the object_path(s) specified in
        `artifacts_to_prefetch`.
        """
        artifact_list = []

        client = get_authenticated_gcs_client()
        def blob_for_path(path):
            url = urlparse(path)
            bucket_name, blob_name = url.netloc, url.path.lstrip('/')
            return client.bucket(bucket_name).blob(blob_name)

        blobs = [blob_for_path(path) for path in artifacts_to_prefetch]
        for b in blobs:
            sha = b.name.split('/')[-1]
            buf = BytesIO()
            b.download_to_file(buf)
            buf.seek(0)
            artifact_list.append((sha, cls.decode_gzip_data(None, buf)))
        return artifact_list
