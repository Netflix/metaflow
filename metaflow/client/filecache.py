from __future__ import print_function
import os
import time
from tempfile import NamedTemporaryFile
from hashlib import sha1

from metaflow.datastore import DATASTORES
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import CLIENT_CACHE_PATH, CLIENT_CACHE_MAX_SIZE

NEW_FILE_QUARANTINE = 10


class FileCacheException(MetaflowException):
    headline = 'File cache error'


class FileCache(object):

    def __init__(self, cache_dir=None, max_size=None):
        self._cache_dir = cache_dir
        self._max_size = max_size
        if self._cache_dir is None:
            self._cache_dir = CLIENT_CACHE_PATH
        if self._max_size is None:
            self._max_size = int(CLIENT_CACHE_MAX_SIZE)
        self._total = 0
        self._objects = None

    def _index_objects(self):
        objects = []
        if os.path.exists(self._cache_dir):
            for subdir in os.listdir(self._cache_dir):
                root = os.path.join(self._cache_dir, subdir)
                if os.path.isdir(root):
                    for obj in os.listdir(root):
                        if obj.endswith('.cached'):
                            path = os.path.join(root, obj)
                            objects.insert(0, (os.path.getctime(path),
                                               os.path.getsize(path),
                                               path))

        self._total = sum(size for _, size, _ in objects)
        self._objects = sorted(objects, reverse=False)

    def _object_path(self, flow_name, run_id, step_name, task_id, name):
        token = os.path.join(flow_name, run_id, step_name, task_id, name).encode('utf-8')
        sha = sha1(token).hexdigest()
        return os.path.join(self._cache_dir, sha[:2], sha + '.cached')

    def _garbage_collect(self):
        now = time.time()
        while self._objects and self._total > self._max_size * 1024**2:
            if now - self._objects[0][0] < NEW_FILE_QUARANTINE:
                break
            ctime, size, path = self._objects.pop(0)
            self._total -= size
            try:
                os.remove(path)
            except OSError:
                # maybe another client had already GC'ed the file away
                pass

    def _makedirs(self, path):
        # this is for python2 compatibility.
        # Python3 has os.makedirs(exist_ok=True).
        try:
            os.makedirs(path)
        except OSError as x:
            if x.errno == 17:
                return
            else:
                raise

    def get_log(self, ds_type, logtype, attempt, flow_name, run_id, step_name, task_id):
        path = self._object_path(flow_name, run_id, step_name, task_id, '_log%s' % logtype)

        def load_func(ds):
            return ds.load_log(logtype, attempt_override=attempt)

        return self._internal_get_data(
            ds_type, flow_name, run_id, step_name, task_id, path, load_func)

    def get_data(self, ds_type, flow_name, sha):
        path = self._object_path(flow_name, '_', '_', '_', sha)

        def load_func(ds):
            return ds.load_data(sha)

        return self._internal_get_data(
            ds_type, flow_name, None, None, None, path, load_func)

    def _internal_get_data(self, ds_type, flow_name, run_id, step_name, task_id, path, load_func):
        ds_cls = DATASTORES.get(ds_type, None)
        if ds_cls is None:
            raise FileCacheException('Datastore %s was not found' % ds_type)

        if ds_cls.datastore_root is None:
            def print_clean(line, **kwargs):
                print(line)
            ds_cls.datastore_root = ds_cls.get_datastore_root_from_config(
                print_clean, create_on_absent=False)

        if ds_cls.datastore_root is None:
            raise FileCacheException('Cannot locate datastore root')

        fileobj = None
        if os.path.exists(path):
            try:
                fileobj = open(path, 'rb')
            except IOError:
                # maybe another client had already GC'ed the file away
                fileobj = None

        if fileobj is None:
            if self._objects is None:
                # index objects lazily at the first request. This can be
                # an expensive operation
                self._index_objects()
            ds = ds_cls(flow_name, run_id, step_name, task_id, mode='d')
            dirname = os.path.dirname(path)
            try:
                self._makedirs(dirname)
            except:  # noqa E722
                raise FileCacheException('Could not create directory: %s' % dirname)

            tmpfile = NamedTemporaryFile(dir=dirname, prefix='s3obj', delete=False)

            try:
                tmpfile.write(load_func(ds))
                os.rename(tmpfile.name, path)
            except:  # noqa E722
                os.unlink(tmpfile.name)
                raise
            size = os.path.getsize(path)
            self._total += size
            self._objects.append((int(time.time()), size, path))
            self._garbage_collect()
            fileobj = open(path, 'rb')
        return fileobj
