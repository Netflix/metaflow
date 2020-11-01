"""
Local storage

Store data under .metaflow/ in the cwd
"""
import os
import json
import gzip
from tempfile import NamedTemporaryFile

from metaflow.metaflow_config import DATASTORE_LOCAL_DIR, DATASTORE_SYSROOT_LOCAL
from .datastore import MetaflowDataStore, DataException, only_if_not_done
from ..metadata import MetaDatum


class LocalDataStore(MetaflowDataStore):
    TYPE = 'local'

    METADATA_DIR = '_meta'

    def _makedirs(self, path):
        try:
            os.makedirs(path)
        except OSError as x:
            if x.errno == 17:
                return
            else:
                raise

    def object_path(self, sha):
        root = os.path.join(self.data_root, sha[:2])
        return os.path.join(root, sha)

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        # Compute path for DATASTORE_SYSROOT_LOCAL
        result = DATASTORE_SYSROOT_LOCAL
        if result is None:
            try:
                # Python2
                current_path = os.getcwdu()
            except: # noqa E722
                current_path = os.getcwd()
            check_dir = os.path.join(current_path, DATASTORE_LOCAL_DIR)
            check_dir = os.path.realpath(check_dir)
            orig_path = check_dir
            top_level_reached = False
            while not os.path.isdir(check_dir):
                new_path = os.path.dirname(current_path)
                if new_path == current_path:
                    top_level_reached = True
                    break  # We are no longer making upward progress
                current_path = new_path
                check_dir = os.path.join(current_path, DATASTORE_LOCAL_DIR)
            if top_level_reached:
                if create_on_absent:
                    # Could not find any directory to use so create a new one
                    echo('Creating local datastore in current directory (%s)' % orig_path,
                         fg='magenta', bold=True)
                    os.mkdir(orig_path)
                    result = orig_path
                else:
                    return None
            else:
                result = check_dir
        else:
            result = os.path.join(result, DATASTORE_LOCAL_DIR)
        return result

    @classmethod
    def get_latest_tasks(cls,
                         flow_name,
                         run_id=None,
                         steps=None,
                         pathspecs=None):
        run_prefix = cls.make_path(flow_name, run_id)
        data_blobs = []

        if os.path.exists(run_prefix):
            if steps is None:
                steps = [s for s in os.listdir(run_prefix) if s != cls.METADATA_DIR]
            if pathspecs is None:
                task_prefixes = []
                for step in steps:
                    step_prefix = cls.make_path(flow_name, run_id, step)
                    for task in os.listdir(step_prefix):
                        if task == cls.METADATA_DIR:
                            continue
                        task_prefixes.append(
                            cls.make_path(flow_name, run_id, step, task))
            else:
                task_prefixes = [cls.make_path(flow_name, pathspec)
                                 for pathspec in pathspecs]
            for task_prefix in task_prefixes:
                step, task = task_prefix.split('/')[-2:]
                # Sort the file listing to iterate in increasing order of
                # attempts.
                latest_data_path = None
                latest_attempt = None
                latest_done_attempt = None
                for fname in sorted(os.listdir(task_prefix)):
                    if cls.is_done_filename(fname):
                        _, attempt = cls.parse_filename(fname)
                        latest_done_attempt = attempt
                        # Read the corresponding metadata file.
                        meta_fname = \
                            cls.get_metadata_filename_for_attempt(attempt)
                        latest_data_path = os.path.join(task_prefix, meta_fname)
                    elif cls.is_attempt_filename(fname):
                        _, attempt = cls.parse_filename(fname)
                        latest_attempt = attempt
                # Only read the metadata if the latest attempt is also done.
                if latest_done_attempt is not None and\
                    latest_done_attempt == latest_attempt:
                    with open(latest_data_path) as f:
                        data_blobs.append((step, task, attempt, f.read()))
            return data_blobs
        else:
            raise DataException("Couldn't find data at %s" % run_prefix)

    @classmethod
    def get_artifacts(cls, artifacts_to_prefetch):
        artifact_list = []
        for path in artifacts_to_prefetch:
            sha = path.split('/')[-1]
            artifact_list.append((sha,
                                  cls.decode_gzip_data(path)))
        return artifact_list

    @only_if_not_done
    def save_log(self, logtype, bytebuffer):
        """
        Save a task-specific log file represented as a bytes object.
        """
        path = self.get_log_location(logtype)
        with open(path + '.tmp', 'wb') as f:
            f.write(bytebuffer)
        os.rename(path + '.tmp', path)
        return path

    def load_log(self, logtype, attempt_override=None):
        """
        Load a task-specific log file represented as a bytes object.
        """
        path = self.get_log_location(logtype, attempt_override)
        with open(path, 'rb') as f:
            return f.read()

    @only_if_not_done
    def save_metadata(self, name, metadata):
        """
        Save a task-specific metadata dictionary as JSON.
        """
        self._makedirs(self.root)
        filename = self.filename_with_attempt_prefix('%s.json' % name,
                                                     self.attempt)
        path = os.path.join(self.root, filename)
        with open(path + '.tmp', 'w') as f:
            json.dump(metadata, f)
        os.rename(path + '.tmp', path)

    def load_metadata(self, name):
        """
        Load a task-specific metadata dictionary as JSON.
        """
        filename = self.filename_with_attempt_prefix('%s.json' % name,
                                                     self.attempt)
        path = os.path.join(self.root, filename)
        with open(path) as f:
            return json.load(f)

    def has_metadata(self, name, with_attempt=True):
        attempt = self.attempt if with_attempt else None
        filename = self.filename_with_attempt_prefix('%s.json' % name, attempt)
        path = os.path.join(self.root, filename)
        return os.path.exists(path)

    @only_if_not_done
    def save_data(self, sha, transformable_object):
        """
        Save a content-addressed data blob if it doesn't exist already.
        """
        path = self.object_path(sha)
        if not os.path.exists(path):
            self._makedirs(os.path.dirname(path))
            # NOTE multiple tasks may try to save an object with the
            # same sha concurrently, hence we need to use a proper tmp
            # file
            with NamedTemporaryFile(dir=os.path.dirname(path),
                                    prefix='blobtmp.',
                                    delete=False) as tmp:
                # NOTE compresslevel makes a huge difference. The default
                # level of 9 can be impossibly slow.
                with gzip.GzipFile(fileobj=tmp,
                                   mode='wb',
                                   compresslevel=3) as f:
                    f.write(transformable_object.current())
            os.rename(tmp.name, path)
        return path

    def load_data(self, sha):
        """
        Load a content-addressed data blob.
        """
        with gzip.open(self.object_path(sha), 'rb') as f:
            return f.read()

    @only_if_not_done
    def done(self):
        """
        Write a marker indicating that datastore has finished writing to
        this path.
        """
        filename = self.get_done_filename_for_attempt(self.attempt)
        path = os.path.join(self.root, filename)
        self._makedirs(self.root)
        try:
            # this is for python2 compatibility.
            # Python3 has open(mode='x').
            fd = os.fdopen(os.open(path,
                                   os.O_EXCL | os.O_WRONLY | os.O_CREAT),
                                   'wb')
            fd.close()
        except OSError as x:
            if x.errno == 17:
                raise DataException('Path %s already exists. Try with a '
                                    'different --run-id.' % path)
            else:
                raise
        self.metadata.register_metadata(
            self.run_id, self.step_name, self.task_id,
            [MetaDatum(field='attempt-done', value=str(self.attempt), type='attempt-done', tags=[])])

        self._is_done_set = True

    def is_done(self):
        """
        A flag indicating whether this datastore directory was closed
        succesfully with done().
        """
        filename = self.get_done_filename_for_attempt(self.attempt)
        path = os.path.join(self.root, filename)
        return os.path.exists(path)
