import itertools
import json

from .. import metaflow_config

from .content_addressed_store import ContentAddressedStore
from .task_datastore import TaskDataStore

class FlowDataStore(object):
    default_backend_class = None

    def __init__(self,
                 flow_name,
                 environment,
                 metadata=None,
                 event_logger=None,
                 monitor=None,
                 backend_class=None,
                 ds_root=None):
        """
        Initialize a Flow level datastore.

        This datastore can then be used to get TaskDataStore to store artifacts
        and metadata about a task as well as a ContentAddressedStore to store
        things like packages, etc.

        Parameters
        ----------
        flow_name : str
            The name of the flow
        environment : MetaflowEnvironment
            Environment this datastore is operating in
        metadata : MetadataProvider, optional
            The metadata provider to use and update if needed, by default None
        event_logger : EventLogger, optional
            EventLogger to use to report events, by default None
        monitor : Monitor, optional
            Monitor to use to measure/monitor events, by default None
        backend_class : type
            Class for the backing DataStoreBackend to use; if not provided use
            default_backend_class, optional
        ds_root : str
            The optional root for this datastore; if not provided, use the
            default for the DataStoreBackend, optional
        """
        backend_class = backend_class if backend_class else \
            self.default_backend_class
        if backend_class is None:
            raise RuntimeError("No backend datastore specified")
        self._backend = backend_class(ds_root)
        self.TYPE = self._backend.TYPE

        # Public attributes
        self.flow_name = flow_name
        self.environment = environment
        self.metadata = metadata
        self.logger = event_logger
        self.monitor = monitor

        self.ca_store = ContentAddressedStore(
            self._backend.path_join(self.flow_name, 'data'),
            self._backend)

    @property
    def datastore_root(self):
        return self._backend.datastore_root

    def get_latest_task_datastores(
            self, run_id=None, steps=None, pathspecs=None, allow_not_done=False):
        """Return a list of TaskDataStore for a subset of
        the tasks (consider eventual consistency for some backends like S3)
        for which the latest attempt is done.

        We filter the list based on `steps` if non-None.
        Alternatively, `pathspecs` can contain the exact list of pathspec(s)
        (run_id/step_name/task_id) that should be filtered.
        Note: When `pathspecs` is specified, we expect strict consistency and
        not eventual consistency in contrast to other modes.

        Parameters
        ----------
        run_id : str, optional
            Run ID to get the tasks from. If not specified, use pathspecs,
            by default None
        steps : List[str] , optional
            Steps to get the tasks from. If run_id is specified, this
            must also be specified, by default None
        pathspecs : List[str], optional
            Full task specs (run_id/step_name/task_id). Can be used instead of
            specifiying run_id and steps, by default None
        allow_not_done : bool, optional
            If True, returns the latest attempt of a task even if that attempt
            wasn't marked as done, by default False

        Returns
        -------
        List[TaskDataStore]
            Task datastores for all the tasks specified.
        """
        task_urls = []
        # Note: When `pathspecs` is specified, we avoid the potentially
        # eventually consistent `list_content` operation, and directly construct
        # the task_urls list.
        if pathspecs:
            task_urls = [self._backend.path_join(self.flow_name, pathspec)
                         for pathspec in pathspecs]
        else:
            run_prefix = self._backend.path_join(self.flow_name, run_id)
            if steps:
                step_urls = [self._backend.path_join(run_prefix, step)
                             for step in steps]
            else:
                step_urls = [step.path for step in self._backend.list_content(
                    [run_prefix]) if step.is_file is False]
            task_urls = [task.path for task in self._backend.list_content(
                step_urls) if task.is_file is False]
        urls = []
        for task_url in task_urls:
            for attempt in range(metaflow_config.MAX_ATTEMPTS):
                for suffix in [TaskDataStore.METADATA_DATA_SUFFIX,
                               TaskDataStore.METADATA_ATTEMPT_SUFFIX,
                               TaskDataStore.METADATA_DONE_SUFFIX]:
                    urls.append(self._backend.path_join(
                        task_url,
                        TaskDataStore.metadata_name_for_attempt(suffix, attempt)
                    ))

        latest_started_attempts = {}
        done_attempts = set()
        data_objs = {}
        with self._backend.load_bytes(urls) as get_results:
            for key, path, meta in get_results:
                if path is not None:
                    _, run, step, task, fname = self._backend.path_split(key)
                    attempt, fname = TaskDataStore.parse_attempt_metadata(fname)
                    attempt = int(attempt)
                    if fname == TaskDataStore.METADATA_DONE_SUFFIX:
                        done_attempts.add((run, step, task, attempt))
                    elif fname == TaskDataStore.METADATA_ATTEMPT_SUFFIX:
                        latest_started_attempts[(run, step, task)] = \
                            max(latest_started_attempts.get((run, step, task), 0),
                                attempt)
                    elif fname == TaskDataStore.METADATA_DATA_SUFFIX:
                        # This somewhat breaks the abstraction since we are using
                        # load_bytes directly instead of load_metadata
                        with open(path, 'rb') as f:
                            data_objs[(run, step, task, attempt)] = json.load(f)
        # We now figure out the latest attempt that started *and* finished.
        # Note that if an attempt started but didn't finish, we do *NOT* return
        # the previous attempt
        latest_started_attempts = set(
            (run, step, task, attempt)
            for (run, step, task), attempt in latest_started_attempts.items())
        if allow_not_done:
            latest_to_fetch = latest_started_attempts
        else:
            latest_to_fetch = latest_started_attempts & done_attempts
        latest_to_fetch = [(v[0], v[1], v[2], v[3], data_objs[v], 'r', allow_not_done)
                           for v in latest_to_fetch]
        return list(itertools.starmap(self.get_task_datastore, latest_to_fetch))

    def get_task_datastore(
            self, run_id, step_name, task_id, attempt=None,
            data_metadata=None, mode='r', allow_not_done=False):

        return TaskDataStore(
            self,
            run_id,
            step_name,
            task_id,
            attempt=attempt,
            data_metadata=data_metadata,
            mode=mode,
            allow_not_done=allow_not_done)

    def save_data(self, data_iter):
        """Saves data to the underlying content-addressed store

        Parameters
        ----------
        data : Iterator[bytes]
            Iterator over blobs to save; each item in the list will be saved individually.

        Returns
        -------
        (str, str)
            Tuple containing the URI to access the saved resource as well as
            the key needed to retrieve it using load_data. This is returned in
            the same order as the input.
        """
        save_results = self.ca_store.save_blobs(data_iter, raw=True)
        return [(r.uri, r.key) for r in save_results]

    def load_data(self, keys, force_raw=False):
        """Retrieves data from the underlying content-addressed store

        Parameters
        ----------
        keys : List[str]
            Keys to retrieve
        force_raw : bool, optional
            Backward compatible mode. Raw data will be properly identified with
            metadata information but older datastores did not do this. If you
            know the data should be handled as raw data, set this to True,
            by default False

        Returns
        -------
        Iterator[bytes]
            Iterator over (key, blob) tuples
        """
        for key, blob in self.ca_store.load_blobs(keys, force_raw=force_raw):
            yield key, blob
