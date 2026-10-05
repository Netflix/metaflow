from metaflow.datastore import flow_datastore
from metaflow.datastore.flow_datastore import FlowDataStore


class _FakeCache:
    def __init__(self, loaded=None):
        self.loaded = loaded
        self.stored = []

    def load_metadata(self, run_id, step_name, task_id, attempt):
        return self.loaded

    def store_metadata(self, run_id, step_name, task_id, attempt, metadata):
        self.stored.append((run_id, step_name, task_id, attempt, metadata))


class _FakeTaskDataStore:
    def __init__(self, flow_datastore, run_id, step_name, task_id, **kwargs):
        self.data_metadata = kwargs["data_metadata"]
        self.ds_metadata = kwargs["data_metadata"] or {"objects": {"a": "x"}}


def _flow_datastore(monkeypatch, cache):
    monkeypatch.setattr(flow_datastore, "TaskDataStore", _FakeTaskDataStore)
    fds = FlowDataStore.__new__(FlowDataStore)
    fds._metadata_cache = cache
    fds.TYPE = "local"
    fds.flow_name = "F"
    return fds


def test_caller_supplied_metadata_is_not_cached(monkeypatch):
    # Log-size reads pass empty placeholder metadata. It must not end up in the
    # cache, or later artifact reads for the same attempt see empty metadata.
    cache = _FakeCache()
    fds = _flow_datastore(monkeypatch, cache)

    fds.get_task_datastore(
        "1", "end", "2", attempt=0, data_metadata={"objects": {}, "info": {}}
    )

    assert cache.stored == []


def test_metadata_loaded_from_the_task_is_cached(monkeypatch):
    cache = _FakeCache()
    fds = _flow_datastore(monkeypatch, cache)

    fds.get_task_datastore("1", "end", "2", attempt=0)

    assert cache.stored == [("1", "end", "2", 0, {"objects": {"a": "x"}})]


def test_cache_hit_is_not_stored_again(monkeypatch):
    cache = _FakeCache(loaded={"objects": {"b": "y"}})
    fds = _flow_datastore(monkeypatch, cache)

    fds.get_task_datastore("1", "end", "2", attempt=0)

    assert cache.stored == []
