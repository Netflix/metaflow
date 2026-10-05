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


def test_log_size_read_does_not_break_artifact_size(tmp_path):
    # End to end with a real local datastore and the real client cache: reading
    # stdout_size first must not leave a later DataArtifact.size broken.
    import os
    import subprocess
    import sys

    flow = tmp_path / "flow.py"
    flow.write_text(
        "from metaflow import FlowSpec, step\n"
        "class CacheExample(FlowSpec):\n"
        "    @step\n"
        "    def start(self):\n"
        "        self.result = 123\n"
        "        self.next(self.end)\n"
        "    @step\n"
        "    def end(self):\n"
        "        print('hello')\n"
        "if __name__ == '__main__':\n"
        "    CacheExample()\n"
    )
    check = tmp_path / "check.py"
    check.write_text(
        "from metaflow import DataArtifact, Flow, Task, namespace\n"
        "namespace(None)\n"
        "ps = Flow('CacheExample').latest_run['start'].task.pathspec\n"
        "task = Task(ps, attempt=0)\n"
        "artifact = DataArtifact(ps + '/result', attempt=0)\n"
        "first = artifact.size\n"
        "task.stdout_size\n"
        "assert artifact.data == 123\n"
        "assert artifact.size == first and first > 0, (first, artifact.size)\n"
    )
    env = dict(
        os.environ,
        METAFLOW_DEFAULT_METADATA="local",
        METAFLOW_DEFAULT_DATASTORE="local",
        METAFLOW_DATASTORE_SYSROOT_LOCAL=str(tmp_path / "ds"),
        METAFLOW_CLIENT_CACHE_PATH=str(tmp_path / "client-cache"),
        METAFLOW_USER="tester",
    )
    for args in (
        [str(flow), "--metadata=local", "--datastore=local", "run"],
        [str(check)],
    ):
        subprocess.run(
            [sys.executable] + args,
            cwd=tmp_path,
            env=env,
            check=True,
            capture_output=True,
        )
