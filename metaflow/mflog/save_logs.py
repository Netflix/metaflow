import os

# This script is used to upload logs during task bootstrapping, so
# it shouldn't have external dependencies besides Metaflow itself
# (e.g. no click for parsing CLI args).
from metaflow.datastore import FlowDataStore
from metaflow.plugins import DATASTORES
from metaflow.util import Path
from . import TASK_LOG_SOURCE

from metaflow.tracing import cli

SMALL_FILE_LIMIT = 1024 * 1024


@cli("save_logs")
def save_logs():
    def _read_file(path):
        with open(path, "rb") as f:
            return f.read()

    # these env vars are set by mflog.mflog_env
    pathspec = os.environ["MF_PATHSPEC"]
    attempt = os.environ["MF_ATTEMPT"]
    ds_type = os.environ["MF_DATASTORE"]
    ds_root = os.environ.get("MF_DATASTORE_ROOT")
    paths = (os.environ["MFLOG_STDOUT"], os.environ["MFLOG_STDERR"])

    flow_name, run_id, step_name, task_id = pathspec.split("/")
    storage_impl = [d for d in DATASTORES if d.TYPE == ds_type][0]
    if ds_root is None:

        def print_clean(line, **kwargs):
            pass

        ds_root = storage_impl.get_datastore_root_from_config(print_clean)
    flow_datastore = FlowDataStore(
        flow_name, None, storage_impl=storage_impl, ds_root=ds_root
    )
    task_datastore = flow_datastore.get_task_datastore(
        run_id, step_name, task_id, int(attempt), mode="w"
    )

    try:
        streams = ("stdout", "stderr")
        sizes = [
            (stream, path, os.path.getsize(path))
            for stream, path in zip(streams, paths)
            if os.path.exists(path)
        ]

        if max(size for _, _, size in sizes) < SMALL_FILE_LIMIT:
            op = _read_file
        else:
            op = Path

        data = {stream: op(path) for stream, path, _ in sizes}
        task_datastore.save_logs(TASK_LOG_SOURCE, data)
    except:
        # Upload failing is not considered a fatal error.
        # This script shouldn't return non-zero exit codes
        # for transient errors.
        pass


if __name__ == "__main__":
    save_logs()
    # to debug delays in logs, comment the line above and uncomment
    # this snippet:
    """
    import sys
    from metaflow.metaflow_profile import profile
    d = {}
    with profile('save_logs', stats_dict=d):
        save_logs()
    print('Save logs took %dms' % d['save_logs'], file=sys.stderr)
    """
