from io import BytesIO
import os
import tarfile

from distutils.dir_util import copy_tree

from metaflow import util
from metaflow.datastore.local_storage import LocalStorage


def sync_local_metadata_to_datastore(metadata_local_dir, task_ds):
    with util.TempDir() as td:
        tar_file_path = os.path.join(td, "metadata.tgz")
        buf = BytesIO()
        with tarfile.open(name=tar_file_path, mode="w:gz", fileobj=buf) as tar:
            tar.add(metadata_local_dir)
        blob = buf.getvalue()
        _, key = task_ds.parent_datastore.save_data([blob], len_hint=1)[0]
        task_ds._dangerous_save_metadata_post_done({"local_metadata": key})


def sync_local_metadata_from_datastore(metadata_local_dir, task_ds):
    def echo_none(*args, **kwargs):
        pass

    key_to_load = task_ds.load_metadata(["local_metadata"])["local_metadata"]
    _, tarball = next(task_ds.parent_datastore.load_data([key_to_load]))
    with util.TempDir() as td:
        with tarfile.open(fileobj=BytesIO(tarball), mode="r:gz") as tar:
            tar.extractall(td)
        copy_tree(
            os.path.join(td, metadata_local_dir),
            LocalStorage.get_datastore_root_from_config(echo_none),
            update=True,
        )
