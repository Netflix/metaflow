from io import BytesIO
import os
import shutil
import tarfile

from metaflow import util
from metaflow.plugins.datastores.local_storage import LocalStorage


def copy_tree(src, dst, update=False):
    if not os.path.exists(dst):
        os.makedirs(dst)
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            copy_tree(s, d, update)
        else:
            if (
                update
                and os.path.exists(d)
                and os.path.getmtime(s) <= os.path.getmtime(d)
            ):
                continue
            shutil.copy2(s, d)


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
            util.tar_safe_extract(tar, td)
        copy_tree(
            os.path.join(td, metadata_local_dir),
            LocalStorage.get_datastore_root_from_config(echo_none),
            update=True,
        )
