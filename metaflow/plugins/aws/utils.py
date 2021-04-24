import sys
import os
import shlex
import tarfile

from distutils.dir_util import copy_tree

from metaflow.datastore.util.s3util import get_s3_client
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.datastore import MetaflowDataStore
from metaflow import util
from metaflow.datastore.local import LocalDataStore
from metaflow.mflog import export_mflog_env_vars,\
                           capture_logs,\
                           BASH_SAVE_LOGS

try:
    # python2
    from urlparse import urlparse
except:  # noqa E722
    # python3
    from urllib.parse import urlparse


def sync_local_metadata_to_datastore(datastore_root, attempt):
    # Sync metadata from local metadatastore to datastore, so that scheduler
    # can pick it up and copy to its own local metadata store.
    with util.TempDir() as td:
        tar_file_path = os.path.join(td, 'metadata.tgz')
        with tarfile.open(tar_file_path, 'w:gz') as tar:
            # The local metadata is stored in the local datastore
            # which, for batch jobs, is always the DATASTORE_LOCAL_DIR
            tar.add(DATASTORE_LOCAL_DIR)
        # At this point we upload what need to s3
        s3, _ = get_s3_client()
        with open(tar_file_path, 'rb') as f:
            path = os.path.join(
                datastore_root,
                MetaflowDataStore.filename_with_attempt_prefix(
                    'metadata.tgz', attempt))
            url = urlparse(path)
            s3.upload_fileobj(f, url.netloc, url.path.lstrip('/'))


def sync_local_metadata_from_datastore(metadata, datastore_root, attempt):
    # Do nothing if metadata is not local
    if metadata.TYPE == 'local':
        return

    # Otherwise, copy metadata from task datastore to local metadata store
    def echo_none(*args, **kwargs):
        pass
    path = os.path.join(
        datastore_root,
        MetaflowDataStore.filename_with_attempt_prefix('metadata.tgz', attempt))
    url = urlparse(path)
    bucket = url.netloc
    key = url.path.lstrip('/')
    s3, err = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        # If we are here, we can download the object
        with util.TempDir() as td:
            tar_file_path = os.path.join(td, 'metadata.tgz')
            with open(tar_file_path, 'wb') as f:
                s3.download_fileobj(bucket, key, f)
            with tarfile.open(tar_file_path, 'r:gz') as tar:
                tar.extractall(td)
            copy_tree(
                os.path.join(td, DATASTORE_LOCAL_DIR),
                LocalDataStore.get_datastore_root_from_config(echo_none),
                update=True)
    except err as e:  # noqa F841
        pass


class CommonTaskAttrs:
    def __init__(self,
        flow_name,
        step_name,
        run_id,
        task_id,
        attempt,
        user,
        version,
    ):
        self.flow_name = flow_name
        self.step_name = step_name
        self.run_id = run_id
        self.task_id = task_id
        self.attempt = attempt
        self.user = user
        self.version = version

    def to_dict(self, key_prefix):
        attrs = {
            key_prefix + 'flow_name': self.flow_name,
            key_prefix + 'step_name': self.step_name,
            key_prefix + 'run_id': self.run_id,
            key_prefix + 'task_id': self.task_id,
            key_prefix + 'retry_count': str(self.attempt),
            key_prefix + 'version': self.version,
        }
        if self.user is not None:
            attrs[key_prefix + 'user'] = self.user
        return attrs

def get_writeable_datastore(
    common_task_attrs,
    datastore_class,
):
    return datastore_class(
            mode='w',
            flow_name=common_task_attrs.flow_name,
            step_name=common_task_attrs.step_name,
            run_id=common_task_attrs.run_id,
            task_id=common_task_attrs.task_id,
            attempt=common_task_attrs.attempt
    )

def get_datastore_root(
    common_task_attrs,
    datastore_class,
):
    return datastore_class.make_path(common_task_attrs.flow_name, common_task_attrs.run_id, common_task_attrs.step_name, common_task_attrs.task_id)


def build_task_command(
    environment,
    code_package_url,
    step_name,
    step_cmds,
    flow_name,
    run_id,
    task_id,
    attempt,
    stdout_path,
    stderr_path,
):
    mflog_expr = export_mflog_env_vars(datastore_type='s3',
                                        stdout_path=stdout_path,
                                        stderr_path=stderr_path,
                                        flow_name=flow_name,
                                        step_name=step_name,
                                        run_id=run_id,
                                        task_id=task_id,
                                        retry_count=attempt,
                                        datastore_root=None)
    init_cmds = environment.get_package_commands(code_package_url)
    init_cmds.extend(environment.bootstrap_commands(step_name))
    init_expr = ' && '.join(init_cmds)

    stdout_dir = os.path.dirname(stdout_path)
    stderr_dir = os.path.dirname(stderr_path)

    # Below we assume that stdout and stderr file are in the same (log) dir
    assert stdout_dir == stderr_dir

    # construct an entry point that
    # 1) initializes the mflog environment (mflog_expr)
    # 2) bootstraps a metaflow environment (init_expr)
    preamble = 'mkdir -p %s && %s && %s' % \
                    (stdout_dir, mflog_expr, init_expr, )

    # execute step and capture logs
    step_expr = capture_logs(' && '.join(step_cmds))

    # after the task has finished, we save its exit code (fail/success)
    # and persist the final logs. The whole entrypoint should exit
    # with the exit code (c) of the task.
    #
    # Note that if step_expr OOMs, this tail expression is never executed.
    # We lose the last logs in this scenario (although they are visible
    # still through AWS CloudWatch console).
    cmd_str = '%s && %s; c=$?; %s; exit $c' % (preamble, step_expr, BASH_SAVE_LOGS)
    return shlex.split('bash -c \"%s\"' % cmd_str)