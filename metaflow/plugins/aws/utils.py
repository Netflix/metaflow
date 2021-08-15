import sys
import os
import shlex
import tarfile

from distutils.dir_util import copy_tree
from io import BytesIO

from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

from metaflow.datastore.local_backend import LocalBackend
from metaflow import util

from metaflow.mflog import export_mflog_env_vars,\
                           capture_logs,\
                           BASH_SAVE_LOGS


def sync_local_metadata_to_datastore(task_ds):
    # Sync metadata from local metadatastore to datastore, so that scheduler
    # can pick it up and copy to its own local metadata store.

    with util.TempDir() as td:
        tar_file_path = os.path.join(td, 'metadata.tgz')
        with tarfile.open(tar_file_path, 'w:gz') as tar:
            # The local metadata is stored in the local datastore
            # which, for batch jobs, is always the DATASTORE_LOCAL_DIR
            tar.add(DATASTORE_LOCAL_DIR)
        # At this point we store it in the datastore; we
        # save it as raw data in the flow datastore and save a pointer
        # to it as metadata for the task
        with open(tar_file_path, 'rb') as f:
            _, key = task_ds.parent_datastore.save_data([f])[0]
        task_ds.save_metadata({'local_metadata': key})


def sync_local_metadata_from_datastore(metadata, task_datastore):
    # Do nothing if metadata is not local
    if metadata.TYPE != 'local':
        return

    # Otherwise, copy metadata from task datastore to local metadata store
    def echo_none(*args, **kwargs):
        pass
    key_to_load = task_datastore.load_metadata(
        ['local_metadata'])['local_metadata']
    tarball = next(task_datastore.parent_datastore.load_data([key_to_load]))
    with util.TempDir() as td:
        with tarfile.open(fileobj=BytesIO(tarball), mode='r:gz') as tar:
            tar.extractall(td)
        copy_tree(
            os.path.join(td, DATASTORE_LOCAL_DIR),
            LocalBackend.get_datastore_root_from_config(echo_none),
            update=True)


class CommonTaskAttrs:
    def __init__(self,
        flow_name,
        run_id,
        step_name,
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
    mflog_expr = export_mflog_env_vars(flow_name=flow_name,
                                        run_id=run_id,
                                        step_name=step_name,
                                        task_id=task_id,
                                        retry_count=attempt,
                                        datastore_type='s3',
                                        datastore_root=None,
                                        stdout_path=stdout_path,
                                        stderr_path=stderr_path)
    init_cmds = environment.get_package_commands(code_package_url)
    init_expr = ' && '.join(init_cmds)

    stdout_dir = os.path.dirname(stdout_path)
    stderr_dir = os.path.dirname(stderr_path)

    # Below we assume that stdout and stderr file are in the same (log) dir
    assert stdout_dir == stderr_dir

    # construct an entry point that
    # 1) initializes the mflog environment (mflog_expr)
    # 2) bootstraps a metaflow environment (init_expr)

    # the `true` command is to make sure that the generated command
    # plays well with docker containers which have entrypoint set as
    # eval $@
    preamble = 'true && mkdir -p %s && %s && %s' % \
                    (stdout_dir, mflog_expr, init_expr, )

    # execute step and capture logs
    step_expr = capture_logs(' && '.join(environment.bootstrap_commands(step_name) + step_cmds))

    # after the task has finished, we save its exit code (fail/success)
    # and persist the final logs. The whole entrypoint should exit
    # with the exit code (c) of the task.
    #
    # Note that if step_expr OOMs, this tail expression is never executed.
    # We lose the last logs in this scenario (although they are visible
    # still through AWS CloudWatch console).
    cmd_str = '%s && %s; c=$?; %s; exit $c' % (preamble, step_expr, BASH_SAVE_LOGS)
    return shlex.split('bash -c \"%s\"' % cmd_str)