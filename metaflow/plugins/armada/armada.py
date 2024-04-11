import os
import shlex

from metaflow.mflog import (
    BASH_SAVE_LOGS,
    bash_capture_logs,
    export_mflog_env_vars,
)

# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


class Armada(object):
    def __init__(
        self,
        datastore,
        metadata,
        environment,
    ):
        self._datastore = datastore
        self._metadata = metadata
        self._environment = environment

    def _command(
        self,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
        code_package_url,
        step_cmds,
    ):
        mflog_expr = export_mflog_env_vars(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            retry_count=attempt,
            datastore_type=self._datastore.TYPE,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
        )
        init_cmds = self._environment.get_package_commands(
            code_package_url, self._datastore.TYPE
        )
        init_expr = " && ".join(init_cmds)
        step_expr = bash_capture_logs(
            " && ".join(
                self._environment.bootstrap_commands(
                    step_name, self._datastore.TYPE
                )
                + step_cmds
            )
        )

        # Construct an entry point that
        # 1) initializes the mflog environment (mflog_expr)
        # 2) bootstraps a metaflow environment (init_expr)
        # 3) executes a task (step_expr)

        # The `true` command is to make sure that the generated command
        # plays well with docker containers which have entrypoint set as
        # eval $@
        cmd_str = "true && mkdir -p %s && %s && %s && %s; " % (
            LOGS_DIR,
            mflog_expr,
            init_expr,
            step_expr,
        )
        # After the task has finished, we save its exit code (fail/success)
        # and persist the final logs. The whole entrypoint should exit
        # with the exit code (c) of the task.
        #
        # Note that if step_expr OOMs, this tail expression is never executed.
        # We lose the last logs in this scenario.
        #
        # TODO: Capture hard exit logs in Kubernetes.
        cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
        # For supporting sandboxes, ensure that a custom script is executed before
        # anything else is executed. The script is passed in as an env var.
        cmd_str = (
            '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"} && %s'
            % cmd_str
        )
        return shlex.split('bash -c "%s"' % cmd_str)

    def launch_job(self, **kwargs):
        pass

    def create_job(
        self,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
        user,
        code_package_sha,
        code_package_url,
        code_package_ds,
        step_cli,
        docker_image,
        docker_image_pull_policy,
        service_account=None,
        secrets=None,
        node_selector=None,
        namespace=None,
        cpu=None,
        gpu=None,
        gpu_vendor=None,
        disk=None,
        memory=None,
        use_tmpfs=None,
        tmpfs_tempdir=None,
        tmpfs_size=None,
        tmpfs_path=None,
        run_time_limit=None,
        env=None,
        persistent_volume_claims=None,
        tolerations=None,
        labels=None,
        shared_memory=None,
        port=None,
    ):
        pass

    def wait(self, stdout_location, stderr_location, echo=None):
        pass
