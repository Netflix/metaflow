import os
import shlex
import atexit
import json
import math
import time
import asyncio

from metaflow import util

from metaflow.metaflow_config import (
    SERVICE_INTERNAL_URL,
    SERVICE_HEADERS,
    DEFAULT_METADATA,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    KUBERNETES_SANDBOX_INIT_SCRIPT,
    OTEL_ENDPOINT,
    DEFAULT_SECRETS_BACKEND_TYPE,
    AWS_SECRETS_MANAGER_DEFAULT_REGION,
    S3_SERVER_SIDE_ENCRYPTION,
    S3_ENDPOINT_URL,
)

from metaflow.metaflow_config_funcs import config_values

from metaflow.mflog import (
    export_mflog_env_vars,
    bash_capture_logs,
    BASH_SAVE_LOGS,
    get_log_tailer,
    tail_logs,
)

from .slurm_client import SlurmClient
from .slurm_exceptions import SlurmException, SlurmKilledException
from .slurm_job import SlurmJob

# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)

loop = asyncio.get_event_loop()


class Slurm(object):
    def __init__(
        self,
        datastore,
        metadata,
        environment,
        slurm_access_params,
    ):
        self.datastore = datastore
        self.metadata = metadata
        self.environment = environment
        self.slurm_client = SlurmClient(**slurm_access_params)
        atexit.register(lambda: self.job.kill() if hasattr(self, "job") else None)

    def _job_name(self, user, flow_name, run_id, step_name, task_id, retry_count):
        return "{user}-{flow_name}-{run_id}-{step_name}-{task_id}-{retry_count}".format(
            user=user,
            flow_name=flow_name,
            run_id=str(run_id) if run_id is not None else "",
            step_name=step_name,
            task_id=str(task_id) if task_id is not None else "",
            retry_count=str(retry_count) if retry_count is not None else "",
        )

    def _command(self, environment, code_package_url, step_name, step_cmds, task_spec):
        mflog_expr = export_mflog_env_vars(
            datastore_type=self.datastore.TYPE,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
            **task_spec
        )
        init_cmds = environment.get_package_commands(
            code_package_url, self.datastore.TYPE
        )
        init_expr = " && ".join(init_cmds)
        step_expr = bash_capture_logs(
            " && ".join(
                environment.bootstrap_commands(step_name, self.datastore.TYPE)
                + step_cmds
            )
        )

        # construct an entry point that
        # 1) initializes the mflog environment (mflog_expr)
        # 2) bootstraps a metaflow environment (init_expr)
        # 3) executes a task (step_expr)

        # the `true` command is to make sure that the generated command
        # plays well with docker containers which have entrypoint set as
        # eval $@
        cmd_str = "true && mkdir -p %s && %s && %s && %s; " % (
            LOGS_DIR,
            mflog_expr,
            init_expr,
            step_expr,
        )
        # after the task has finished, we save its exit code (fail/success)
        # and persist the final logs. The whole entrypoint should exit
        # with the exit code (c) of the task.
        #
        # Note that if step_expr OOMs, this tail expression is never executed.
        # We lose the last logs in this scenario.
        cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
        # For supporting sandboxes, ensure that a custom script is executed before
        # anything else is executed. The script is passed in as an env var.
        cmd_str = (
            '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"} && %s'
            % cmd_str
        )

        return shlex.split('bash -c "%s"' % cmd_str)

    def create_job(
        self,
        step_name,
        step_cli,
        task_spec,
        code_package_sha,
        code_package_url,
        code_package_ds,
        partition=None,
        nodes=None,
        ntasks=None,
        ntasks_per_node=None,
        cpus_per_task=None,
        memory=None,
        memory_per_cpu=None,
        constraint=None,
        nodelist=None,
        exclude=None,
        gres=None,
        run_time_limit=None,
        env=None,
        attrs=None,
    ) -> SlurmJob:
        if env is None:
            env = {}
        if attrs is None:
            attrs = {}

        job_name = self._job_name(
            attrs.get("metaflow.user"),
            attrs.get("metaflow.flow_name"),
            attrs.get("metaflow.run_id"),
            attrs.get("metaflow.step_name"),
            attrs.get("metaflow.task_id"),
            attrs.get("metaflow.retry_count"),
        )

        loop.run_until_complete(self.slurm_client.connect())

        slurm_job = (
            SlurmJob(
                client=self.slurm_client,
                name=job_name,
                command=self._command(
                    self.environment, code_package_url, step_name, [step_cli], task_spec
                ),
                loop=loop,
                step_name=step_name,
                step_cli=step_cli,
                task_spec=task_spec,
                code_package_sha=code_package_sha,
                code_package_url=code_package_url,
                code_package_ds=code_package_ds,
                partition=partition,
                nodes=nodes,
                ntasks=ntasks,
                ntasks_per_node=ntasks_per_node,
                cpus_per_task=cpus_per_task,
                memory=memory,
                memory_per_cpu=memory_per_cpu,
                constraint=constraint,
                nodelist=nodelist,
                exclude=exclude,
                gres=gres,
                run_time_limit=run_time_limit,
                env=env,
                attrs=attrs,
            )
            .environment_variable("METAFLOW_CODE_SHA", code_package_sha)
            .environment_variable("METAFLOW_CODE_URL", code_package_url)
            .environment_variable("METAFLOW_CODE_DS", code_package_ds)
            .environment_variable("METAFLOW_USER", attrs["metaflow.user"])
            .environment_variable("METAFLOW_SERVICE_URL", SERVICE_INTERNAL_URL)
            .environment_variable(
                "METAFLOW_SERVICE_HEADERS", json.dumps(SERVICE_HEADERS)
            )
            .environment_variable("METAFLOW_DATASTORE_SYSROOT_S3", DATASTORE_SYSROOT_S3)
            .environment_variable("METAFLOW_DATATOOLS_S3ROOT", DATATOOLS_S3ROOT)
            .environment_variable("METAFLOW_DEFAULT_DATASTORE", self.datastore.TYPE)
            .environment_variable("METAFLOW_DEFAULT_METADATA", DEFAULT_METADATA)
            .environment_variable("METAFLOW_SLURM_WORKLOAD", 1)
            .environment_variable("METAFLOW_RUNTIME_ENVIRONMENT", "slurm")
            .environment_variable(
                "METAFLOW_INIT_SCRIPT", KUBERNETES_SANDBOX_INIT_SCRIPT
            )
            .environment_variable("METAFLOW_OTEL_ENDPOINT", OTEL_ENDPOINT)
        )

        for k, v in config_values():
            if k.startswith("METAFLOW_CONDA_") or k.startswith("METAFLOW_DEBUG_"):
                slurm_job.environment_variable(k, v)

        if DEFAULT_SECRETS_BACKEND_TYPE is not None:
            slurm_job.environment_variable(
                "METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE", DEFAULT_SECRETS_BACKEND_TYPE
            )
        if AWS_SECRETS_MANAGER_DEFAULT_REGION is not None:
            slurm_job.environment_variable(
                "METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION",
                AWS_SECRETS_MANAGER_DEFAULT_REGION,
            )
        if S3_SERVER_SIDE_ENCRYPTION is not None:
            slurm_job.environment_variable(
                "METAFLOW_S3_SERVER_SIDE_ENCRYPTION", S3_SERVER_SIDE_ENCRYPTION
            )
        if S3_ENDPOINT_URL is not None:
            slurm_job.environment_variable("METAFLOW_S3_ENDPOINT_URL", S3_ENDPOINT_URL)

        for name, value in env.items():
            slurm_job.environment_variable(name, value)

        return slurm_job

    def launch_job(self, **kwargs):
        self.job = self.create_job(**kwargs).create().execute()

    def wait(self, stdout_location, stderr_location, echo=None):
        def update_delay(secs_since_start):
            # this sigmoid function reaches
            # - 0.1 after 11 minutes
            # - 0.5 after 15 minutes
            # - 1.0 after 23 minutes
            # in other words, the user will see very frequent updates
            # during the first 10 minutes
            sigmoid = 1.0 / (1.0 + math.exp(-0.01 * secs_since_start + 9.0))
            return 0.5 + sigmoid * 30.0

        def wait_for_launch(job):
            status = job.status

            echo(
                "Task is starting (%s)..." % status,
                "stderr",
                job_id=job.id,
            )
            t = time.time()
            start_time = time.time()
            while job.is_waiting:
                new_status = job.status
                if status != new_status or (time.time() - t) > 30:
                    status = new_status
                    echo(
                        "Task is starting (%s)..." % status,
                        "stderr",
                        job_id=job.id,
                    )
                    t = time.time()
                time.sleep(update_delay(time.time() - start_time))

        _make_prefix = lambda: b"[%s] " % util.to_bytes(self.job.id)

        stdout_tail = get_log_tailer(stdout_location, self.datastore.TYPE)
        stderr_tail = get_log_tailer(stderr_location, self.datastore.TYPE)

        # 1) Loop until the job has started
        wait_for_launch(self.job)

        # 2) Tail logs until the job has finished
        tail_logs(
            prefix=_make_prefix(),
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            echo=echo,
            has_log_updates=lambda: self.job.is_running,
        )

        if self.job.has_failed:
            msg = next(
                msg
                for msg in [
                    self.job.message,
                    "Task crashed.",
                ]
                if msg is not None
            )
            raise SlurmException(
                "%s " "This could be a transient error. " "Use @retry to retry." % msg
            )
        else:
            if self.job.is_running:
                # Kill the job if it is still running by throwing an exception.
                raise SlurmKilledException("Task failed!")
            echo(
                "Task finished with message '%s'." % self.job.message,
                "stderr",
                job_id=self.job.id,
            )
