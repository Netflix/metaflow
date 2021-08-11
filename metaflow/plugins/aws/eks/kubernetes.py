import os
import time
import json
import select
import atexit
import shlex
import time
import warnings

from metaflow import util
from metaflow.datastore.util.s3tail import S3Tail
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    BATCH_METADATA_SERVICE_URL,
    DATATOOLS_S3ROOT,
    DATASTORE_LOCAL_DIR,
    DATASTORE_SYSROOT_S3,
    DEFAULT_METADATA,
    BATCH_METADATA_SERVICE_HEADERS,
)
from metaflow.mflog import (
    export_mflog_env_vars,
    bash_capture_logs,
    update_delay,
    BASH_SAVE_LOGS,
)
from metaflow.mflog.mflog import refine, set_should_persist

from .kubernetes_client import KubernetesClient

# Redirect structured logs to /logs/
LOGS_DIR = "/logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


class KubernetesException(MetaflowException):
    headline = "Kubernetes error"


class KubernetesKilledException(MetaflowException):
    headline = "Kubernetes Batch job killed"


class Kubernetes(object):
    def __init__(
        self,
        datastore,
        metadata,
        environment,
    ):
        self.datastore = datastore
        self.metadata = metadata
        self.environment = environment

        # TODO: Issue a kill request for all pending Kubernetes Batch jobs at exit.
        # atexit.register(
        #     lambda: self.job.kill() if hasattr(self, 'job') else None)

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
        print("lo")
        mflog_expr = export_mflog_env_vars(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            retry_count=attempt,
            datastore_type=self.datastore.TYPE,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
        )
        return ["echo", "hello"]
        init_cmds = self.environment.get_package_commands(code_package_url)
        init_expr = " && ".join(init_cmds)
        step_expr = bash_capture_logs(
            " && ".join(
                self.environment.bootstrap_commands(step_name) + step_cmds
            )
        )

        # Construct an entry point that
        # 1) initializes the mflog environment (mflog_expr)
        # 2) bootstraps a metaflow environment (init_expr)
        # 3) executes a task (step_expr)

        # The `true` command is to make sure that the generated command
        # plays well with docker containers which have entrypoint set as
        # eval $@
        cmd_str = "true && mkdir -p /logs && %s && %s && %s; " % (
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
        # TODO: Find a way to capture hard exit logs.
        cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
        print("lo")
        return shlex.split('bash -c "%s"' % cmd_str)

    def _name(self, user, flow_name, run_id, step_name, task_id, attempt):
        print("hi")
        return (
            "{user}-{flow_name}-{run_id}-"
            "{step_name}-{task_id}-{attempt}".format(
                user=user,
                flow_name=flow_name,
                run_id=str(run_id) if run_id is not None else "",
                step_name=step_name,
                task_id=str(task_id) if task_id is not None else "",
                attempt=str(attempt) if attempt is not None else "",
            ).lower()
        )

    def create_job(
        self,
        user,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
        code_package_sha,
        code_package_url,
        code_package_ds,
        step_cli,
        docker_image,
        service_account=None,
        cpu=None,
        gpu=None,
        memory=None,
        run_time_limit=None,
        env={},
    ):
        job = (
            KubernetesClient()
            .job()
            .create(
                name=self._name(
                    user=user,
                    flow_name=flow_name,
                    run_id=run_id,
                    step_name=step_name,
                    task_id=task_id,
                    attempt=attempt,
                )
            )
        )

        print(job)
        print("lk")
        # job.name("hello")
        print("lk1")
        print(job)
        print("yahoo")
        job.name(
            self._name(
                user=user,
                flow_name=flow_name,
                run_id=run_id,
                step_name=step_name,
                task_id=task_id,
                attempt=attempt,
            )
        ).command(
            self._command(
                flow_name=flow_name,
                run_id=run_id,
                step_name=step_name,
                task_id=task_id,
                attempt=attempt,
                code_package_url=code_package_url,
                step_cmds=[step_cli],
            )
        ).image(
            docker_image
        ).service_account(
            "s3-full-access"
        ).cpu(
            cpu
        ).gpu(
            gpu
        ).memory(
            memory
        ).timeout_in_secs(
            run_time_limit
        ).environment_variable(
            "AWS_DEFAULT_REGION", "us-west-2"
        ).environment_variable(
            "METAFLOW_CODE_SHA", code_package_sha
        ).environment_variable(
            "METAFLOW_CODE_URL", code_package_url
        ).environment_variable(
            "METAFLOW_CODE_DS", code_package_ds
        ).environment_variable(
            "METAFLOW_USER", user
        ).environment_variable(
            "METAFLOW_SERVICE_URL", BATCH_METADATA_SERVICE_URL
        ).environment_variable(
            "METAFLOW_SERVICE_HEADERS",
            json.dumps(BATCH_METADATA_SERVICE_HEADERS),
        ).environment_variable(
            "METAFLOW_DATASTORE_SYSROOT_S3", DATASTORE_SYSROOT_S3
        ).environment_variable(
            "METAFLOW_DATATOOLS_S3ROOT", DATATOOLS_S3ROOT
        ).environment_variable(
            "METAFLOW_DEFAULT_DATASTORE", "s3"
        ).environment_variable(
            "METAFLOW_DEFAULT_METADATA", DEFAULT_METADATA
        ).annotation(
            "metaflow-flow-name", flow_name
        ).annotation(
            "metaflow-run-id", run_id
        ).annotation(
            "metaflow-step-name", step_name
        ).annotation(
            "metaflow-task-id", task_id
        ).annotation(
            "metaflow-attempt", attempt
        ).annotation(
            "metaflow-user", user
        )
        # Skip setting METAFLOW_DATASTORE_SYSROOT_LOCAL because metadata
        # sync between the local user instance and the remote Kubernetes
        # instance assumes metadata is stored in DATASTORE_LOCAL_DIR
        # on the remote Kubernetes instance; this happens when
        # METAFLOW_DATASTORE_SYSROOT_LOCAL is NOT set (see
        # get_datastore_root_from_config in datastore/local.py).
        for name, value in env.items():
            job.environment_variable(name, value)

        # A Container in a Pod may fail for a number of reasons, such as
        # because the process in it exited with a non-zero exit code, or the
        # Container was killed due to OOM etc. If this happens, fail the pod
        # and let Metaflow handle the retries.
        job.restart_policy("Never").retries(0)

        return job

    def wait(self, job, stdout_location, stderr_location, echo=None):
        self.job = job

        def wait_for_launch(job):
            status = job.status
            echo(
                "Task is starting (status %s)..." % status,
                "stderr",
                job_id=job.id,
            )
            t = time.time()
            while True:
                if status != job.status or (time.time() - t) > 30:
                    status = job.status
                    echo(
                        "Task is starting (status %s)..." % status,
                        "stderr",
                        job_id=job.id,
                    )
                    t = time.time()
                if job.is_running or job.is_done or job.is_crashed:
                    break
                select.poll().poll(200)

        prefix = b"[%s] " % util.to_bytes(self.job.id)

        def _print_available(tail, stream, should_persist=False):
            # print the latest batch of lines from S3Tail
            try:
                for line in tail:
                    if should_persist:
                        line = set_should_persist(line)
                    else:
                        line = refine(line, prefix=prefix)
                    echo(line.strip().decode("utf-8", errors="replace"), stream)
            except Exception as ex:
                echo(
                    "[ temporary error in fetching logs: %s ]" % ex,
                    "stderr",
                    batch_id=self.job.id,
                )

        stdout_tail = S3Tail(stdout_location)
        stderr_tail = S3Tail(stderr_location)

        # 1) Loop until the job has started
        wait_for_launch(self.job)

        # 2) Loop until the job has finished
        start_time = time.time()
        is_running = True
        next_log_update = start_time
        log_update_delay = 1

        while is_running:
            if time.time() > next_log_update:
                _print_available(stdout_tail, "stdout")
                _print_available(stderr_tail, "stderr")
                now = time.time()
                log_update_delay = update_delay(now - start_time)
                next_log_update = now + log_update_delay
                is_running = self.job.is_running

            # This sleep should never delay log updates. On the other hand,
            # we should exit this loop when the task has finished without
            # a long delay, regardless of the log tailing schedule
            d = min(log_update_delay, 5.0)
            select.poll().poll(d * 1000)

        # 3) Fetch remaining logs
        #
        # It is possible that we exit the loop above before all logs have been
        # shown.
        #
        # TODO if we notice AWS Batch failing to upload logs to S3, we can add a
        # HEAD request here to ensure that the file exists prior to calling
        # S3Tail and note the user about truncated logs if it doesn't
        _print_available(stdout_tail, "stdout")
        _print_available(stderr_tail, "stderr")
        # In case of hard crashes (OOM), the final save_logs won't happen.
        # We fetch the remaining logs from AWS CloudWatch and persist them to
        # Amazon S3.
        #
        # TODO: AWS CloudWatch fetch logs

        if self.job.is_crashed:
            msg = next(
                msg
                for msg in [
                    self.job.reason,
                    self.job.status_reason,
                    "Task crashed.",
                ]
                if msg is not None
            )
            raise BatchException(
                "%s "
                "This could be a transient error. "
                "Use @retry to retry." % msg
            )
        else:
            if self.job.is_running:
                # Kill the job if it is still running by throwing an exception.
                raise BatchException("Task failed!")
            echo(
                "Task finished with exit code %s." % self.job.status_code,
                "stderr",
                batch_id=self.job.id,
            )
