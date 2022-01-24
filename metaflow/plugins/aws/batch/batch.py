import atexit
import copy
import json
import os
import select
import shlex
import time

from metaflow import util
from metaflow.datatools.s3tail import S3Tail
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    BATCH_METADATA_SERVICE_URL,
    DATATOOLS_S3ROOT,
    DATASTORE_LOCAL_DIR,
    DATASTORE_SYSROOT_S3,
    DEFAULT_METADATA,
    BATCH_METADATA_SERVICE_HEADERS,
    BATCH_EMIT_TAGS,
    DATASTORE_CARD_S3ROOT,
)
from metaflow.mflog.mflog import refine, set_should_persist
from metaflow.mflog import (
    export_mflog_env_vars,
    capture_output_to_mflog,
    tail_logs,
    BASH_SAVE_LOGS,
)

from .batch_client import BatchClient

# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


class BatchException(MetaflowException):
    headline = "AWS Batch error"


class BatchKilledException(MetaflowException):
    headline = "AWS Batch task killed"


class Batch(object):
    def __init__(self, metadata, environment):
        self.metadata = metadata
        self.environment = environment
        self._client = BatchClient()
        atexit.register(lambda: self.job.kill() if hasattr(self, "job") else None)

    def _command(self, environment, code_package_url, step_name, step_cmds, task_spec):
        mflog_expr = export_mflog_env_vars(
            datastore_type="s3",
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
            **task_spec
        )
        init_cmds = environment.get_package_commands(code_package_url)
        init_expr = " && ".join(init_cmds)
        step_expr = " && ".join(
            [
                capture_output_to_mflog(a)
                for a in (environment.bootstrap_commands(step_name))
            ]
            + step_cmds
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
        # We lose the last logs in this scenario (although they are visible
        # still through AWS CloudWatch console).
        cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
        return shlex.split('bash -c "%s"' % cmd_str)

    def _search_jobs(self, flow_name, run_id, user):
        if user is None:
            regex = "-{flow_name}-".format(flow_name=flow_name)
        else:
            regex = "{user}-{flow_name}-".format(user=user, flow_name=flow_name)
        jobs = []
        for job in self._client.unfinished_jobs():
            if regex in job["jobName"]:
                jobs.append(job["jobId"])
        if run_id is not None:
            run_id = run_id[run_id.startswith("sfn-") and len("sfn-") :]
        for job in self._client.describe_jobs(jobs):
            parameters = job["parameters"]
            match = (
                (user is None or parameters["metaflow.user"] == user)
                and (parameters["metaflow.flow_name"] == flow_name)
                and (run_id is None or parameters["metaflow.run_id"] == run_id)
            )
            if match:
                yield job

    def _job_name(self, user, flow_name, run_id, step_name, task_id, retry_count):
        return "{user}-{flow_name}-{run_id}-{step_name}-{task_id}-{retry_count}".format(
            user=user,
            flow_name=flow_name,
            run_id=str(run_id) if run_id is not None else "",
            step_name=step_name,
            task_id=str(task_id) if task_id is not None else "",
            retry_count=str(retry_count) if retry_count is not None else "",
        )

    def list_jobs(self, flow_name, run_id, user, echo):
        jobs = self._search_jobs(flow_name, run_id, user)
        found = False
        for job in jobs:
            found = True
            echo(
                "{name} [{id}] ({status})".format(
                    name=job["jobName"], id=job["jobId"], status=job["status"]
                )
            )
        if not found:
            echo("No running AWS Batch jobs found.")

    def kill_jobs(self, flow_name, run_id, user, echo):
        jobs = self._search_jobs(flow_name, run_id, user)
        found = False
        for job in jobs:
            found = True
            try:
                self._client.attach_job(job["jobId"]).kill()
                echo(
                    "Killing AWS Batch job: {name} [{id}] ({status})".format(
                        name=job["jobName"],
                        id=job["jobId"],
                        status=job["status"],
                    )
                )
            except Exception as e:
                echo(
                    "Failed to terminate AWS Batch job %s [%s]"
                    % (job["jobId"], repr(e))
                )
        if not found:
            echo("No running AWS Batch jobs found.")

    def create_job(
        self,
        step_name,
        step_cli,
        task_spec,
        code_package_sha,
        code_package_url,
        code_package_ds,
        image,
        queue,
        iam_role=None,
        execution_role=None,
        cpu=None,
        gpu=None,
        memory=None,
        run_time_limit=None,
        shared_memory=None,
        max_swap=None,
        swappiness=None,
        env={},
        attrs={},
        host_volumes=None,
        num_parallel=0,
    ):
        job_name = self._job_name(
            attrs.get("metaflow.user"),
            attrs.get("metaflow.flow_name"),
            attrs.get("metaflow.run_id"),
            attrs.get("metaflow.step_name"),
            attrs.get("metaflow.task_id"),
            attrs.get("metaflow.retry_count"),
        )
        job = (
            self._client.job()
            .job_name(job_name)
            .job_queue(queue)
            .command(
                self._command(
                    self.environment, code_package_url, step_name, [step_cli], task_spec
                )
            )
            .image(image)
            .iam_role(iam_role)
            .execution_role(execution_role)
            .job_def(
                image,
                iam_role,
                queue,
                execution_role,
                shared_memory,
                max_swap,
                swappiness,
                host_volumes=host_volumes,
                num_parallel=num_parallel,
            )
            .cpu(cpu)
            .gpu(gpu)
            .memory(memory)
            .shared_memory(shared_memory)
            .max_swap(max_swap)
            .swappiness(swappiness)
            .timeout_in_secs(run_time_limit)
            .task_id(attrs.get("metaflow.task_id"))
            .environment_variable("AWS_DEFAULT_REGION", self._client.region())
            .environment_variable("METAFLOW_CODE_SHA", code_package_sha)
            .environment_variable("METAFLOW_CODE_URL", code_package_url)
            .environment_variable("METAFLOW_CODE_DS", code_package_ds)
            .environment_variable("METAFLOW_USER", attrs["metaflow.user"])
            .environment_variable("METAFLOW_SERVICE_URL", BATCH_METADATA_SERVICE_URL)
            .environment_variable(
                "METAFLOW_SERVICE_HEADERS", json.dumps(BATCH_METADATA_SERVICE_HEADERS)
            )
            .environment_variable("METAFLOW_DATASTORE_SYSROOT_S3", DATASTORE_SYSROOT_S3)
            .environment_variable("METAFLOW_DATATOOLS_S3ROOT", DATATOOLS_S3ROOT)
            .environment_variable("METAFLOW_DEFAULT_DATASTORE", "s3")
            .environment_variable("METAFLOW_DEFAULT_METADATA", DEFAULT_METADATA)
            .environment_variable("METAFLOW_CARD_S3ROOT", DATASTORE_CARD_S3ROOT)
            .environment_variable("METAFLOW_RUNTIME_ENVIRONMENT", "aws-batch")
        )
        # Skip setting METAFLOW_DATASTORE_SYSROOT_LOCAL because metadata sync between the local user
        # instance and the remote AWS Batch instance assumes metadata is stored in DATASTORE_LOCAL_DIR
        # on the remote AWS Batch instance; this happens when METAFLOW_DATASTORE_SYSROOT_LOCAL
        # is NOT set (see get_datastore_root_from_config in datastore/local.py).
        for name, value in env.items():
            job.environment_variable(name, value)
        if attrs:
            for key, value in attrs.items():
                job.parameter(key, value)
        # Tags for AWS Batch job (for say cost attribution)
        if BATCH_EMIT_TAGS:
            for key in [
                "metaflow.flow_name",
                "metaflow.run_id",
                "metaflow.step_name",
                "metaflow.version",
                "metaflow.run_id.$",
                "metaflow.user",
                "metaflow.owner",
                "metaflow.production_token",
            ]:
                if key in attrs:
                    job.tag(key, attrs.get(key))
        return job

    def launch_job(
        self,
        step_name,
        step_cli,
        task_spec,
        code_package_sha,
        code_package_url,
        code_package_ds,
        image,
        queue,
        iam_role=None,
        execution_role=None,  # for FARGATE compatibility
        cpu=None,
        gpu=None,
        memory=None,
        run_time_limit=None,
        shared_memory=None,
        max_swap=None,
        swappiness=None,
        host_volumes=None,
        num_parallel=0,
        env={},
        attrs={},
    ):
        if queue is None:
            queue = next(self._client.active_job_queues(), None)
            if queue is None:
                raise BatchException(
                    "Unable to launch AWS Batch job. No job queue "
                    " specified and no valid & enabled queue found."
                )
        job = self.create_job(
            step_name,
            capture_output_to_mflog(step_cli),
            task_spec,
            code_package_sha,
            code_package_url,
            code_package_ds,
            image,
            queue,
            iam_role,
            execution_role,
            cpu,
            gpu,
            memory,
            run_time_limit,
            shared_memory,
            max_swap,
            swappiness,
            env=env,
            attrs=attrs,
            host_volumes=host_volumes,
            num_parallel=num_parallel,
        )
        self.num_parallel = num_parallel
        self.job = job.execute()

    def wait(self, stdout_location, stderr_location, echo=None):
        def wait_for_launch(job, child_jobs):
            status = job.status
            echo(
                "Task is starting (status %s)..." % status,
                "stderr",
                batch_id=job.id,
            )
            t = time.time()
            while True:
                if status != job.status or (time.time() - t) > 30:
                    if not child_jobs:
                        child_statuses = ""
                    else:
                        status_keys = set(
                            [child_job.status for child_job in child_jobs]
                        )
                        status_counts = [
                            (
                                status,
                                len(
                                    [
                                        child_job.status == status
                                        for child_job in child_jobs
                                    ]
                                ),
                            )
                            for status in status_keys
                        ]
                        child_statuses = " (parallel node status: [{}])".format(
                            ", ".join(
                                [
                                    "{}:{}".format(status, num)
                                    for (status, num) in sorted(status_counts)
                                ]
                            )
                        )
                    status = job.status
                    echo(
                        "Task is starting (status %s)... %s" % (status, child_statuses),
                        "stderr",
                        batch_id=job.id,
                    )
                    t = time.time()
                if job.is_running or job.is_done or job.is_crashed:
                    break
                select.poll().poll(200)

        prefix = b"[%s] " % util.to_bytes(self.job.id)
        stdout_tail = S3Tail(stdout_location)
        stderr_tail = S3Tail(stderr_location)

        child_jobs = []
        if self.num_parallel > 1:
            for node in range(1, self.num_parallel):
                child_job = copy.copy(self.job)
                child_job._id = child_job._id + "#{}".format(node)
                child_jobs.append(child_job)

        # 1) Loop until the job has started
        wait_for_launch(self.job, child_jobs)

        # 2) Tail logs until the job has finished
        tail_logs(
            prefix=prefix,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            echo=echo,
            has_log_updates=lambda: self.job.is_running,
        )

        # In case of hard crashes (OOM), the final save_logs won't happen.
        # We can fetch the remaining logs from AWS CloudWatch and persist them
        # to Amazon S3.

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
                "%s " "This could be a transient error. " "Use @retry to retry." % msg
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
