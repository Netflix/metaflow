import os
import time
import json
import select
import shlex
import time
import re
import hashlib

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
    DATASTORE_CARD_S3ROOT,
)
from metaflow.mflog import (
    export_mflog_env_vars,
    capture_output_to_mflog,
    tail_logs,
    BASH_SAVE_LOGS,
)
from metaflow.mflog.mflog import refine, set_should_persist

from .kubernetes_client import KubernetesClient

# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


class KubernetesException(MetaflowException):
    headline = "Kubernetes error"


class KubernetesKilledException(MetaflowException):
    headline = "Kubernetes Batch job killed"


def generate_rfc1123_name(flow_name, run_id, step_name, task_id, attempt):
    """
    Generate RFC 1123 compatible name. Specifically, the format is:
        <let-or-digit>[*[<let-or-digit-or-hyphen>]<let-or-digit>]

    The generated name consists from a human-readable prefix, derived from
    flow/step/task/attempt, and a hash suffux.
    """
    long_name = "-".join(
        [
            flow_name,
            run_id,
            step_name,
            task_id,
            attempt,
        ]
    )
    hash = hashlib.sha256(long_name.encode("utf-8")).hexdigest()

    if long_name.startswith("_"):
        # RFC 1123 names can't start with hyphen so slap an extra prefix on it
        sanitized_long_name = "u" + long_name.replace("_", "-").lower()
    else:
        sanitized_long_name = long_name.replace("_", "-").lower()

    # the name has to be under 63 chars total
    return sanitized_long_name[:57] + "-" + hash[:5]


LABEL_VALUE_REGEX = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9\-\_\.]{0,61}[a-zA-Z0-9])?$")


def sanitize_label_value(val):
    # Label sanitization: if the value can be used as is, return it as is.
    # If it can't, sanitize and add a suffix based on hash of the original
    # value, replace invalid chars and truncate.
    #
    # The idea here is that even if there are non-allowed chars in the same
    # position, this function will likely return distinct values, so you can
    # still filter on those. For example, "alice$" and "alice&" will be
    # sanitized into different values "alice_b3f201" and "alice_2a6f13".
    if val == "" or LABEL_VALUE_REGEX.match(val):
        return val
    hash = hashlib.sha256(val.encode("utf-8")).hexdigest()

    # Replace invalid chars with dots, and if the first char is
    # non-alphahanumeric, replace it with 'u' to make it valid
    sanitized_val = re.sub("^[^A-Z0-9a-z]", "u", re.sub(r"[^A-Za-z0-9.\-_]", "_", val))
    return sanitized_val[:57] + "-" + hash[:5]


class Kubernetes(object):
    def __init__(
        self,
        datastore,
        metadata,
        environment,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
    ):
        self._datastore = datastore
        self._metadata = metadata
        self._environment = environment

        self._flow_name = flow_name
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._attempt = str(attempt)

    def _command(
        self,
        code_package_url,
        step_cmds,
    ):
        mflog_expr = export_mflog_env_vars(
            flow_name=self._flow_name,
            run_id=self._run_id,
            step_name=self._step_name,
            task_id=self._task_id,
            retry_count=self._attempt,
            datastore_type=self._datastore.TYPE,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
        )
        init_cmds = self._environment.get_package_commands(code_package_url)
        init_expr = " && ".join(init_cmds)
        step_expr = " && ".join(
            [
                capture_output_to_mflog(a)
                for a in (
                    self._environment.bootstrap_commands(self._step_name) + step_cmds
                )
            ]
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
        # TODO: Find a way to capture hard exit logs in Kubernetes.
        cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
        return shlex.split('bash -c "%s"' % cmd_str)

    def launch_job(self, **kwargs):
        self._job = self.create_job(**kwargs).execute()

    def create_job(
        self,
        user,
        code_package_sha,
        code_package_url,
        code_package_ds,
        step_cli,
        docker_image,
        service_account=None,
        secrets=None,
        node_selector=None,
        namespace=None,
        cpu=None,
        gpu=None,
        disk=None,
        memory=None,
        run_time_limit=None,
        env={},
    ):
        # TODO: Test for DNS-1123 compliance. Python names can have underscores
        #       which are not valid Kubernetes names. We can potentially make
        #       the pathspec DNS-1123 compliant by stripping away underscores
        #       etc. and relying on Kubernetes to attach a suffix to make the
        #       name unique within a namespace.
        #
        # Set the pathspec (along with attempt) as the Kubernetes job name.
        # Kubernetes job names are supposed to be unique within a Kubernetes
        # namespace and compliant with DNS-1123. The pathspec (with attempt)
        # can provide that guarantee, however, for flows launched via AWS Step
        # Functions (and potentially Argo), we may not get the task_id or the
        # attempt_id while submitting the job to the Kubernetes cluster. If
        # that is indeed the case, we can rely on Kubernetes to generate a name
        # for us.
        job_name = generate_rfc1123_name(
            self._flow_name,
            self._run_id,
            self._step_name,
            self._task_id,
            self._attempt,
        )

        job = (
            KubernetesClient()
            .job(
                name=job_name,
                namespace=namespace,
                service_account=service_account,
                secrets=secrets,
                node_selector=node_selector,
                command=self._command(
                    code_package_url=code_package_url,
                    step_cmds=[step_cli],
                ),
                image=docker_image,
                cpu=cpu,
                memory=memory,
                disk=disk,
                timeout_in_seconds=run_time_limit,
                # Retries are handled by Metaflow runtime
                retries=0,
            )
            .environment_variable("METAFLOW_CODE_SHA", code_package_sha)
            .environment_variable("METAFLOW_CODE_URL", code_package_url)
            .environment_variable("METAFLOW_CODE_DS", code_package_ds)
            .environment_variable("METAFLOW_USER", user)
            .environment_variable("METAFLOW_SERVICE_URL", BATCH_METADATA_SERVICE_URL)
            .environment_variable(
                "METAFLOW_SERVICE_HEADERS",
                json.dumps(BATCH_METADATA_SERVICE_HEADERS),
            )
            .environment_variable("METAFLOW_DATASTORE_SYSROOT_S3", DATASTORE_SYSROOT_S3)
            .environment_variable("METAFLOW_DATATOOLS_S3ROOT", DATATOOLS_S3ROOT)
            .environment_variable("METAFLOW_DEFAULT_DATASTORE", "s3")
            .environment_variable("METAFLOW_DEFAULT_METADATA", DEFAULT_METADATA)
            .environment_variable("METAFLOW_KUBERNETES_WORKLOAD", 1)
            .environment_variable("METAFLOW_RUNTIME_ENVIRONMENT", "kubernetes")
            .environment_variable("METAFLOW_CARD_S3ROOT", DATASTORE_CARD_S3ROOT)
            .label("app", "metaflow")
            .label("metaflow/flow_name", sanitize_label_value(self._flow_name))
            .label("metaflow/run_id", sanitize_label_value(self._run_id))
            .label("metaflow/step_name", sanitize_label_value(self._step_name))
            .label("metaflow/task_id", sanitize_label_value(self._task_id))
            .label("metaflow/attempt", sanitize_label_value(self._attempt))
        )

        # Skip setting METAFLOW_DATASTORE_SYSROOT_LOCAL because metadata sync
        # between the local user instance and the remote Kubernetes pod
        # assumes metadata is stored in DATASTORE_LOCAL_DIR on the Kubernetes
        # pod; this happens when METAFLOW_DATASTORE_SYSROOT_LOCAL is NOT set (
        # see get_datastore_root_from_config in datastore/local.py).
        for name, value in env.items():
            job.environment_variable(name, value)

        # Add labels to the Kubernetes job
        #
        # Apply recommended labels https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
        #
        # TODO: 1. Verify the behavior of high cardinality labels like instance,
        #          version etc. in the app.kubernetes.io namespace before
        #          introducing them here.
        job.label("app.kubernetes.io/name", "metaflow-task").label(
            "app.kubernetes.io/part-of", "metaflow"
        ).label("app.kubernetes.io/created-by", sanitize_label_value(user))
        # Add Metaflow system tags as labels as well!
        for sys_tag in self._metadata.sticky_sys_tags:
            job.label(
                "metaflow/%s" % sys_tag[: sys_tag.index(":")],
                sanitize_label_value(sys_tag[sys_tag.index(":") + 1 :]),
            )
        # TODO: Add annotations based on https://kubernetes.io/blog/2021/04/20/annotating-k8s-for-humans/

        return job.create()

    def wait(self, stdout_location, stderr_location, echo=None):
        def wait_for_launch(job):
            status = job.status
            echo(
                "Task is starting (Status %s)..." % status,
                "stderr",
                job_id=job.id,
            )
            t = time.time()
            while True:
                new_status = job.status
                if status != new_status or (time.time() - t) > 30:
                    status = new_status
                    echo(
                        "Task is starting (Status %s)..." % status,
                        "stderr",
                        job_id=job.id,
                    )
                    t = time.time()
                if job.is_running or job.is_done:
                    break
                time.sleep(1)

        prefix = b"[%s] " % util.to_bytes(self._job.id)
        stdout_tail = S3Tail(stdout_location)
        stderr_tail = S3Tail(stderr_location)

        # 1) Loop until the job has started
        wait_for_launch(self._job)

        # 2) Tail logs until the job has finished
        tail_logs(
            prefix=prefix,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            echo=echo,
            has_log_updates=lambda: self._job.is_running,
        )

        # 3) Fetch remaining logs
        #
        # It is possible that we exit the loop above before all logs have been
        # shown.
        #
        # TODO (savin): If we notice Kubernetes failing to upload logs to S3,
        #               we can add a HEAD request here to ensure that the file
        #               exists prior to calling S3Tail and note the user about
        #               truncated logs if it doesn't.
        # TODO (savin): For hard crashes, we can fetch logs from the pod.

        if self._job.has_failed:
            exit_code, reason = self._job.reason
            msg = next(
                msg
                for msg in [
                    reason,
                    "Task crashed",
                ]
                if msg is not None
            )
            if exit_code:
                if int(exit_code) == 139:
                    raise KubernetesException("Task failed with a segmentation fault.")
                else:
                    msg = "%s (exit code %s)" % (msg, exit_code)
            raise KubernetesException(
                "%s. This could be a transient error. " "Use @retry to retry." % msg
            )

        exit_code, _ = self._job.reason
        echo(
            "Task finished with exit code %s." % exit_code,
            "stderr",
            job_id=self._job.id,
        )
