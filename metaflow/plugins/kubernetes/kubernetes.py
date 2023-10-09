import json
import math
import os
import re
import shlex
import time
from typing import Dict, List, Optional

from metaflow import current, util
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    ARGO_EVENTS_EVENT,
    ARGO_EVENTS_EVENT_BUS,
    ARGO_EVENTS_EVENT_SOURCE,
    ARGO_EVENTS_SERVICE_ACCOUNT,
    ARGO_EVENTS_INTERNAL_WEBHOOK_URL,
    AWS_SECRETS_MANAGER_DEFAULT_REGION,
    ARGO_EVENTS_WEBHOOK_AUTH,
    AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
    CARD_AZUREROOT,
    CARD_GSROOT,
    CARD_S3ROOT,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    DEFAULT_AWS_CLIENT_PROVIDER,
    DEFAULT_METADATA,
    DEFAULT_SECRETS_BACKEND_TYPE,
    KUBERNETES_FETCH_EC2_METADATA,
    KUBERNETES_LABELS,
    KUBERNETES_SANDBOX_INIT_SCRIPT,
    S3_ENDPOINT_URL,
    SERVICE_HEADERS,
    SERVICE_INTERNAL_URL,
    S3_SERVER_SIDE_ENCRYPTION,
)
from metaflow.metaflow_config_funcs import config_values

from metaflow.mflog import (
    BASH_SAVE_LOGS,
    bash_capture_logs,
    export_mflog_env_vars,
    get_log_tailer,
    tail_logs,
)

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


class Kubernetes(object):
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
                self._environment.bootstrap_commands(step_name, self._datastore.TYPE)
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
        self._job = self.create_job(**kwargs).execute()

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
    ):
        if env is None:
            env = {}

        job = (
            KubernetesClient()
            .job(
                generate_name="t-",
                namespace=namespace,
                service_account=service_account,
                secrets=secrets,
                node_selector=node_selector,
                command=self._command(
                    flow_name=flow_name,
                    run_id=run_id,
                    step_name=step_name,
                    task_id=task_id,
                    attempt=attempt,
                    code_package_url=code_package_url,
                    step_cmds=[step_cli],
                ),
                image=docker_image,
                image_pull_policy=docker_image_pull_policy,
                cpu=cpu,
                memory=memory,
                disk=disk,
                gpu=gpu,
                gpu_vendor=gpu_vendor,
                timeout_in_seconds=run_time_limit,
                # Retries are handled by Metaflow runtime
                retries=0,
                step_name=step_name,
                tolerations=tolerations,
                labels=self._get_labels(labels),
                use_tmpfs=use_tmpfs,
                tmpfs_tempdir=tmpfs_tempdir,
                tmpfs_size=tmpfs_size,
                tmpfs_path=tmpfs_path,
                persistent_volume_claims=persistent_volume_claims,
            )
            .environment_variable("METAFLOW_CODE_SHA", code_package_sha)
            .environment_variable("METAFLOW_CODE_URL", code_package_url)
            .environment_variable("METAFLOW_CODE_DS", code_package_ds)
            .environment_variable("METAFLOW_USER", user)
            .environment_variable("METAFLOW_SERVICE_URL", SERVICE_INTERNAL_URL)
            .environment_variable(
                "METAFLOW_SERVICE_HEADERS",
                json.dumps(SERVICE_HEADERS),
            )
            .environment_variable("METAFLOW_DATASTORE_SYSROOT_S3", DATASTORE_SYSROOT_S3)
            .environment_variable("METAFLOW_DATATOOLS_S3ROOT", DATATOOLS_S3ROOT)
            .environment_variable("METAFLOW_DEFAULT_DATASTORE", self._datastore.TYPE)
            .environment_variable("METAFLOW_DEFAULT_METADATA", DEFAULT_METADATA)
            .environment_variable("METAFLOW_KUBERNETES_WORKLOAD", 1)
            .environment_variable(
                "METAFLOW_KUBERNETES_FETCH_EC2_METADATA", KUBERNETES_FETCH_EC2_METADATA
            )
            .environment_variable("METAFLOW_RUNTIME_ENVIRONMENT", "kubernetes")
            .environment_variable(
                "METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE", DEFAULT_SECRETS_BACKEND_TYPE
            )
            .environment_variable("METAFLOW_CARD_S3ROOT", CARD_S3ROOT)
            .environment_variable(
                "METAFLOW_DEFAULT_AWS_CLIENT_PROVIDER", DEFAULT_AWS_CLIENT_PROVIDER
            )
            .environment_variable(
                "METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION",
                AWS_SECRETS_MANAGER_DEFAULT_REGION,
            )
            .environment_variable("METAFLOW_S3_ENDPOINT_URL", S3_ENDPOINT_URL)
            .environment_variable(
                "METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT",
                AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
            )
            .environment_variable(
                "METAFLOW_DATASTORE_SYSROOT_AZURE", DATASTORE_SYSROOT_AZURE
            )
            .environment_variable("METAFLOW_CARD_AZUREROOT", CARD_AZUREROOT)
            .environment_variable("METAFLOW_DATASTORE_SYSROOT_GS", DATASTORE_SYSROOT_GS)
            .environment_variable("METAFLOW_CARD_GSROOT", CARD_GSROOT)
            # support Metaflow sandboxes
            .environment_variable(
                "METAFLOW_INIT_SCRIPT", KUBERNETES_SANDBOX_INIT_SCRIPT
            )
            # Skip setting METAFLOW_DATASTORE_SYSROOT_LOCAL because metadata sync
            # between the local user instance and the remote Kubernetes pod
            # assumes metadata is stored in DATASTORE_LOCAL_DIR on the Kubernetes
            # pod; this happens when METAFLOW_DATASTORE_SYSROOT_LOCAL is NOT set (
            # see get_datastore_root_from_config in datastore/local.py).
        )

        # Temporary passing of *some* environment variables. Do not rely on this
        # mechanism as it will be removed in the near future
        for k, v in config_values():
            if k.startswith("METAFLOW_CONDA_") or k.startswith("METAFLOW_DEBUG_"):
                job.environment_variable(k, v)

        if S3_SERVER_SIDE_ENCRYPTION is not None:
            job.environment_variable(
                "METAFLOW_S3_SERVER_SIDE_ENCRYPTION", S3_SERVER_SIDE_ENCRYPTION
            )

        # Set environment variables to support metaflow.integrations.ArgoEvent
        job.environment_variable(
            "METAFLOW_ARGO_EVENTS_WEBHOOK_URL", ARGO_EVENTS_INTERNAL_WEBHOOK_URL
        )
        job.environment_variable("METAFLOW_ARGO_EVENTS_EVENT", ARGO_EVENTS_EVENT)
        job.environment_variable(
            "METAFLOW_ARGO_EVENTS_EVENT_BUS", ARGO_EVENTS_EVENT_BUS
        )
        job.environment_variable(
            "METAFLOW_ARGO_EVENTS_EVENT_SOURCE", ARGO_EVENTS_EVENT_SOURCE
        )
        job.environment_variable(
            "METAFLOW_ARGO_EVENTS_SERVICE_ACCOUNT", ARGO_EVENTS_SERVICE_ACCOUNT
        )
        job.environment_variable(
            "METAFLOW_ARGO_EVENTS_WEBHOOK_AUTH",
            ARGO_EVENTS_WEBHOOK_AUTH,
        )

        tmpfs_enabled = use_tmpfs or (tmpfs_size and not use_tmpfs)
        if tmpfs_enabled and tmpfs_tempdir:
            job.environment_variable("METAFLOW_TEMPDIR", tmpfs_path)

        for name, value in env.items():
            job.environment_variable(name, value)

        annotations = {
            "metaflow/user": user,
            "metaflow/flow_name": flow_name,
        }
        if current.get("project_name"):
            annotations.update(
                {
                    "metaflow/project_name": current.project_name,
                    "metaflow/branch_name": current.branch_name,
                    "metaflow/project_flow_name": current.project_flow_name,
                }
            )

        for name, value in annotations.items():
            job.annotation(name, value)

        (
            job.annotation("metaflow/run_id", run_id)
            .annotation("metaflow/step_name", step_name)
            .annotation("metaflow/task_id", task_id)
            .annotation("metaflow/attempt", attempt)
            .label("app.kubernetes.io/name", "metaflow-task")
            .label("app.kubernetes.io/part-of", "metaflow")
        )

        return job.create()

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

        prefix = b"[%s] " % util.to_bytes(self._job.id)

        stdout_tail = get_log_tailer(stdout_location, self._datastore.TYPE)
        stderr_tail = get_log_tailer(stderr_location, self._datastore.TYPE)

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
        # TODO : If we notice Kubernetes failing to upload logs to S3,
        #        we can add a HEAD request here to ensure that the file
        #        exists prior to calling S3Tail and note the user about
        #        truncated logs if it doesn't.
        # TODO : For hard crashes, we can fetch logs from the pod.

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
                if int(exit_code) == 137:
                    raise KubernetesException(
                        "Task ran out of memory. "
                        "Increase the available memory by specifying "
                        "@resource(memory=...) for the step. "
                    )
                if int(exit_code) == 134:
                    raise KubernetesException("%s (exit code %s)" % (msg, exit_code))
                else:
                    msg = "%s (exit code %s)" % (msg, exit_code)
            raise KubernetesException(
                "%s. This could be a transient error. Use @retry to retry." % msg
            )

        exit_code, _ = self._job.reason
        echo(
            "Task finished with exit code %s." % exit_code,
            "stderr",
            job_id=self._job.id,
        )

    @staticmethod
    def _get_labels(extra_labels=None):
        if extra_labels is None:
            extra_labels = {}
        env_labels = KUBERNETES_LABELS.split(",") if KUBERNETES_LABELS else []
        env_labels = parse_kube_keyvalue_list(env_labels, False)
        labels = {**env_labels, **extra_labels}
        validate_kube_labels(labels)
        return labels


def validate_kube_labels(
    labels: Optional[Dict[str, Optional[str]]],
) -> bool:
    """Validate label values.

    This validates the kubernetes label values.  It does not validate the keys.
    Ideally, keys should be static and also the validation rules for keys are
    more complex than those for values.  For full validation rules, see:

    https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
    """

    def validate_label(s: Optional[str]):
        regex_match = r"^(([A-Za-z0-9][-A-Za-z0-9_.]{0,61})?[A-Za-z0-9])?$"
        if not s:
            # allow empty label
            return True
        if not re.search(regex_match, s):
            raise KubernetesException(
                'Invalid value: "%s"\n'
                "A valid label must be an empty string or one that\n"
                "  - Consist of alphanumeric, '-', '_' or '.' characters\n"
                "  - Begins and ends with an alphanumeric character\n"
                "  - Is at most 63 characters" % s
            )
        return True

    return all([validate_label(v) for v in labels.values()]) if labels else True


def parse_kube_keyvalue_list(items: List[str], requires_both: bool = True):
    try:
        ret = {}
        for item_str in items:
            item = item_str.split("=", 1)
            if requires_both:
                item[1]  # raise IndexError
            if str(item[0]) in ret:
                raise KubernetesException("Duplicate key found: %s" % str(item[0]))
            ret[str(item[0])] = str(item[1]) if len(item) > 1 else None
        return ret
    except KubernetesException as e:
        raise e
    except (AttributeError, IndexError):
        raise KubernetesException("Unable to parse kubernetes list: %s" % items)
