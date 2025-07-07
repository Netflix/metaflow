import json
import math
import random
import time
from collections import namedtuple
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import KUBERNETES_JOBSET_GROUP, KUBERNETES_JOBSET_VERSION
from metaflow.tracing import inject_tracing_vars

from .kube_utils import qos_requests_and_limits


class KubernetesJobsetException(MetaflowException):
    headline = "Kubernetes jobset error"


# TODO [DUPLICATE CODE]: Refactor this method to a separate file so that
# It can be used by both KubernetesJob and KubernetesJobset
def k8s_retry(deadline_seconds=60, max_backoff=32):
    def decorator(function):
        from functools import wraps

        @wraps(function)
        def wrapper(*args, **kwargs):
            from kubernetes import client

            deadline = time.time() + deadline_seconds
            retry_number = 0

            while True:
                try:
                    result = function(*args, **kwargs)
                    return result
                except client.rest.ApiException as e:
                    if e.status == 500:
                        current_t = time.time()
                        backoff_delay = min(
                            math.pow(2, retry_number) + random.random(), max_backoff
                        )
                        if current_t + backoff_delay < deadline:
                            time.sleep(backoff_delay)
                            retry_number += 1
                            continue  # retry again
                        else:
                            raise
                    else:
                        raise

        return wrapper

    return decorator


JobsetStatus = namedtuple(
    "JobsetStatus",
    [
        "control_pod_failed",  # boolean
        "control_exit_code",
        "control_pod_status",  # string like (<pod-status>):(<container-status>) [used for user-messaging]
        "control_started",
        "control_completed",
        "worker_pods_failed",
        "workers_are_suspended",
        "workers_have_started",
        "all_jobs_are_suspended",
        "jobset_finished",
        "jobset_failed",
        "status_unknown",
        "jobset_was_terminated",
        "some_jobs_are_running",
    ],
)


def _basic_validation_for_js(jobset):
    if not jobset.get("status") or not _retrieve_replicated_job_statuses(jobset):
        return False
    worker_jobs = [
        w for w in jobset.get("spec").get("replicatedJobs") if w["name"] == "worker"
    ]
    if len(worker_jobs) == 0:
        raise KubernetesJobsetException("No worker jobs found in the jobset manifest")
    control_job = [
        w for w in jobset.get("spec").get("replicatedJobs") if w["name"] == "control"
    ]
    if len(control_job) == 0:
        raise KubernetesJobsetException("No control job found in the jobset manifest")
    return True


def _derive_pod_status_and_status_code(control_pod):
    overall_status = None
    control_exit_code = None
    control_pod_failed = False
    if control_pod:
        container_status = None
        pod_status = control_pod.get("status", {}).get("phase")
        container_statuses = control_pod.get("status", {}).get("containerStatuses")
        if container_statuses is None:
            container_status = ": ".join(
                filter(
                    None,
                    [
                        control_pod.get("status", {}).get("reason"),
                        control_pod.get("status", {}).get("message"),
                    ],
                )
            )
        else:
            for k, v in container_statuses[0].get("state", {}).items():
                if v is not None:
                    control_exit_code = v.get("exit_code")
                    container_status = ": ".join(
                        filter(
                            None,
                            [v.get("reason"), v.get("message")],
                        )
                    )
        if container_status is None:
            overall_status = "pod status: %s | container status: %s" % (
                pod_status,
                container_status,
            )
        else:
            overall_status = "pod status: %s" % pod_status
        if pod_status == "Failed":
            control_pod_failed = True
    return overall_status, control_exit_code, control_pod_failed


def _retrieve_replicated_job_statuses(jobset):
    # We needed this abstraction because Jobsets changed thier schema
    # in version v0.3.0 where `ReplicatedJobsStatus` became `replicatedJobsStatus`
    # So to handle users having an older version of jobsets, we need to account
    # for both the schemas.
    if jobset.get("status", {}).get("replicatedJobsStatus", None):
        return jobset.get("status").get("replicatedJobsStatus")
    elif jobset.get("status", {}).get("ReplicatedJobsStatus", None):
        return jobset.get("status").get("ReplicatedJobsStatus")
    return None


def _construct_jobset_logical_status(jobset, control_pod=None):
    if not _basic_validation_for_js(jobset):
        return JobsetStatus(
            control_started=False,
            control_completed=False,
            workers_are_suspended=False,
            workers_have_started=False,
            all_jobs_are_suspended=False,
            jobset_finished=False,
            jobset_failed=False,
            status_unknown=True,
            jobset_was_terminated=False,
            control_exit_code=None,
            control_pod_status=None,
            worker_pods_failed=False,
            control_pod_failed=False,
            some_jobs_are_running=False,
        )

    js_status = jobset.get("status")

    control_started = False
    control_completed = False
    workers_are_suspended = False
    workers_have_started = False
    all_jobs_are_suspended = jobset.get("spec", {}).get("suspend", False)
    jobset_finished = False
    jobset_failed = False
    status_unknown = False
    jobset_was_terminated = False
    worker_pods_failed = False
    some_jobs_are_running = False

    total_worker_jobs = [
        w["replicas"]
        for w in jobset.get("spec").get("replicatedJobs", [])
        if w["name"] == "worker"
    ][0]
    total_control_jobs = [
        w["replicas"]
        for w in jobset.get("spec").get("replicatedJobs", [])
        if w["name"] == "control"
    ][0]

    if total_worker_jobs == 0 and total_control_jobs == 0:
        jobset_was_terminated = True

    replicated_job_statuses = _retrieve_replicated_job_statuses(jobset)
    for job_status in replicated_job_statuses:
        if job_status["active"] > 0:
            some_jobs_are_running = True

        if job_status["name"] == "control":
            control_started = job_status["active"] > 0 or job_status["succeeded"] > 0
            control_completed = job_status["succeeded"] > 0
            if job_status["failed"] > 0:
                jobset_failed = True

        if job_status["name"] == "worker":
            workers_have_started = job_status["active"] == total_worker_jobs
            if "suspended" in job_status:
                # `replicatedJobStatus` didn't have `suspend` field
                #  until v0.3.0. So we need to account for that.
                workers_are_suspended = job_status["suspended"] > 0
            if job_status["failed"] > 0:
                worker_pods_failed = True
                jobset_failed = True

    if js_status.get("conditions"):
        for condition in js_status["conditions"]:
            if condition["type"] == "Completed":
                jobset_finished = True
            if condition["type"] == "Failed":
                jobset_failed = True

    (
        overall_status,
        control_exit_code,
        control_pod_failed,
    ) = _derive_pod_status_and_status_code(control_pod)

    return JobsetStatus(
        control_started=control_started,
        control_completed=control_completed,
        workers_are_suspended=workers_are_suspended,
        workers_have_started=workers_have_started,
        all_jobs_are_suspended=all_jobs_are_suspended,
        jobset_finished=jobset_finished,
        jobset_failed=jobset_failed,
        status_unknown=status_unknown,
        jobset_was_terminated=jobset_was_terminated,
        control_exit_code=control_exit_code,
        control_pod_status=overall_status,
        worker_pods_failed=worker_pods_failed,
        control_pod_failed=control_pod_failed,
        some_jobs_are_running=some_jobs_are_running,
    )


class RunningJobSet(object):
    def __init__(self, client, name, namespace, group, version):
        self._client = client
        self._name = name
        self._pod_name = None
        self._namespace = namespace
        self._group = group
        self._version = version
        self._pod = self._fetch_pod()
        self._jobset = self._fetch_jobset()

        import atexit

        def best_effort_kill():
            try:
                self.kill()
            except Exception:
                pass

        atexit.register(best_effort_kill)

    def __repr__(self):
        return "{}('{}/{}')".format(
            self.__class__.__name__, self._namespace, self._name
        )

    @k8s_retry()
    def _fetch_jobset(
        self,
    ):
        # name : name of jobset.
        # namespace : namespace of the jobset
        # Query the jobset and return the object's status field as a JSON object
        client = self._client.get()
        with client.ApiClient() as api_client:
            api_instance = client.CustomObjectsApi(api_client)
            try:
                jobset = api_instance.get_namespaced_custom_object(
                    group=self._group,
                    version=self._version,
                    namespace=self._namespace,
                    plural="jobsets",
                    name=self._name,
                )
                return jobset
            except client.rest.ApiException as e:
                if e.status == 404:
                    raise KubernetesJobsetException(
                        "Unable to locate Kubernetes jobset %s" % self._name
                    )
                raise

    @k8s_retry()
    def _fetch_pod(self):
        # Fetch pod metadata.
        client = self._client.get()
        pods = (
            client.CoreV1Api()
            .list_namespaced_pod(
                namespace=self._namespace,
                label_selector="jobset.sigs.k8s.io/jobset-name={}".format(self._name),
            )
            .to_dict()["items"]
        )
        if pods:
            for pod in pods:
                # check the labels of the pod to see if
                # the `jobset.sigs.k8s.io/replicatedjob-name` is set to `control`
                if (
                    pod["metadata"]["labels"].get(
                        "jobset.sigs.k8s.io/replicatedjob-name"
                    )
                    == "control"
                ):
                    return pod
        return {}

    def kill(self):
        plural = "jobsets"
        client = self._client.get()
        if not (self.is_running or self.is_waiting):
            return
        try:
            # Killing the control pod will trigger the jobset to mark everything as failed.
            # Since jobsets have a successPolicy set to `All` which ensures that everything has
            # to succeed for the jobset to succeed.
            from kubernetes.stream import stream

            control_pod = self._fetch_pod()
            stream(
                client.CoreV1Api().connect_get_namespaced_pod_exec,
                name=control_pod["metadata"]["name"],
                namespace=control_pod["metadata"]["namespace"],
                command=[
                    "/bin/sh",
                    "-c",
                    "/sbin/killall5",
                ],
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        except Exception:
            with client.ApiClient() as api_client:
                # If we are unable to kill the control pod then
                # Delete the jobset to kill the subsequent pods.
                # There are a few reasons for deleting a jobset to kill it :
                # 1. Jobset has a `suspend` attribute to suspend it's execution, but this
                # doesn't play nicely when jobsets are deployed with other components like kueue.
                # 2. Jobset doesn't play nicely when we mutate status
                # 3. Deletion is a gaurenteed way of removing any pods.
                api_instance = client.CustomObjectsApi(api_client)
                try:
                    api_instance.delete_namespaced_custom_object(
                        group=self._group,
                        version=self._version,
                        namespace=self._namespace,
                        plural=plural,
                        name=self._name,
                    )
                except Exception as e:
                    raise KubernetesJobsetException(
                        "Exception when deleting existing jobset: %s\n" % e
                    )

    @property
    def id(self):
        if self._pod_name:
            return "pod %s" % self._pod_name
        if self._pod:
            self._pod_name = self._pod["metadata"]["name"]
            return self.id
        return "jobset %s" % self._name

    @property
    def is_done(self):
        def done():
            return (
                self._jobset_is_completed
                or self._jobset_has_failed
                or self._jobset_was_terminated
            )

        if not done():
            # If not done, fetch newer status
            self._jobset = self._fetch_jobset()
            self._pod = self._fetch_pod()
        return done()

    @property
    def status(self):
        if self.is_done:
            return "Jobset is done"

        status = _construct_jobset_logical_status(self._jobset, control_pod=self._pod)
        if status.status_unknown:
            return "Jobset status is unknown"
        if status.control_started:
            if status.control_pod_status:
                return "Jobset is running: %s" % status.control_pod_status
            return "Jobset is running"
        if status.all_jobs_are_suspended:
            return "Jobset is waiting to be unsuspended"

        return "Jobset waiting for jobs to start"

    @property
    def has_succeeded(self):
        return self.is_done and self._jobset_is_completed

    @property
    def has_failed(self):
        return self.is_done and self._jobset_has_failed

    @property
    def is_running(self):
        if self.is_done:
            return False
        status = _construct_jobset_logical_status(self._jobset, control_pod=self._pod)
        if status.some_jobs_are_running:
            return True
        return False

    @property
    def _jobset_was_terminated(self):
        return _construct_jobset_logical_status(
            self._jobset, control_pod=self._pod
        ).jobset_was_terminated

    @property
    def is_waiting(self):
        return not self.is_done and not self.is_running

    @property
    def reason(self):
        # return exit code and reason
        if self.is_done and not self.has_succeeded:
            self._pod = self._fetch_pod()
        elif self.has_succeeded:
            return 0, None
        status = _construct_jobset_logical_status(self._jobset, control_pod=self._pod)
        if status.control_pod_failed:
            return (
                status.control_exit_code,
                "control-pod failed [%s]" % status.control_pod_status,
            )
        elif status.worker_pods_failed:
            return None, "Worker pods failed"
        return None, None

    @property
    def _jobset_is_completed(self):
        return _construct_jobset_logical_status(
            self._jobset, control_pod=self._pod
        ).jobset_finished

    @property
    def _jobset_has_failed(self):
        return _construct_jobset_logical_status(
            self._jobset, control_pod=self._pod
        ).jobset_failed


def _make_domain_name(
    jobset_name, main_job_name, main_job_index, main_pod_index, namespace
):
    return "%s-%s-%s-%s.%s.%s.svc.cluster.local" % (
        jobset_name,
        main_job_name,
        main_job_index,
        main_pod_index,
        jobset_name,
        namespace,
    )


class JobSetSpec(object):
    def __init__(self, kubernetes_sdk, name, **kwargs):
        self._kubernetes_sdk = kubernetes_sdk
        self._kwargs = kwargs
        self.name = name

    def replicas(self, replicas):
        self._kwargs["replicas"] = replicas
        return self

    def step_name(self, step_name):
        self._kwargs["step_name"] = step_name
        return self

    def namespace(self, namespace):
        self._kwargs["namespace"] = namespace
        return self

    def command(self, command):
        self._kwargs["command"] = command
        return self

    def image(self, image):
        self._kwargs["image"] = image
        return self

    def cpu(self, cpu):
        self._kwargs["cpu"] = cpu
        return self

    def memory(self, mem):
        self._kwargs["memory"] = mem
        return self

    def environment_variable(self, name, value):
        # Never set to None
        if value is None:
            return self
        self._kwargs["environment_variables"] = dict(
            self._kwargs.get("environment_variables", {}), **{name: value}
        )
        return self

    def secret(self, name):
        if name is None:
            return self
        if len(self._kwargs.get("secrets", [])) == 0:
            self._kwargs["secrets"] = []
        self._kwargs["secrets"] = list(set(self._kwargs["secrets"] + [name]))

    def environment_variable_from_selector(self, name, label_value):
        # Never set to None
        if label_value is None:
            return self
        self._kwargs["environment_variables_from_selectors"] = dict(
            self._kwargs.get("environment_variables_from_selectors", {}),
            **{name: label_value}
        )
        return self

    def label(self, name, value):
        self._kwargs["labels"] = dict(self._kwargs.get("labels", {}), **{name: value})
        return self

    def annotation(self, name, value):
        self._kwargs["annotations"] = dict(
            self._kwargs.get("annotations", {}), **{name: value}
        )
        return self

    def dump(self):
        client = self._kubernetes_sdk
        use_tmpfs = self._kwargs["use_tmpfs"]
        tmpfs_size = self._kwargs["tmpfs_size"]
        tmpfs_enabled = use_tmpfs or (tmpfs_size and not use_tmpfs)
        shared_memory = (
            int(self._kwargs["shared_memory"])
            if self._kwargs["shared_memory"]
            else None
        )
        qos_requests, qos_limits = qos_requests_and_limits(
            self._kwargs["qos"],
            self._kwargs["cpu"],
            self._kwargs["memory"],
            self._kwargs["disk"],
        )
        security_context = self._kwargs.get("security_context", {})
        _security_context = {}
        if security_context is not None and len(security_context) > 0:
            _security_context = {
                "security_context": client.V1SecurityContext(**security_context)
            }
        return dict(
            name=self.name,
            template=client.api_client.ApiClient().sanitize_for_serialization(
                client.V1JobTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        namespace=self._kwargs["namespace"],
                        # We don't set any annotations here
                        # since they have been either set in the JobSpec
                        # or on the JobSet level
                    ),
                    spec=client.V1JobSpec(
                        # Retries are handled by Metaflow when it is responsible for
                        # executing the flow. The responsibility is moved to Kubernetes
                        # when Argo Workflows is responsible for the execution.
                        backoff_limit=self._kwargs.get("retries", 0),
                        completions=1,
                        parallelism=1,
                        ttl_seconds_after_finished=7
                        * 60
                        * 60  # Remove job after a week. TODO: Make this configurable
                        * 24,
                        template=client.V1PodTemplateSpec(
                            metadata=client.V1ObjectMeta(
                                annotations=self._kwargs.get("annotations", {}),
                                labels=self._kwargs.get("labels", {}),
                                namespace=self._kwargs["namespace"],
                            ),
                            spec=client.V1PodSpec(
                                subdomain=self._kwargs["subdomain"],
                                set_hostname_as_fqdn=True,
                                # Timeout is set on the pod and not the job (important!)
                                active_deadline_seconds=self._kwargs[
                                    "timeout_in_seconds"
                                ],
                                # TODO (savin): Enable affinities for GPU scheduling.
                                # affinity=?,
                                containers=[
                                    client.V1Container(
                                        command=self._kwargs["command"],
                                        termination_message_policy="FallbackToLogsOnError",
                                        ports=(
                                            []
                                            if self._kwargs["port"] is None
                                            else [
                                                client.V1ContainerPort(
                                                    container_port=int(
                                                        self._kwargs["port"]
                                                    )
                                                )
                                            ]
                                        ),
                                        env=[
                                            client.V1EnvVar(name=k, value=str(v))
                                            for k, v in self._kwargs.get(
                                                "environment_variables", {}
                                            ).items()
                                        ]
                                        # And some downward API magic. Add (key, value)
                                        # pairs below to make pod metadata available
                                        # within Kubernetes container.
                                        + [
                                            client.V1EnvVar(
                                                name=k,
                                                value_from=client.V1EnvVarSource(
                                                    field_ref=client.V1ObjectFieldSelector(
                                                        field_path=str(v)
                                                    )
                                                ),
                                            )
                                            for k, v in self._kwargs.get(
                                                "environment_variables_from_selectors",
                                                {},
                                            ).items()
                                        ]
                                        + [
                                            client.V1EnvVar(name=k, value=str(v))
                                            for k, v in inject_tracing_vars({}).items()
                                        ],
                                        env_from=[
                                            client.V1EnvFromSource(
                                                secret_ref=client.V1SecretEnvSource(
                                                    name=str(k),
                                                    # optional=True
                                                )
                                            )
                                            for k in list(
                                                self._kwargs.get("secrets", [])
                                            )
                                            if k
                                        ],
                                        image=self._kwargs["image"],
                                        image_pull_policy=self._kwargs[
                                            "image_pull_policy"
                                        ],
                                        name=self._kwargs["step_name"].replace(
                                            "_", "-"
                                        ),
                                        resources=client.V1ResourceRequirements(
                                            requests=qos_requests,
                                            limits={
                                                **qos_limits,
                                                **{
                                                    "%s.com/gpu".lower()
                                                    % self._kwargs["gpu_vendor"]: str(
                                                        self._kwargs["gpu"]
                                                    )
                                                    for k in [0]
                                                    # Don't set GPU limits if gpu isn't specified.
                                                    if self._kwargs["gpu"] is not None
                                                },
                                            },
                                        ),
                                        volume_mounts=(
                                            [
                                                client.V1VolumeMount(
                                                    mount_path=self._kwargs.get(
                                                        "tmpfs_path"
                                                    ),
                                                    name="tmpfs-ephemeral-volume",
                                                )
                                            ]
                                            if tmpfs_enabled
                                            else []
                                        )
                                        + (
                                            [
                                                client.V1VolumeMount(
                                                    mount_path="/dev/shm", name="dhsm"
                                                )
                                            ]
                                            if shared_memory
                                            else []
                                        )
                                        + (
                                            [
                                                client.V1VolumeMount(
                                                    mount_path=path, name=claim
                                                )
                                                for claim, path in self._kwargs[
                                                    "persistent_volume_claims"
                                                ].items()
                                            ]
                                            if self._kwargs["persistent_volume_claims"]
                                            is not None
                                            else []
                                        ),
                                        **_security_context,
                                    )
                                ],
                                node_selector=self._kwargs.get("node_selector"),
                                image_pull_secrets=[
                                    client.V1LocalObjectReference(secret)
                                    for secret in self._kwargs.get("image_pull_secrets")
                                    or []
                                ],
                                # TODO (savin): Support preemption policies
                                # preemption_policy=?,
                                #
                                # A Container in a Pod may fail for a number of
                                # reasons, such as because the process in it exited
                                # with a non-zero exit code, or the Container was
                                # killed due to OOM etc. If this happens, fail the pod
                                # and let Metaflow handle the retries.
                                restart_policy="Never",
                                service_account_name=self._kwargs["service_account"],
                                # Terminate the container immediately on SIGTERM
                                termination_grace_period_seconds=0,
                                tolerations=[
                                    client.V1Toleration(**toleration)
                                    for toleration in self._kwargs.get("tolerations")
                                    or []
                                ],
                                volumes=(
                                    [
                                        client.V1Volume(
                                            name="tmpfs-ephemeral-volume",
                                            empty_dir=client.V1EmptyDirVolumeSource(
                                                medium="Memory",
                                                # Add default unit as ours differs from Kubernetes default.
                                                size_limit="{}Mi".format(tmpfs_size),
                                            ),
                                        )
                                    ]
                                    if tmpfs_enabled
                                    else []
                                )
                                + (
                                    [
                                        client.V1Volume(
                                            name="dhsm",
                                            empty_dir=client.V1EmptyDirVolumeSource(
                                                medium="Memory",
                                                size_limit="{}Mi".format(shared_memory),
                                            ),
                                        )
                                    ]
                                    if shared_memory
                                    else []
                                )
                                + (
                                    [
                                        client.V1Volume(
                                            name=claim,
                                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                                claim_name=claim
                                            ),
                                        )
                                        for claim in self._kwargs[
                                            "persistent_volume_claims"
                                        ].keys()
                                    ]
                                    if self._kwargs["persistent_volume_claims"]
                                    is not None
                                    else []
                                ),
                            ),
                        ),
                    ),
                )
            ),
            replicas=self._kwargs["replicas"],
        )


class KubernetesJobSet(object):
    def __init__(
        self,
        client,
        name=None,
        namespace=None,
        num_parallel=None,
        # explcitly declaring num_parallel because we need to ensure that
        # num_parallel is an INTEGER and this abstraction is called by the
        # local runtime abstraction of kubernetes.
        # Argo will call another abstraction that will allow setting a lot of these
        # values from the top level argo code.
        **kwargs
    ):
        self._client = client
        self._annotations = {}
        self._labels = {}
        self._group = KUBERNETES_JOBSET_GROUP
        self._version = KUBERNETES_JOBSET_VERSION
        self._namespace = namespace
        self.name = name

        self._jobset_control_addr = _make_domain_name(
            name,
            "control",
            0,
            0,
            namespace,
        )

        self._control_spec = JobSetSpec(
            client.get(), name="control", namespace=namespace, **kwargs
        )
        self._worker_spec = JobSetSpec(
            client.get(), name="worker", namespace=namespace, **kwargs
        )
        assert (
            type(num_parallel) == int
        ), "num_parallel must be an integer"  # todo: [final-refactor] : fix-me

    @property
    def jobset_control_addr(self):
        return self._jobset_control_addr

    @property
    def worker(self):
        return self._worker_spec

    @property
    def control(self):
        return self._control_spec

    def environment_variable_from_selector(self, name, label_value):
        self.worker.environment_variable_from_selector(name, label_value)
        self.control.environment_variable_from_selector(name, label_value)
        return self

    def environment_variables_from_selectors(self, env_dict):
        for name, label_value in env_dict.items():
            self.worker.environment_variable_from_selector(name, label_value)
            self.control.environment_variable_from_selector(name, label_value)
        return self

    def environment_variable(self, name, value):
        self.worker.environment_variable(name, value)
        self.control.environment_variable(name, value)
        return self

    def label(self, name, value):
        self.worker.label(name, value)
        self.control.label(name, value)
        self._labels = dict(self._labels, **{name: value})
        return self

    def annotation(self, name, value):
        self.worker.annotation(name, value)
        self.control.annotation(name, value)
        self._annotations = dict(self._annotations, **{name: value})
        return self

    def labels(self, labels):
        for k, v in labels.items():
            self.label(k, v)
        return self

    def annotations(self, annotations):
        for k, v in annotations.items():
            self.annotation(k, v)
        return self

    def secret(self, name):
        self.worker.secret(name)
        self.control.secret(name)
        return self

    def dump(self):
        client = self._client.get()
        return dict(
            apiVersion=self._group + "/" + self._version,
            kind="JobSet",
            metadata=client.api_client.ApiClient().sanitize_for_serialization(
                client.V1ObjectMeta(
                    name=self.name,
                    labels=self._labels,
                    annotations=self._annotations,
                )
            ),
            spec=dict(
                replicatedJobs=[self.control.dump(), self.worker.dump()],
                suspend=False,
                startupPolicy=dict(
                    # We explicitly set an InOrder Startup policy so that
                    # we can ensure that the control pod starts before the worker pods.
                    # This is required so that when worker pods try to access the control's IP
                    # we are able to resolve the control's IP address.
                    startupPolicyOrder="InOrder"
                ),
                successPolicy=None,
                # The Failure Policy helps setting the number of retries for the jobset.
                # but we don't rely on it and instead rely on either the local scheduler
                # or the Argo Workflows to handle retries.
                failurePolicy=None,
                network=None,
            ),
            status=None,
        )

    def execute(self):
        client = self._client.get()
        api_instance = client.CoreV1Api()

        with client.ApiClient() as api_client:
            api_instance = client.CustomObjectsApi(api_client)
            try:
                jobset_obj = api_instance.create_namespaced_custom_object(
                    group=self._group,
                    version=self._version,
                    namespace=self._namespace,
                    plural="jobsets",
                    body=self.dump(),
                )
            except Exception as e:
                raise KubernetesJobsetException(
                    "Exception when calling CustomObjectsApi->create_namespaced_custom_object: %s\n"
                    % e
                )

        return RunningJobSet(
            client=self._client,
            name=jobset_obj["metadata"]["name"],
            namespace=jobset_obj["metadata"]["namespace"],
            group=self._group,
            version=self._version,
        )


class KubernetesArgoJobSet(object):
    def __init__(self, kubernetes_sdk, name=None, namespace=None, **kwargs):
        self._kubernetes_sdk = kubernetes_sdk
        self._annotations = {}
        self._labels = {}
        self._group = KUBERNETES_JOBSET_GROUP
        self._version = KUBERNETES_JOBSET_VERSION
        self._namespace = namespace
        self.name = name

        self._jobset_control_addr = _make_domain_name(
            name,
            "control",
            0,
            0,
            namespace,
        )

        self._control_spec = JobSetSpec(
            kubernetes_sdk, name="control", namespace=namespace, **kwargs
        )
        self._worker_spec = JobSetSpec(
            kubernetes_sdk, name="worker", namespace=namespace, **kwargs
        )

    @property
    def jobset_control_addr(self):
        return self._jobset_control_addr

    @property
    def worker(self):
        return self._worker_spec

    @property
    def control(self):
        return self._control_spec

    def environment_variable_from_selector(self, name, label_value):
        self.worker.environment_variable_from_selector(name, label_value)
        self.control.environment_variable_from_selector(name, label_value)
        return self

    def environment_variables_from_selectors(self, env_dict):
        for name, label_value in env_dict.items():
            self.worker.environment_variable_from_selector(name, label_value)
            self.control.environment_variable_from_selector(name, label_value)
        return self

    def environment_variable(self, name, value):
        self.worker.environment_variable(name, value)
        self.control.environment_variable(name, value)
        return self

    def label(self, name, value):
        self.worker.label(name, value)
        self.control.label(name, value)
        self._labels = dict(self._labels, **{name: value})
        return self

    def labels(self, labels):
        for k, v in labels.items():
            self.label(k, v)
        return self

    def annotation(self, name, value):
        self.worker.annotation(name, value)
        self.control.annotation(name, value)
        self._annotations = dict(self._annotations, **{name: value})
        return self

    def annotations(self, annotations):
        for k, v in annotations.items():
            self.annotation(k, v)
        return self

    def dump(self):
        client = self._kubernetes_sdk

        data = json.dumps(
            client.ApiClient().sanitize_for_serialization(
                dict(
                    apiVersion=self._group + "/" + self._version,
                    kind="JobSet",
                    metadata=client.api_client.ApiClient().sanitize_for_serialization(
                        client.V1ObjectMeta(
                            name=self.name,
                            labels=self._labels,
                            annotations=self._annotations,
                        )
                    ),
                    spec=dict(
                        replicatedJobs=[self.control.dump(), self.worker.dump()],
                        suspend=False,
                        startupPolicy=None,
                        successPolicy=None,
                        # The Failure Policy helps setting the number of retries for the jobset.
                        # but we don't rely on it and instead rely on either the local scheduler
                        # or the Argo Workflows to handle retries.
                        failurePolicy=None,
                        network=None,
                    ),
                    status=None,
                )
            )
        )
        # The values we populate in the Jobset manifest (for Argo Workflows) piggybacks on the Argo Workflow's templating engine.
        # Even though Argo Workflows's templating helps us constructing all the necessary IDs and populating the fields
        # required by Metaflow, we run into one glitch. When we construct JSON/YAML serializable objects,
        # anything between two braces such as `{{=asInt(inputs.parameters.workerCount)}}` gets quoted. This is a problem
        # since we need to pass the value of `inputs.parameters.workerCount` as an integer and not as a string.
        # If we pass it as a string, the jobset controller will not accept the Jobset CRD we submitted to kubernetes.
        # To get around this, we need to replace the quoted substring with the unquoted substring because YAML /JSON parsers
        # won't allow deserialization with the quoting trivially.

        # This is super important because the `inputs.parameters.workerCount` is used to set the number of replicas;
        # The value for number of replicas is derived from the value of `num_parallel` (which is set in the user-code).
        # Since the value of `num_parallel` can be dynamic and can change from run to run, we need to ensure that the
        # value can be passed-down dynamically and is **explicitly set as a integer** in the Jobset Manifest submitted as a
        # part of the Argo Workflow

        quoted_substring = '"{{=asInt(inputs.parameters.workerCount)}}"'
        unquoted_substring = "{{=asInt(inputs.parameters.workerCount)}}"
        return data.replace(quoted_substring, unquoted_substring)
