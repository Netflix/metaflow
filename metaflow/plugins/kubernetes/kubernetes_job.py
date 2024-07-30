import copy
import json
import math
import random
import sys
import time

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import KUBERNETES_SECRETS
from metaflow.tracing import inject_tracing_vars
from metaflow.unbounded_foreach import UBF_CONTROL, UBF_TASK

CLIENT_REFRESH_INTERVAL_SECONDS = 300
from .kubernetes_jobsets import (
    KubernetesJobSet,
)  # We need this import for Kubernetes Client.


class KubernetesJobException(MetaflowException):
    headline = "Kubernetes job error"


# Implements truncated exponential backoff from
# https://cloud.google.com/storage/docs/retry-strategy#exponential-backoff
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


class KubernetesJob(object):
    def __init__(self, client, **kwargs):
        self._client = client
        self._kwargs = kwargs

    def create_job_spec(self):
        client = self._client.get()

        # tmpfs variables
        use_tmpfs = self._kwargs["use_tmpfs"]
        tmpfs_size = self._kwargs["tmpfs_size"]
        tmpfs_enabled = use_tmpfs or (tmpfs_size and not use_tmpfs)
        shared_memory = (
            int(self._kwargs["shared_memory"])
            if self._kwargs["shared_memory"]
            else None
        )
        return client.V1JobSpec(
            # Retries are handled by Metaflow when it is responsible for
            # executing the flow. The responsibility is moved to Kubernetes
            # when Argo Workflows is responsible for the execution.
            backoff_limit=self._kwargs.get("retries", 0),
            completions=self._kwargs.get("completions", 1),
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
                    # Timeout is set on the pod and not the job (important!)
                    active_deadline_seconds=self._kwargs["timeout_in_seconds"],
                    # TODO (savin): Enable affinities for GPU scheduling.
                    # affinity=?,
                    containers=[
                        client.V1Container(
                            command=self._kwargs["command"],
                            termination_message_policy="FallbackToLogsOnError",
                            ports=[]
                            if self._kwargs["port"] is None
                            else [
                                client.V1ContainerPort(
                                    container_port=int(self._kwargs["port"])
                                )
                            ],
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
                                for k, v in {
                                    "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                                    "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                                    "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
                                    "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME": "spec.serviceAccountName",
                                    "METAFLOW_KUBERNETES_NODE_IP": "status.hostIP",
                                }.items()
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
                                for k in list(self._kwargs.get("secrets", []))
                                + KUBERNETES_SECRETS.split(",")
                                if k
                            ],
                            image=self._kwargs["image"],
                            image_pull_policy=self._kwargs["image_pull_policy"],
                            name=self._kwargs["step_name"].replace("_", "-"),
                            resources=client.V1ResourceRequirements(
                                requests={
                                    "cpu": str(self._kwargs["cpu"]),
                                    "memory": "%sM" % str(self._kwargs["memory"]),
                                    "ephemeral-storage": "%sM"
                                    % str(self._kwargs["disk"]),
                                },
                                limits={
                                    "%s.com/gpu".lower()
                                    % self._kwargs["gpu_vendor"]: str(
                                        self._kwargs["gpu"]
                                    )
                                    for k in [0]
                                    # Don't set GPU limits if gpu isn't specified.
                                    if self._kwargs["gpu"] is not None
                                },
                            ),
                            volume_mounts=(
                                [
                                    client.V1VolumeMount(
                                        mount_path=self._kwargs.get("tmpfs_path"),
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
                                    client.V1VolumeMount(mount_path=path, name=claim)
                                    for claim, path in self._kwargs[
                                        "persistent_volume_claims"
                                    ].items()
                                ]
                                if self._kwargs["persistent_volume_claims"] is not None
                                else []
                            ),
                        )
                    ],
                    node_selector=self._kwargs.get("node_selector"),
                    # TODO (savin): Support image_pull_secrets
                    # image_pull_secrets=?,
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
                        for toleration in self._kwargs.get("tolerations") or []
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
                            for claim in self._kwargs["persistent_volume_claims"].keys()
                        ]
                        if self._kwargs["persistent_volume_claims"] is not None
                        else []
                    ),
                    # TODO (savin): Set termination_message_policy
                ),
            ),
        )

    def create(self):
        # A discerning eye would notice and question the choice of using the
        # V1Job construct over the V1Pod construct given that we don't rely much
        # on any of the V1Job semantics. The major reasons at the moment are -
        #     1. It makes the Kubernetes UIs (Octant, Lens) a bit easier on
        #        the eyes, although even that can be questioned.
        #     2. AWS Step Functions, at the moment (Apr' 22) only supports
        #        executing Jobs and not Pods as part of it's publicly declared
        #        API. When we ship the AWS Step Functions integration with EKS,
        #        it will hopefully lessen our workload.
        #
        # Note: This implementation ensures that there is only one unique Pod
        # (unique UID) per Metaflow task attempt.
        client = self._client.get()

        self._job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                # Annotations are for humans
                annotations=self._kwargs.get("annotations", {}),
                # While labels are for Kubernetes
                labels=self._kwargs.get("labels", {}),
                generate_name=self._kwargs["generate_name"],
                namespace=self._kwargs["namespace"],  # Defaults to `default`
            ),
            spec=self.create_job_spec(),
        )
        return self

    def execute(self):
        client = self._client.get()
        try:
            # TODO: Make job submission back-pressure aware. Currently
            #       there doesn't seem to be a kubernetes-native way to
            #       achieve the guarantees that we are seeking.
            #       https://github.com/kubernetes/enhancements/issues/1040
            #       Hopefully, we will be able to get creative with kube-batch
            response = (
                client.BatchV1Api()
                .create_namespaced_job(
                    body=self._job, namespace=self._kwargs["namespace"]
                )
                .to_dict()
            )
            return RunningJob(
                client=self._client,
                name=response["metadata"]["name"],
                uid=response["metadata"]["uid"],
                namespace=response["metadata"]["namespace"],
            )
        except client.rest.ApiException as e:
            raise KubernetesJobException(
                "Unable to launch Kubernetes job.\n %s"
                % (json.loads(e.body)["message"] if e.body is not None else e.reason)
            )

    def step_name(self, step_name):
        self._kwargs["step_name"] = step_name
        return self

    def namespace(self, namespace):
        self._kwargs["namespace"] = namespace
        return self

    def name(self, name):
        self._kwargs["name"] = name
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

    def label(self, name, value):
        self._kwargs["labels"] = dict(self._kwargs.get("labels", {}), **{name: value})
        return self

    def annotation(self, name, value):
        self._kwargs["annotations"] = dict(
            self._kwargs.get("annotations", {}), **{name: value}
        )
        return self


class RunningJob(object):
    # State Machine implementation for the lifecycle behavior documented in
    # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
    #
    # This object encapsulates *both* V1Job and V1Pod. It simplifies the status
    # to "running" and "done" (failed/succeeded) state. Note that V1Job and V1Pod
    # status fields are not guaranteed to be always in sync due to the way job
    # controller works.

    # To ascertain the status of V1Job, we peer into the lifecycle status of
    # the pod it is responsible for executing. Unfortunately, the `phase`
    # attributes (pending, running, succeeded, failed etc.) only provide
    # partial answers and the official API conventions guide suggests that
    # it may soon be deprecated (however, not anytime soon - see
    # https://github.com/kubernetes/kubernetes/issues/7856). `conditions` otoh
    # provide a deeper understanding about the state of the pod; however
    # conditions are not state machines and can be oscillating - from the
    # official API conventions guide:
    #     In general, condition values may change back and forth, but some
    #     condition transitions may be monotonic, depending on the resource and
    #     condition type. However, conditions are observations and not,
    #     themselves, state machines, nor do we define comprehensive state
    #     machines for objects, nor behaviors associated with state
    #     transitions. The system is level-based rather than edge-triggered,
    #     and should assume an Open World.
    # As a follow-up, we can synthesize our notion of "phase" state
    # machine from `conditions`, since Kubernetes won't do it for us (for
    # many good reasons).
    #
    # `conditions` can be of the following types -
    #    1. (kubelet) Initialized (always True since we don't rely on init
    #       containers)
    #    2. (kubelet) ContainersReady
    #    3. (kubelet) Ready (same as ContainersReady since we don't use
    #       ReadinessGates -
    #       https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/status/generate.go)
    #    4. (kube-scheduler) PodScheduled
    #       (https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/scheduler.go)

    def __init__(self, client, name, uid, namespace):
        self._client = client
        self._name = name
        self._pod_name = None
        self._id = uid
        self._namespace = namespace

        self._job = self._fetch_job()
        self._pod = self._fetch_pod()

        import atexit

        def best_effort_kill():
            try:
                self.kill()
            except Exception as ex:
                pass

        atexit.register(best_effort_kill)

    def __repr__(self):
        return "{}('{}/{}')".format(
            self.__class__.__name__, self._namespace, self._name
        )

    @k8s_retry()
    def _fetch_job(self):
        client = self._client.get()
        try:
            return (
                client.BatchV1Api()
                .read_namespaced_job(name=self._name, namespace=self._namespace)
                .to_dict()
            )
        except client.rest.ApiException as e:
            if e.status == 404:
                raise KubernetesJobException(
                    "Unable to locate Kubernetes batch/v1 job %s" % self._name
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
                label_selector="job-name={}".format(self._name),
            )
            .to_dict()["items"]
        )
        if pods:
            return pods[0]
        return {}

    def kill(self):
        # Terminating a Kubernetes job is a bit tricky. Issuing a
        # `BatchV1Api.delete_namespaced_job` will also remove all traces of the
        # job object from the Kubernetes API server which may not be desirable.
        # This forces us to be a bit creative in terms of how we handle kill:
        #
        # 1. If the container is alive and kicking inside the pod, we simply
        #    attach ourselves to the container and issue a kill signal. The
        #    way we have initialized the Job ensures that the job will cleanly
        #    terminate.
        # 2. In scenarios where either the pod (unschedulable etc.) or the
        #    container (ImagePullError etc.) hasn't come up yet, we become a
        #    bit creative by patching the job parallelism to 0. This ensures
        #    that the underlying node's resources are made available to
        #    kube-scheduler again. The downside is that the Job wouldn't mark
        #    itself as done and the pod metadata disappears from the API
        #    server. There is an open issue in the Kubernetes GH to provide
        #    better support for job terminations -
        #    https://github.com/kubernetes/enhancements/issues/2232
        # 3. If the pod object hasn't shown up yet, we set the parallelism to 0
        #    to preempt it.
        client = self._client.get()

        if not self.is_done:
            if self.is_running:
                # Case 1.
                from kubernetes.stream import stream

                api_instance = client.CoreV1Api
                try:
                    # TODO: stream opens a web-socket connection. It may
                    #       not be desirable to open multiple web-socket
                    #       connections frivolously (think killing a
                    #       workflow during a for-each step).
                    stream(
                        api_instance().connect_get_namespaced_pod_exec,
                        name=self._pod["metadata"]["name"],
                        namespace=self._namespace,
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
                except:
                    # Best effort. It's likely that this API call could be
                    # blocked for the user.
                    # --------------------------------------------------------
                    # We try patching Job parallelism anyway. Stopping any runaway
                    # jobs (and their pods) is secondary to correctly showing
                    # "Killed" status on the Kubernetes pod.
                    #
                    # This has the effect of pausing the job.
                    try:
                        client.BatchV1Api().patch_namespaced_job(
                            name=self._name,
                            namespace=self._namespace,
                            field_manager="metaflow",
                            body={"spec": {"parallelism": 0}},
                        )
                    except:
                        # Best effort.
                        pass
                        # raise
            else:
                # Case 2.
                # This has the effect of pausing the job.
                try:
                    client.BatchV1Api().patch_namespaced_job(
                        name=self._name,
                        namespace=self._namespace,
                        field_manager="metaflow",
                        body={"spec": {"parallelism": 0}},
                    )
                except:
                    # Best effort.
                    pass
                    # raise
        return self

    @property
    def id(self):
        if self._pod_name:
            return "pod %s" % self._pod_name
        if self._pod:
            self._pod_name = self._pod["metadata"]["name"]
            return self.id
        return "job %s" % self._name

    @property
    def is_done(self):
        # Check if the container is done. As a side effect, also refreshes self._job and
        # self._pod with the latest state
        def done():
            # Either the container succeeds or fails naturally or else we may have
            # forced the pod termination causing the job to still be in an
            # active state but for all intents and purposes dead to us.
            return (
                bool(self._job["status"].get("succeeded"))
                or bool(self._job["status"].get("failed"))
                or self._are_pod_containers_done
                or (self._job["spec"]["parallelism"] == 0)
            )

        if not done():
            # If not done, fetch newer status
            self._job = self._fetch_job()
            self._pod = self._fetch_pod()
        return done()

    @property
    def status(self):
        if not self.is_done:
            if bool(self._job["status"].get("active")):
                if self._pod:
                    msg = (
                        "Pod is %s"
                        % self._pod.get("status", {})
                        .get("phase", "uninitialized")
                        .lower()
                    )
                    # TODO (savin): parse Pod conditions
                    container_status = (
                        self._pod["status"].get("container_statuses") or [None]
                    )[0]
                    if container_status:
                        # We have a single container inside the pod
                        status = {"status": "waiting"}
                        for k, v in container_status["state"].items():
                            if v is not None:
                                status["status"] = k
                                status.update(v)
                        msg += ", Container is %s" % status["status"].lower()
                        reason = ""
                        if status.get("reason"):
                            pass
                            reason = status["reason"]
                        if status.get("message"):
                            reason += " - %s" % status["message"]
                        if reason:
                            msg += " - %s" % reason
                    return msg
                return "Job is active"
            return "Job status is unknown"
        return "Job is done"

    @property
    def has_succeeded(self):
        # The tasks container is in a terminal state and the status is marked as succeeded
        return self.is_done and self._have_containers_succeeded

    @property
    def has_failed(self):
        # Either the container is marked as failed or the Job is not allowed to
        # any more pods
        retval = self.is_done and (
            bool(self._job["status"].get("failed"))
            or self._has_any_container_failed
            or (self._job["spec"]["parallelism"] == 0)
        )
        return retval

    @property
    def _have_containers_succeeded(self):
        container_statuses = self._pod.get("status", {}).get("container_statuses", [])
        if not container_statuses:
            return False

        for cstatus in container_statuses:
            # If the terminated field is not set, the pod is still running.
            terminated = cstatus.get("state", {}).get("terminated", {})
            if not terminated:
                return False

            # If the terminated field is set but the `finished_at` field is not set,
            # the pod is still considered as running.
            if not terminated.get("finished_at"):
                return False

            # If finished_at is set AND reason is Completed
            if terminated.get("reason", "").lower() != "completed":
                return False

        return True

    @property
    def _has_any_container_failed(self):
        container_statuses = self._pod.get("status", {}).get("container_statuses", [])
        if not container_statuses:
            return False

        for cstatus in container_statuses:
            # If the terminated field is not set, the pod is still running. Too early
            # to determine if any container failed.
            terminated = cstatus.get("state", {}).get("terminated", {})
            if not terminated:
                return False

            # If the terminated field is set but the `finished_at` field is not set,
            # the pod is still considered as running. Too early to determine if any
            # container failed.
            if not terminated.get("finished_at"):
                return False

            # If finished_at is set AND reason is Error, it means that the
            # container failed.
            if terminated.get("reason", "").lower() == "error":
                return True

        # If none of the containers are marked as failed, the pod is not
        # considered failed.
        return False

    @property
    def _are_pod_containers_done(self):
        # All containers in the pod have a containerStatus that has a
        # finishedAt set.
        container_statuses = self._pod.get("status", {}).get("container_statuses", [])
        if not container_statuses:
            return False

        for cstatus in container_statuses:
            # If the terminated field is not set, the pod is still running. Too early
            # to determine if any container failed.
            terminated = cstatus.get("state", {}).get("terminated", {})
            if not terminated:
                return False

            # If the terminated field is set but the `finished_at` field is not set,
            # the pod is still considered as running.
            if not terminated.get("finished_at"):
                return False

        # If we got until here, the containers were marked terminated and their
        # finishedAt was set.
        return True

    @property
    def is_running(self):
        # Returns true if the container is running.
        if self.is_done:
            return False

        return not self._are_pod_containers_done

    @property
    def is_waiting(self):
        return not self.is_done and not self.is_running

    @property
    def reason(self):
        if self.is_done:
            if self.has_succeeded:
                return 0, None
            # Best effort since Pod object can disappear on us at anytime
            else:
                if self._pod.get("status", {}).get("phase") not in (
                    "Succeeded",
                    "Failed",
                ):
                    # If pod status is dirty, check for newer status
                    self._pod = self._fetch_pod()
                if self._pod:
                    if self._pod.get("status", {}).get("container_statuses") is None:
                        # We're done, but no container_statuses is set
                        # This can happen when the pod is evicted
                        return None, ": ".join(
                            filter(
                                None,
                                [
                                    self._pod.get("status", {}).get("reason"),
                                    self._pod.get("status", {}).get("message"),
                                ],
                            )
                        )

                    for k, v in (
                        self._pod.get("status", {})
                        .get("container_statuses", [{}])[0]
                        .get("state", {})
                        .items()
                    ):
                        if v is not None:
                            return v.get("exit_code"), ": ".join(
                                filter(
                                    None,
                                    [v.get("reason"), v.get("message")],
                                )
                            )
        return None, None
