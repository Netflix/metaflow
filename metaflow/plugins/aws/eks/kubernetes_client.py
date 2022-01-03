import os
import time
import math
import random


try:
    unicode
except NameError:
    unicode = str
    basestring = str

from metaflow.exception import MetaflowException

CLIENT_REFRESH_INTERVAL_SECONDS = 300


class KubernetesJobException(MetaflowException):
    headline = "Kubernetes job error"


# Implements truncated exponential backoff from https://cloud.google.com/storage/docs/retry-strategy#exponential-backoff
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


class KubernetesClient(object):
    def __init__(self):
        # TODO: Look into removing the usage of Kubernetes Python SDK
        # at some point in the future. Given that Kubernetes Python SDK
        # aggressively drops support for older kubernetes clusters, continued
        # dependency on it may bite our users.

        try:
            # Kubernetes is a soft dependency.
            from kubernetes import client, config
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import module 'kubernetes'. Install kubernetes "
                "Python package (https://pypi.org/project/kubernetes/) first."
            )
        self._refresh_client()

    def _refresh_client(self):
        from kubernetes import client, config

        if os.getenv("KUBERNETES_SERVICE_HOST"):
            # We are inside a pod, authenticate via ServiceAccount assigned to us
            config.load_incluster_config()
        else:
            # Use kubeconfig, likely $HOME/.kube/config
            # TODO (savin):
            #     1. Support generating kubeconfig on the fly using boto3
            #     2. Support auth via OIDC - https://docs.aws.amazon.com/eks/latest/userguide/authenticate-oidc-identity-provider.html
            # Supporting the above auth mechanisms (atleast 1.) should be
            # good enough for the initial rollout.
            config.load_kube_config()
        self._client = client
        self._client_refresh_timestamp = time.time()

    def job(self, **kwargs):
        return KubernetesJob(self, **kwargs)

    def get(self):
        if (
            time.time() - self._client_refresh_timestamp
            > CLIENT_REFRESH_INTERVAL_SECONDS
        ):
            self._refresh_client()

        return self._client


class KubernetesJob(object):
    def __init__(self, client_wrapper, **kwargs):
        self._client_wrapper = client_wrapper
        self._kwargs = kwargs

    def create(self):
        # Check that job attributes are sensible.

        # CPU value should be greater than 0
        if not (
            isinstance(self._kwargs["cpu"], (int, unicode, basestring, float))
            and float(self._kwargs["cpu"]) > 0
        ):
            raise KubernetesJobException(
                "Invalid CPU value ({}); it should be greater than 0".format(
                    self._kwargs["cpu"]
                )
            )

        # Memory value should be greater than 0
        if not (
            isinstance(self._kwargs["memory"], (int, unicode, basestring))
            and int(self._kwargs["memory"]) > 0
        ):
            raise KubernetesJobException(
                "Invalid memory value ({}); it should be greater than 0".format(
                    self._kwargs["memory"]
                )
            )

        # Disk value should be greater than 0
        if not (
            isinstance(self._kwargs["disk"], (int, unicode, basestring))
            and int(self._kwargs["disk"]) > 0
        ):
            raise KubernetesJobException(
                "Invalid disk value ({}); it should be greater than 0".format(
                    self._kwargs["disk"]
                )
            )

        # TODO(s) (savin)
        # 1. Add support for GPUs.

        # A discerning eye would notice and question the choice of using the
        # V1Job construct over the V1Pod construct given that we don't rely much
        # on any of the V1Job semantics. The major reasons at the moment are -
        #     1. It makes the Kubernetes UIs (Octant, Lens) a bit more easy on
        #        the eyes, although even that can be questioned.
        #     2. AWS Step Functions, at the moment (Aug' 21) only supports
        #        executing Jobs and not Pods as part of it's publicly declared
        #        API. When we ship the AWS Step Functions integration with EKS,
        #        it will hopefully lessen our workload.
        #
        # Note: This implementation ensures that there is only one unique Pod
        # (unique UID) per Metaflow task attempt.
        client = self._client_wrapper.get()
        self._job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                # Annotations are for humans
                annotations=self._kwargs.get("annotations", {}),
                # While labels are for Kubernetes
                labels=self._kwargs.get("labels", {}),
                name=self._kwargs["name"],  # Unique within the namespace
                namespace=self._kwargs["namespace"],  # Defaults to `default`
            ),
            spec=client.V1JobSpec(
                # Retries are handled by Metaflow when it is responsible for
                # executing the flow. The responsibility is moved to Kubernetes
                # when AWS Step Functions / Argo are responsible for the
                # execution.
                backoff_limit=self._kwargs.get("retries", 0),
                completions=1,  # A single non-indexed pod job
                # TODO (savin): Implement a job clean-up option in the
                # kubernetes CLI.
                ttl_seconds_after_finished=7
                * 60
                * 60  # Remove job after a week. TODO (savin): Make this
                * 24,  # configurable
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        annotations=self._kwargs.get("annotations", {}),
                        labels=self._kwargs.get("labels", {}),
                        name=self._kwargs["name"],
                        namespace=self._kwargs["namespace"],
                    ),
                    spec=client.V1PodSpec(
                        # Timeout is set on the pod and not the job (important!)
                        active_deadline_seconds=self._kwargs["timeout_in_seconds"],
                        # TODO (savin): Enable affinities for GPU scheduling.
                        #               This requires some thought around the
                        #               UX since specifying affinities can get
                        #               complicated quickly. We may well decide
                        #               to move it out of scope for the initial
                        #               roll out.
                        # affinity=?,
                        containers=[
                            client.V1Container(
                                command=self._kwargs["command"],
                                env=[
                                    client.V1EnvVar(name=k, value=str(v))
                                    for k, v in self._kwargs.get(
                                        "environment_variables", {}
                                    ).items()
                                ]
                                # And some downward API magic. Add (key, value)
                                # pairs below to make pod metadata available
                                # within Kubernetes container.
                                #
                                # TODO: Figure out a way to make job
                                # metadata visible within the container
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
                                    }.items()
                                ],
                                env_from=[
                                    client.V1EnvFromSource(
                                        secret_ref=client.V1SecretEnvSource(name=str(k))
                                    )
                                    for k in self._kwargs.get("secrets", [])
                                ],
                                image=self._kwargs["image"],
                                name=self._kwargs["name"],
                                resources=client.V1ResourceRequirements(
                                    requests={
                                        "cpu": str(self._kwargs["cpu"]),
                                        "memory": "%sM" % str(self._kwargs["memory"]),
                                        "ephemeral-storage": "%sM"
                                        % str(self._kwargs["disk"]),
                                    }
                                ),
                            )
                        ],
                        node_selector={
                            # TODO: What should be the format of node selector -
                            #       key:value or key=value?
                            str(k.split("=", 1)[0]): str(k.split("=", 1)[1])
                            for k in self._kwargs.get("node_selector", [])
                        },
                        # TODO (savin): At some point in the very near future,
                        #               support docker access secrets.
                        # image_pull_secrets=?,
                        #
                        # TODO (savin): We should, someday, get into the pod
                        #               priority business
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
                        # TODO (savin): Enable tolerations for GPU scheduling.
                        #               This requires some thought around the
                        #               UX since specifying tolerations can get
                        #               complicated quickly.
                        # tolerations=?,
                        #
                        # TODO (savin): At some point in the very near future,
                        #               support custom volumes (PVCs/EVCs).
                        # volumes=?,
                        #
                        # TODO (savin): Set termination_message_policy
                    ),
                ),
            ),
        )
        return self

    def execute(self):
        client = self._client_wrapper.get()
        try:
            # TODO (savin): Make job submission back-pressure aware. Currently
            #               there doesn't seem to be a kubernetes-native way to
            #               achieve the guarantees that we are seeking.
            #               Hopefully, we will be able to get creative soon.
            response = (
                client.BatchV1Api()
                .create_namespaced_job(
                    body=self._job, namespace=self._kwargs["namespace"]
                )
                .to_dict()
            )
            return RunningJob(
                client_wrapper=self._client_wrapper,
                name=response["metadata"]["name"],
                uid=response["metadata"]["uid"],
                namespace=response["metadata"]["namespace"],
            )
        except client.rest.ApiException as e:
            raise KubernetesJobException(
                "Unable to launch Kubernetes job.\n %s" % str(e)
            )

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
    #
    # For example, for a successful job, RunningJob states and their corresponding
    # K8S object states look like this:
    #
    # | V1JobStatus.active | V1JobStatus.succeeded | V1PodStatus.phase | RunningJob.is_running | RunningJob.is_done |
    # |--------------------|-----------------------|-------------------|-----------------------|--------------------|
    # |                  0 |                     0 | N/A               | False                 | False              |
    # |                  0 |                     0 | Pending           | False                 | False              |
    # |                  1 |                     0 | Running           | True                  | False              |
    # |                  1 |                     0 | Succeeded         | True                  | True               |
    # |                  0 |                     1 | Succeeded         | False                 | True               |

    # To ascertain the status of V1Job, we peer into the lifecycle status of
    # the pod it is responsible for executing. Unfortunately, the `phase`
    # attributes (pending, running, succeeded, failed etc.) only provide
    # partial answers and the official API conventions guide suggests that
    # it may soon be deprecated (however, not anytime soon - see
    # https://github.com/kubernetes/kubernetes/issues/7856). `conditions` otoh
    # provide a deeper understanding about the state of the pod; however
    # conditions are not state machines and can be oscillating - from the
    # offical API conventions guide:
    #     In general, condition values may change back and forth, but some
    #     condition transitions may be monotonic, depending on the resource and
    #     condition type. However, conditions are observations and not,
    #     themselves, state machines, nor do we define comprehensive state
    #     machines for objects, nor behaviors associated with state
    #     transitions. The system is level-based rather than edge-triggered,
    #     and should assume an Open World.
    # In this implementation, we synthesize our notion of "phase" state
    # machine from `conditions`, since Kubernetes won't do it for us (for
    # many good reasons).
    #
    #
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
    #    5. (kube-scheduler) Unschedulable
    #
    # WIP...

    JOB_ACTIVE = "job:active"
    JOB_FAILED = ""

    def __init__(self, client_wrapper, name, uid, namespace):
        self._client_wrapper = client_wrapper
        self._name = name
        self._id = uid
        self._namespace = namespace

        self._job = self._fetch_job()
        self._pod = self._fetch_pod()

        import atexit

        atexit.register(self.kill)

    def __repr__(self):
        return "{}('{}/{}')".format(
            self.__class__.__name__, self._namespace, self._name
        )

    @k8s_retry()
    def _fetch_job(self):
        client = self._client_wrapper.get()
        try:
            return (
                client.BatchV1Api()
                .read_namespaced_job(name=self._name, namespace=self._namespace)
                .to_dict()
            )
        except client.rest.ApiException as e:
            # TODO: Handle failures as well as the fact that a different
            #       process can delete the job.
            raise e

    @k8s_retry()
    def _fetch_pod(self):
        """Fetch pod metadata. May return None if pod does not exist."""
        client = self._client_wrapper.get()

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
        else:
            return None

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
        #    https://github.com/kubernetes/enhancements/issues/2232 but
        #    meanwhile as a quick follow-up, we should investigate ways to
        #    terminate the pod without deleting the object.
        # 3. If the pod object hasn't shown up yet, we set the parallelism to 0
        #    to preempt it.
        client = self._client_wrapper.get()
        if not self._check_is_done():
            if self._check_is_running():

                # Unless there is a bug in the code, self._pod cannot be None
                # if we're in "running" state.
                assert self._pod is not None

                # Case 1.
                from kubernetes.stream import stream

                api_instance = client.CoreV1Api
                try:
                    # TODO (savin): stream opens a web-socket connection. It may
                    #               not be desirable to open multiple web-socket
                    #               connections frivolously (think killing a
                    #               workflow during a for-each step).
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
                    # TODO (savin): Forward the error to the user.
                    # pass
                    raise
            else:
                # Case 2.
                try:
                    # TODO (savin): Also patch job annotation to reflect this
                    #               action.
                    client.BatchV1Api().patch_namespaced_job(
                        name=self._name,
                        namespace=self._namespace,
                        field_manager="metaflow",
                        body={"spec": {"parallelism": 0}},
                    )
                except:
                    # Best effort.
                    # TODO (savin): Forward the error to the user.
                    # pass
                    raise
        return self

    @property
    def id(self):
        # TODO (savin): Should we use pod id instead?
        return self._id

    def _check_is_done(self):
        def _job_done():
            # Either the job succeeds or fails naturally or we may have
            # forced the pod termination causing the job to still be in an
            # active state but for all intents and purposes dead to us.

            # TODO (savin): check for self._job
            return (
                bool(self._job["status"].get("succeeded"))
                or bool(self._job["status"].get("failed"))
                or (self._job["spec"]["parallelism"] == 0)
            )

        if not _job_done():
            # If not done, check for newer status
            self._job = self._fetch_job()
        if _job_done():
            return True
        else:
            # It is possible for the job metadata to not be updated yet, but the
            # Pod has already succeeded or failed.
            self._pod = self._fetch_pod()
            if self._pod and (self._pod["status"]["phase"] in ("Succeeded", "Failed")):
                return True
            else:
                return False

    def _get_status(self):
        if not self._check_is_done():
            # If not done, check for newer pod status
            self._pod = self._fetch_pod()
        # Success!
        if bool(self._job["status"].get("succeeded")):
            return "Job:Succeeded"
        # Failure!
        if bool(self._job["status"].get("failed")) or (
            self._job["spec"]["parallelism"] == 0
        ):
            return "Job:Failed"
        if bool(self._job["status"].get("active")):
            msg = "Job:Active"
            if self._pod:
                msg += " Pod:%s" % self._pod["status"]["phase"].title()
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
                    msg += " Container:%s" % status["status"].title()
                    reason = ""
                    if status.get("reason"):
                        reason = status["reason"]
                    if status.get("message"):
                        reason += ":%s" % status["message"]
                    if reason:
                        msg += " [%s]" % reason
            # TODO (savin): This message should be shortened before release.
            return msg
        return "Job:Unknown"

    def _check_has_succeeded(self):
        # Job is in a terminal state and the status is marked as succeeded
        if self._check_is_done():
            if bool(self._job["status"].get("succeeded")) or (
                self._pod and self._pod["status"]["phase"] == "Succeeded"
            ):
                return True
            else:
                return False
        else:
            return False

    def _check_has_failed(self):
        # Job is in a terminal state and either the status is marked as failed
        # or the Job is not allowed to launch any more pods

        if self._check_is_done():
            if (
                bool(self._job["status"].get("failed"))
                or (self._job["spec"]["parallelism"] == 0)
                or (self._pod and self._pod["status"]["phase"] == "Failed")
            ):
                return True
            else:
                return False
        else:
            return False

    def _check_is_running(self):
        # Returns true if the container is running.
        if not self._check_is_done():
            # If not done, check if pod has been assigned and is in Running
            # phase
            if self._pod is None:
                return False
            pod_phase = self._pod.get("status", {}).get("phase")
            return pod_phase == "Running"
        return False

    def _get_done_reason(self):
        if self._check_is_done():
            if self._check_has_succeeded():
                return 0, None
            # Best effort since Pod object can disappear on us at anytime
            else:

                def _done():
                    return self._pod.get("status", {}).get("phase") in (
                        "Succeeded",
                        "Failed",
                    )

                if not _done():
                    # If pod status is dirty, check for newer status
                    self._pod = self._fetch_pod()
                if self._pod:
                    pod_status = self._pod["status"]
                    if pod_status.get("container_statuses") is None:
                        # We're done, but no container_statuses is set
                        # This can happen when the pod is evicted
                        return None, ": ".join(
                            filter(
                                None,
                                [pod_status.get("reason"), pod_status.get("message")],
                            )
                        )

                    for k, v in (
                        pod_status.get("container_statuses", [{}])[0]
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

    @property
    def is_done(self):
        return self._check_is_done()

    @property
    def has_failed(self):
        return self._check_has_failed()

    @property
    def is_running(self):
        return self._check_is_running()

    @property
    def reason(self):
        return self._get_done_reason()

    @property
    def status(self):
        return self._get_status()
