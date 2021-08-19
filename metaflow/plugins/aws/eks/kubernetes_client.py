from collections import defaultdict

try:
    unicode
except NameError:
    unicode = str
    basestring = str

from metaflow.exception import MetaflowException

# TODO (savin): Ensure that the client works swimmingly well with the sandbox.
class KubernetesClient(object):
    def __init__(self):
        # TODO (savin): Look into removing the usage of Kubernetes Python SDK
        # at some point in the future. Given that Kubernetes Python SDK
        # aggressively drops support for older kubernetes clusters, continued
        # dependency on it may bite our users.

        # TODO (savin): Look into various ways to configure the Kubernetes
        # client.
        try:
            # Kubernetes is a soft dependency.
            from kubernetes import client, config
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import module 'kubernetes'. Install kubernetes "
                "Python package (https://pypi.org/project/kubernetes/) first."
            )

        config.load_kube_config()
        self._client = client

    def job(self, **kwargs):
        return KubernetesJob(self._client, **kwargs)

    def attach_job(self, job_id):
        job = RunningJob(job_id, self._client)
        return job.update()


class KubernetesJobException(MetaflowException):
    headline = "Kubernetes job error"


class KubernetesJob(object):
    def __init__(self, client, **kwargs):
        self._client = client
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

        # TODO(s)
        # 1. Find a way to ensure that a pod is cleanly terminated automatically
        #    if the container fails to start properly (invalid docker image
        #    etc.)

        # A discerning eye would notice and question the choice of using the
        # V1Job construct over the V1Pod construct given that we don't rely on
        # any of the V1Job semantics. The only reasons at the moment are -
        #     1. It makes the Kubernetes UIs (Octant, Lens) a bit more easy on
        #        the eyes, although even that can be questioned.
        #     2. AWS Step Functions, at the moment (Aug' 21) only supports
        #        executing Jobs and not Pods as part of it's publicly declared
        #        API. When we ship the AWS Step Functions integration with EKS,
        #        it will hopefully lessen our workload.
        #
        # The current implementation assumes that there is only one unique Pod
        # (unique UID) per Metaflow task attempt.
        self._job = self._client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=self._client.V1ObjectMeta(
                # Annotations are for humans
                annotations=self._kwargs.get("annotations", {}),
                # While labels are for Kubernetes
                labels=self._kwargs.get("labels", {}),
                name=self._kwargs["name"],  # Unique within the namespace
                namespace=self._kwargs["namespace"],  # Defaults to `default`
            ),
            spec=self._client.V1JobSpec(
                # Retries are handled by Metaflow when it is responsible for
                # executing the flow. The responsibility is moved to Kubernetes
                # when AWS Step Functions / Argo are responsible for the
                # execution.
                backoff_limit=self._kwargs.get("retries", 0),
                ttl_seconds_after_finished=0,  # Delete the job immediately
                template=self._client.V1PodTemplateSpec(
                    metadata=self._client.V1ObjectMeta(
                        annotations=self._kwargs.get("annotations", {}),
                        labels=self._kwargs.get("labels", {}),
                        name=self._kwargs["name"],
                        namespace=self._kwargs["namespace"],
                    ),
                    spec=self._client.V1PodSpec(
                        # Timeout is set on the pod and not the job (important!)
                        active_deadline_seconds=self._kwargs[
                            "timeout_in_seconds"
                        ],
                        # affinity=?,
                        # automount_service_account_token=?,
                        containers=[
                            self._client.V1Container(
                                command=self._kwargs["command"],
                                env=[
                                    self._client.V1EnvVar(name=k, value=str(v))
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
                                    self._client.V1EnvVar(
                                        name=k,
                                        value_from=self._client.V1EnvVarSource(
                                            field_ref=self._client.V1ObjectFieldSelector(
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
                                image=self._kwargs["image"],
                                name=self._kwargs["name"],
                                resources=self._client.V1ResourceRequirements(
                                    requests={
                                        "cpu": str(self._kwargs["cpu"]),
                                        "memory": "%sM"
                                        % str(self._kwargs["memory"]),
                                    }
                                ),
                            )
                        ],
                        # image_pull_secrets=?,
                        # preemption_policy=?,
                        #
                        # A Container in a Pod may fail for a number of
                        # reasons, such as because the process in it exited
                        # with a non-zero exit code, or the Container was
                        # killed due to OOM etc. If this happens, fail the pod
                        # and let Metaflow handle the retries.
                        restart_policy="Never",
                        service_account_name=self._kwargs["service_account"],
                        # tolerations=?,
                        # volumes=?,
                    ),
                ),
            ),
        )
        return self

    def execute(self):
        try:
            response = (
                self._client.BatchV1Api()
                .create_namespaced_job(
                    body=self._job, namespace=self._kwargs["namespace"]
                )
                .to_dict()
            )
            return RunningJob(
                client=self._client,
                name=response["metadata"]["name"],
                namespace=response["metadata"]["namespace"],
            )
        except self._client.rest.ApiException as e:
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
        self._kwargs["memory"] = memory
        return self

    def environment_variable(self, name, value):
        self._kwargs["environment_variables"] = dict(
            self._kwargs.get("environment_variables", {}), **{name: value}
        )
        return self

    def label(self, name, value):
        self._kwargs["labels"] = dict(
            self._kwargs.get("labels", {}), **{name: value}
        )
        return self

    def annotation(self, name, value):
        self._kwargs["annotations"] = dict(
            self._kwargs.get("annotations", {}), **{name: value}
        )
        return self


class RunningJob(object):

    # StateMachine implementation for the lifecycle behavior documented in
    # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/

    def __init__(self, client, name, namespace):
        self._client = client
        self._name = name
        self._namespace = namespace

        self._pod = None

        self.update()
        self._id = self._pod["metadata"]["labels"]["controller-uid"]

    def __repr__(self):
        return "{}('{}/{}')".format(
            self.__class__.__name__, self._namespace, self._name
        )

    def _update(self):
        try:
            return (
                self._client.CoreV1Api()
                .list_namespaced_pod(
                    namespace=self._namespace,
                    label_selector="job-name={}".format(self._name),
                )
                .to_dict()["items"][0]
            )
        except self._client.rest.ApiException as e:
            # TODO: Handle failures
            raise e

    def update(self):
        self._pod = self._update()
        # print(self._pod["status"])
        return self

    @property
    def id(self):
        return self._id

    @property
    def is_done(self):
        def _done():
            return self._pod["status"]["phase"] in ("Succeeded", "Failed")

        if not _done():
            # if not done, check for newer status
            self.update()
        return _done()

    @property
    def status(self):
        if not self.is_done:
            # if not done, check for newer status (see the implementation of
            # self.is_done)
            pass
        return self._pod["status"]["phase"]

    @property
    def has_succeeded(self):
        return self.status == "Succeeded"

    @property
    def has_failed(self):
        return self.status == "Failed"

    @property
    def is_running(self):
        return self.status == "Running"

    @property
    def container_status(self):
        return (
            self._pod["status"].get("container_statuses", [{}])[0].get("state")
        )

    @property
    def reason(self):
        return "foo"

    @property
    def status_code(self):
        return 1
