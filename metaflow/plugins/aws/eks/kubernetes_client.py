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

        # TODO (savin): Guard kubernetes import within a try..except.
        try:
            from kubernetes import client, config
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import module 'kubernetes'. Install kubernetes Python package first."
            )
        from kubernetes import client, config

        config.load_kube_config()
        self._client = client.BatchV1Api()

    def job(self):
        return KubernetesJob(self._client)

    def attach_job(self, job_id):
        job = RunningJob(job_id, self._client)
        return job.update()


class KubernetesJobException(MetaflowException):
    headline = "Kubernetes job error"


class KubernetesJob(object):
    def __init__(self, client):
        # TODO (savin): Guard kubernetes import within a try..except.
        # TODO(?) (savin): Remove dependency on Kubernetes Python SDK.
        from kubernetes import client, config

        self._client = client
        self._job = None
        self._namespace = None

    def create(
        self,
        name,
        namespace,
        service_account,
        annotations,
        labels,
        env,
        command,
        image,
        cpu,
        memory,
        retries,
        timeout_in_secs,
    ):
        self._namespace = namespace
        self._job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                annotations=self._annotations,  # annotations are for humans
                labels=self._labels,  # while labels are for kubernetes
                name=self._name,  # unique within the namespace
                namespace=self._namespace,  # empty defaults to `default`
            ),
            spec=client.V1JobSpec(
                backoff_limit=self._retries,
                ttl_seconds_after_finished=0,  # delete the job immediately
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        annotations=self._annotations,
                        labels=self._labels,
                        name=self._name,
                        namespace=self._namespace,
                    ),
                    spec=client.V1PodSpec(
                        # Timeout is set on the pod
                        active_deadline_seconds=self._timeout_in_secs,
                        # affinity=?,
                        # automount_service_account_token=?,
                        containers=[
                            client.V1Container(
                                command=self._command,
                                env=[
                                    client.V1EnvVar(name=k, value=v)
                                    for k, v in self._env.items()
                                ],
                                image=self._image,
                                name=self._name,
                                resources=client.V1ResourceRequirements(
                                    requests={
                                        "cpu": self._cpu,
                                        "memory": self._memory,
                                    }
                                ),
                            )
                        ],
                        # image_pull_secrets=?,
                        # preemption_policy=?,
                        restart_policy="Never",
                        service_account_name=self._service_account,
                        # tolerations=?,
                        # volumes=?,
                    ),
                ),
            ),
        )

    def execute(self):
        config.load_kube_config()

        response = client.BatchV1Api().create_namespaced_job(
            body=self._job, namespace=self._namespace
        )
        return RunningJob(
            response.to_dict()["spec"]["template"]["metadata"]["name"],
            self._client,
        )

    def name(self, name):
        self._name = name
        return self

    def command(self, command):
        self._command = command
        return self

    def image(self, image):
        self._image = image
        return self

    def cpu(self, cpu):
        if not (
            isinstance(cpu, (int, unicode, basestring, float))
            and float(cpu) > 0
        ):
            raise KubernetesJobException(
                "Invalid CPU value ({}); it should be greater than 0".format(
                    cpu
                )
            )
        self._cpu = str(cpu)
        return self

    def memory(self, mem):
        if not (isinstance(mem, (int, unicode, basestring)) and int(mem) > 0):
            raise KubernetesJobException(
                "Invalid memory value ({}); it should be greater than 0".format(
                    mem
                )
            )
        self._memory = str(mem) + "M"
        return self

    def environment_variable(self, name, value):
        if name in self.env:
            raise KubernetesJobException(
                "Duplicate environment variable ({})".format(name)
            )
        self._env[name] = str(value)
        return self
