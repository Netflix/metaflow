import copy
import math
import random
import time
from metaflow.metaflow_current import current
from metaflow.exception import MetaflowException
from metaflow.unbounded_foreach import UBF_CONTROL, UBF_TASK
import json
from metaflow.metaflow_config import KUBERNETES_JOBSET_GROUP, KUBERNETES_JOBSET_VERSION
from collections import namedtuple


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
            except Exception as ex:
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
        # Get the jobset
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

                # Suspend the jobset and set the replica's to Zero.
                #
                jobset["spec"]["suspend"] = True
                for replicated_job in jobset["spec"]["replicatedJobs"]:
                    replicated_job["replicas"] = 0

                api_instance.replace_namespaced_custom_object(
                    group=self._group,
                    version=self._version,
                    namespace=self._namespace,
                    plural=plural,
                    name=jobset["metadata"]["name"],
                    body=jobset,
                )
            except Exception as e:
                raise KubernetesJobsetException(
                    "Exception when suspending existing jobset: %s\n" % e
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


class TaskIdConstructor:
    @classmethod
    def jobset_worker_id(cls, control_task_id: str):
        return "".join(
            [control_task_id.replace("control", "worker"), "-", "$WORKER_REPLICA_INDEX"]
        )

    @classmethod
    def join_step_task_ids(cls, num_parallel):
        """
        Called within the step decorator to set the `flow._control_mapper_tasks`.
        Setting these allows the flow to know which tasks are needed in the join step.
        We set this in the `task_pre_step` method of the decorator.
        """
        control_task_id = current.task_id
        worker_task_id_base = control_task_id.replace("control", "worker")
        mapper = lambda idx: worker_task_id_base + "-%s" % (str(idx))
        return control_task_id, [mapper(idx) for idx in range(0, num_parallel - 1)]

    @classmethod
    def argo(cls):
        pass


def _jobset_specific_env_vars(client, jobset_main_addr, master_port, num_parallel):
    return [
        client.V1EnvVar(
            name="MASTER_ADDR",
            value=jobset_main_addr,
        ),
        client.V1EnvVar(
            name="MASTER_PORT",
            value=str(master_port),
        ),
        client.V1EnvVar(
            name="WORLD_SIZE",
            value=str(num_parallel),
        ),
    ] + [
        client.V1EnvVar(
            name="JOBSET_RESTART_ATTEMPT",
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(
                    field_path="metadata.annotations['jobset.sigs.k8s.io/restart-attempt']"
                )
            ),
        ),
        client.V1EnvVar(
            name="METAFLOW_KUBERNETES_JOBSET_NAME",
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(
                    field_path="metadata.annotations['jobset.sigs.k8s.io/jobset-name']"
                )
            ),
        ),
        client.V1EnvVar(
            name="WORKER_REPLICA_INDEX",
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(
                    field_path="metadata.annotations['jobset.sigs.k8s.io/job-index']"
                )
            ),
        ),
    ]


def get_control_job(
    client,
    job_spec,
    jobset_main_addr,
    subdomain,
    port=None,
    num_parallel=None,
    namespace=None,
    annotations=None,
) -> dict:
    master_port = port

    job_spec = copy.deepcopy(job_spec)
    job_spec.parallelism = 1
    job_spec.completions = 1
    job_spec.template.spec.set_hostname_as_fqdn = True
    job_spec.template.spec.subdomain = subdomain
    job_spec.template.metadata.annotations = copy.copy(annotations)

    for idx in range(len(job_spec.template.spec.containers[0].command)):
        # CHECK FOR THE ubf_context in the command.
        # Replace the UBF context to the one appropriately matching control/worker.
        # Since we are passing the `step_cli` one time from the top level to one
        # KuberentesJobSet, we need to ensure that UBF context is replaced properly
        # in all the worker jobs.
        if UBF_CONTROL in job_spec.template.spec.containers[0].command[idx]:
            job_spec.template.spec.containers[0].command[idx] = (
                job_spec.template.spec.containers[0]
                .command[idx]
                .replace(UBF_CONTROL, UBF_CONTROL + " " + "--split-index 0")
            )

    job_spec.template.spec.containers[0].env = (
        job_spec.template.spec.containers[0].env
        + _jobset_specific_env_vars(client, jobset_main_addr, master_port, num_parallel)
        + [
            client.V1EnvVar(
                name="CONTROL_INDEX",
                value=str(0),
            )
        ]
    )

    # Based on https://github.com/kubernetes-sigs/jobset/blob/v0.5.0/api/jobset/v1alpha2/jobset_types.go#L178
    return dict(
        name="control",
        template=client.api_client.ApiClient().sanitize_for_serialization(
            client.V1JobTemplateSpec(
                metadata=client.V1ObjectMeta(
                    namespace=namespace,
                    # We don't set any annotations here
                    # since they have been either set in the JobSpec
                    # or on the JobSet level
                ),
                spec=job_spec,
            )
        ),
        replicas=1,  # The control job will always have 1 replica.
    )


def get_worker_job(
    client,
    job_spec,
    job_name,
    jobset_main_addr,
    subdomain,
    control_task_id=None,
    worker_task_id=None,
    replicas=1,
    port=None,
    num_parallel=None,
    namespace=None,
    annotations=None,
) -> dict:
    master_port = port

    job_spec = copy.deepcopy(job_spec)
    job_spec.parallelism = 1
    job_spec.completions = 1
    job_spec.template.spec.set_hostname_as_fqdn = True
    job_spec.template.spec.subdomain = subdomain
    job_spec.template.metadata.annotations = copy.copy(annotations)

    for idx in range(len(job_spec.template.spec.containers[0].command)):
        if control_task_id in job_spec.template.spec.containers[0].command[idx]:
            job_spec.template.spec.containers[0].command[idx] = (
                job_spec.template.spec.containers[0]
                .command[idx]
                .replace(control_task_id, worker_task_id)
            )
        # CHECK FOR THE ubf_context in the command.
        # Replace the UBF context to the one appropriately matching control/worker.
        # Since we are passing the `step_cli` one time from the top level to one
        # KuberentesJobSet, we need to ensure that UBF context is replaced properly
        # in all the worker jobs.
        if UBF_CONTROL in job_spec.template.spec.containers[0].command[idx]:
            # Since all command will have a UBF_CONTROL, we need to replace the UBF_CONTROL
            # with the actual UBF Context and also ensure that we are setting the correct
            # split-index for the worker jobs.
            split_index_str = "--split-index `expr $[WORKER_REPLICA_INDEX] + 1`"  # This set in the environment variables below
            job_spec.template.spec.containers[0].command[idx] = (
                job_spec.template.spec.containers[0]
                .command[idx]
                .replace(UBF_CONTROL, UBF_TASK + " " + split_index_str)
            )

    job_spec.template.spec.containers[0].env = job_spec.template.spec.containers[
        0
    ].env + _jobset_specific_env_vars(
        client, jobset_main_addr, master_port, num_parallel
    )

    # Based on https://github.com/kubernetes-sigs/jobset/blob/v0.5.0/api/jobset/v1alpha2/jobset_types.go#L178
    return dict(
        name=job_name,
        template=client.api_client.ApiClient().sanitize_for_serialization(
            client.V1JobTemplateSpec(
                metadata=client.V1ObjectMeta(
                    namespace=namespace,
                    # We don't set any annotations here
                    # since they have been either set in the JobSpec
                    # or on the JobSet level
                ),
                spec=job_spec,
            )
        ),
        replicas=replicas,
    )


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


class KubernetesJobSet(object):
    def __init__(
        self,
        client,
        name=None,
        job_spec=None,
        namespace=None,
        num_parallel=None,
        annotations=None,
        labels=None,
        port=None,
        task_id=None,
        **kwargs
    ):
        self._client = client
        self._kwargs = kwargs
        self._group = KUBERNETES_JOBSET_GROUP
        self._version = KUBERNETES_JOBSET_VERSION
        self.name = name

        main_job_name = "control"
        main_job_index = 0
        main_pod_index = 0
        subdomain = self.name
        num_parallel = int(1 if not num_parallel else num_parallel)
        self._namespace = namespace
        jobset_main_addr = _make_domain_name(
            self.name,
            main_job_name,
            main_job_index,
            main_pod_index,
            self._namespace,
        )

        annotations = {} if not annotations else annotations
        labels = {} if not labels else labels

        if "metaflow/task_id" in annotations:
            del annotations["metaflow/task_id"]

        control_job = get_control_job(
            client=self._client.get(),
            job_spec=job_spec,
            jobset_main_addr=jobset_main_addr,
            subdomain=subdomain,
            port=port,
            num_parallel=num_parallel,
            namespace=namespace,
            annotations=annotations,
        )
        worker_task_id = TaskIdConstructor.jobset_worker_id(task_id)
        worker_job = get_worker_job(
            client=self._client.get(),
            job_spec=job_spec,
            job_name="worker",
            jobset_main_addr=jobset_main_addr,
            subdomain=subdomain,
            control_task_id=task_id,
            worker_task_id=worker_task_id,
            replicas=num_parallel - 1,
            port=port,
            num_parallel=num_parallel,
            namespace=namespace,
            annotations=annotations,
        )
        worker_jobs = [worker_job]
        # Based on https://github.com/kubernetes-sigs/jobset/blob/v0.5.0/api/jobset/v1alpha2/jobset_types.go#L163
        _kclient = client.get()
        self._jobset = dict(
            apiVersion=self._group + "/" + self._version,
            kind="JobSet",
            metadata=_kclient.api_client.ApiClient().sanitize_for_serialization(
                _kclient.V1ObjectMeta(
                    name=self.name, labels=labels, annotations=annotations
                )
            ),
            spec=dict(
                replicatedJobs=[control_job] + worker_jobs,
                suspend=False,
                startupPolicy=None,
                successPolicy=None,
                # The Failure Policy helps setting the number of retries for the jobset.
                # It cannot accept a value of 0 for maxRestarts.
                # So the attempt needs to be smartly set.
                # If there is no retry decorator then we not set maxRestarts and instead we will
                # set the attempt statically to 0. Otherwise we will make the job pickup the attempt
                # from the `V1EnvVarSource.value_from.V1ObjectFieldSelector.field_path` = "metadata.annotations['jobset.sigs.k8s.io/restart-attempt']"
                # failurePolicy={
                #     "maxRestarts" : 1
                # },
                # The can be set for ArgoWorkflows
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
                    body=self._jobset,
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
