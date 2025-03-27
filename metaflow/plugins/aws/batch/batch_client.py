# -*- coding: utf-8 -*-
from collections import defaultdict
import copy
import random
import time
import hashlib

try:
    unicode
except NameError:
    unicode = str
    basestring = str

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import AWS_SANDBOX_ENABLED


class BatchClient(object):
    def __init__(self):
        from ..aws_client import get_aws_client

        self._client = get_aws_client("batch")

    def active_job_queues(self):
        paginator = self._client.get_paginator("describe_job_queues")
        return (
            queue["jobQueueName"]
            for page in paginator.paginate()
            for queue in page["jobQueues"]
            if queue["state"] == "ENABLED" and queue["status"] == "VALID"
        )

    def unfinished_jobs(self):
        queues = self.active_job_queues()
        return (
            job
            for queue in queues
            for status in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]
            for page in self._client.get_paginator("list_jobs").paginate(
                jobQueue=queue, jobStatus=status
            )
            for job in page["jobSummaryList"]
        )

    def describe_jobs(self, job_ids):
        for jobIds in [job_ids[i : i + 100] for i in range(0, len(job_ids), 100)]:
            for jobs in self._client.describe_jobs(jobs=jobIds)["jobs"]:
                yield jobs

    def describe_job_queue(self, job_queue):
        paginator = self._client.get_paginator("describe_job_queues").paginate(
            jobQueues=[job_queue], maxResults=1
        )
        return paginator.paginate()["jobQueues"][0]

    def job(self):
        return BatchJob(self._client)

    def attach_job(self, job_id):
        job = RunningJob(job_id, self._client)
        return job.update()

    def region(self):
        return self._client._client_config.region_name


class BatchJobException(MetaflowException):
    headline = "AWS Batch job error"


class BatchJob(object):
    def __init__(self, client):
        self._client = client
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def execute(self):
        if self._image is None:
            raise BatchJobException(
                "Unable to launch AWS Batch job. No docker image specified."
            )
        if self._iam_role is None:
            raise BatchJobException(
                "Unable to launch AWS Batch job. No IAM role specified."
            )

        # Multinode
        if getattr(self, "num_parallel", 0) >= 1:
            num_nodes = self.num_parallel
            # We need this task-id set so that all the nodes are aware of the control
            # task's task-id. These "MF_" variables populate the `current.parallel` namedtuple
            self.environment_variable("MF_PARALLEL_CONTROL_TASK_ID", self._task_id)
            main_task_override = copy.deepcopy(self.payload["containerOverrides"])

            # main
            commands = self.payload["containerOverrides"]["command"][-1]
            # add split-index as this worker is also an ubf_task
            commands = commands.replace("[multinode-args]", "--split-index 0")
            main_task_override["command"][-1] = commands

            # secondary tasks
            secondary_task_container_override = copy.deepcopy(
                self.payload["containerOverrides"]
            )
            secondary_commands = self.payload["containerOverrides"]["command"][-1]
            # other tasks do not have control- prefix, and have the split id appended to the task -id
            secondary_commands = secondary_commands.replace(
                self._task_id,
                self._task_id.replace("control-", "")
                + "-node-$AWS_BATCH_JOB_NODE_INDEX",
            )
            secondary_commands = secondary_commands.replace(
                "ubf_control",
                "ubf_task",
            )
            secondary_commands = secondary_commands.replace(
                "[multinode-args]", "--split-index $AWS_BATCH_JOB_NODE_INDEX"
            )

            secondary_task_container_override["command"][-1] = secondary_commands
            secondary_overrides = (
                [
                    {
                        "targetNodes": "1:{}".format(num_nodes - 1),
                        "containerOverrides": secondary_task_container_override,
                    }
                ]
                if num_nodes > 1
                else []
            )
            self.payload["nodeOverrides"] = {
                "nodePropertyOverrides": [
                    {"targetNodes": "0:0", "containerOverrides": main_task_override},
                ]
                + secondary_overrides,
            }
            del self.payload["containerOverrides"]

        response = self._client.submit_job(**self.payload)
        job = RunningJob(response["jobId"], self._client)
        return job.update()

    def _register_job_definition(
        self,
        image,
        job_role,
        job_queue,
        execution_role,
        shared_memory,
        max_swap,
        swappiness,
        inferentia,
        efa,
        memory,
        host_volumes,
        efs_volumes,
        use_tmpfs,
        tmpfs_tempdir,
        tmpfs_size,
        tmpfs_path,
        num_parallel,
        ephemeral_storage,
        log_driver,
        log_options,
    ):
        # identify platform from any compute environment associated with the
        # queue
        if AWS_SANDBOX_ENABLED:
            # within the Metaflow sandbox, we can't execute the
            # describe_job_queues directive for AWS Batch to detect compute
            # environment platform, so let's just default to EC2 for now.
            platform = "EC2"
        else:
            response = self._client.describe_job_queues(jobQueues=[job_queue])
            if len(response["jobQueues"]) == 0:
                raise BatchJobException("AWS Batch Job Queue %s not found." % job_queue)
            compute_environment = response["jobQueues"][0]["computeEnvironmentOrder"][
                0
            ]["computeEnvironment"]
            response = self._client.describe_compute_environments(
                computeEnvironments=[compute_environment]
            )
            platform = response["computeEnvironments"][0]["computeResources"]["type"]

        # compose job definition
        job_definition = {
            "type": "container",
            "containerProperties": {
                "image": image,
                "jobRoleArn": job_role,
                "command": ["echo", "hello world"],
                "resourceRequirements": [
                    {"value": "1", "type": "VCPU"},
                    {"value": "4096", "type": "MEMORY"},
                ],
            },
            # This propagates the AWS Batch resource tags to the underlying
            # ECS tasks.
            "propagateTags": True,
        }

        log_options_dict = {}
        if log_options:
            if isinstance(log_options, str):
                log_options = [log_options]
            for each_log_option in log_options:
                k, v = each_log_option.split(":", 1)
                log_options_dict[k] = v

        if log_driver or log_options:
            job_definition["containerProperties"]["logConfiguration"] = {}
        if log_driver:
            job_definition["containerProperties"]["logConfiguration"][
                "logDriver"
            ] = log_driver
        if log_options:
            job_definition["containerProperties"]["logConfiguration"][
                "options"
            ] = log_options_dict

        if platform == "FARGATE" or platform == "FARGATE_SPOT":
            if num_parallel > 1:
                raise BatchJobException("Fargate does not support multinode jobs.")
            if execution_role is None:
                raise BatchJobException(
                    "No AWS Fargate task execution IAM role found. Please see "
                    "https://docs.aws.amazon.com/batch/latest/userguide/execution-IAM-role.html "
                    "and set the role as METAFLOW_ECS_FARGATE_EXECUTION_ROLE "
                    "environment variable."
                )
            job_definition["containerProperties"]["executionRoleArn"] = execution_role
            job_definition["platformCapabilities"] = ["FARGATE"]
            job_definition["containerProperties"]["networkConfiguration"] = {
                "assignPublicIp": "ENABLED"
            }
            if ephemeral_storage:
                job_definition["containerProperties"]["ephemeralStorage"] = {
                    "sizeInGiB": ephemeral_storage
                }

        if platform == "EC2" or platform == "SPOT":
            if "linuxParameters" not in job_definition["containerProperties"]:
                job_definition["containerProperties"]["linuxParameters"] = {}
            if shared_memory is not None:
                if not (
                    isinstance(shared_memory, (int, unicode, basestring))
                    and int(float(shared_memory)) > 0
                ):
                    raise BatchJobException(
                        "Invalid shared memory size value ({}); "
                        "it should be greater than 0".format(shared_memory)
                    )
                else:
                    job_definition["containerProperties"]["linuxParameters"][
                        "sharedMemorySize"
                    ] = int(float(shared_memory))
            if swappiness is not None:
                if not (
                    isinstance(swappiness, (int, unicode, basestring))
                    and int(swappiness) >= 0
                    and int(swappiness) < 100
                ):
                    raise BatchJobException(
                        "Invalid swappiness value ({}); "
                        "(should be 0 or greater and less than 100)".format(swappiness)
                    )
                else:
                    job_definition["containerProperties"]["linuxParameters"][
                        "swappiness"
                    ] = int(swappiness)
            if max_swap is not None:
                if not (
                    isinstance(max_swap, (int, unicode, basestring))
                    and int(max_swap) >= 0
                ):
                    raise BatchJobException(
                        "Invalid swappiness value ({}); "
                        "(should be 0 or greater)".format(max_swap)
                    )
                else:
                    job_definition["containerProperties"]["linuxParameters"][
                        "maxSwap"
                    ] = int(max_swap)
            if ephemeral_storage:
                raise BatchJobException(
                    "The ephemeral_storage parameter is only available for FARGATE compute environments"
                )

        if inferentia:
            if not (isinstance(inferentia, (int, unicode, basestring))):
                raise BatchJobException(
                    "Invalid inferentia value: ({}) (should be 0 or greater)".format(
                        inferentia
                    )
                )
            else:
                job_definition["containerProperties"]["linuxParameters"]["devices"] = []
                for i in range(int(inferentia)):
                    job_definition["containerProperties"]["linuxParameters"][
                        "devices"
                    ].append(
                        {
                            "containerPath": "/dev/neuron{}".format(i),
                            "hostPath": "/dev/neuron{}".format(i),
                            "permissions": ["READ", "WRITE"],
                        }
                    )

        if host_volumes or efs_volumes:
            job_definition["containerProperties"]["volumes"] = []
            job_definition["containerProperties"]["mountPoints"] = []

            if host_volumes:
                if isinstance(host_volumes, str):
                    host_volumes = [host_volumes]
                for host_path in host_volumes:
                    container_path = host_path
                    if ":" in host_path:
                        host_path, container_path = host_path.split(":", 1)
                    name = host_path.replace("/", "_").replace(".", "_")
                    job_definition["containerProperties"]["volumes"].append(
                        {"name": name, "host": {"sourcePath": host_path}}
                    )
                    job_definition["containerProperties"]["mountPoints"].append(
                        {"sourceVolume": name, "containerPath": container_path}
                    )

            if efs_volumes:
                if isinstance(efs_volumes, str):
                    efs_volumes = [efs_volumes]
                for efs_id in efs_volumes:
                    container_path = "/mnt/" + efs_id
                    if ":" in efs_id:
                        efs_id, container_path = efs_id.split(":", 1)
                    name = "efs_" + efs_id
                    job_definition["containerProperties"]["volumes"].append(
                        {
                            "name": name,
                            "efsVolumeConfiguration": {
                                "fileSystemId": efs_id,
                                "transitEncryption": "ENABLED",
                            },
                        }
                    )
                    job_definition["containerProperties"]["mountPoints"].append(
                        {"sourceVolume": name, "containerPath": container_path}
                    )

        if use_tmpfs and (platform == "FARGATE" or platform == "FARGATE_SPOT"):
            raise BatchJobException(
                "tmpfs is not available for Fargate compute resources"
            )
        if use_tmpfs or (tmpfs_size and not use_tmpfs):
            if tmpfs_size:
                if not (isinstance(tmpfs_size, (int, unicode, basestring))):
                    raise BatchJobException(
                        "Invalid tmpfs value: ({}) (should be 0 or greater)".format(
                            tmpfs_size
                        )
                    )
            else:
                # default tmpfs behavior - https://man7.org/linux/man-pages/man5/tmpfs.5.html
                tmpfs_size = int(float(memory)) / 2

            job_definition["containerProperties"]["linuxParameters"]["tmpfs"] = [
                {
                    "containerPath": tmpfs_path,
                    "size": int(tmpfs_size),
                    "mountOptions": [
                        # should map to rw, suid, dev, exec, auto, nouser, and async
                        "defaults"
                    ],
                }
            ]

        if efa:
            if not (isinstance(efa, (int, unicode, basestring))):
                raise BatchJobException(
                    "Invalid efa value: ({}) (should be 0 or greater)".format(efa)
                )
            else:
                if "linuxParameters" not in job_definition["containerProperties"]:
                    job_definition["containerProperties"]["linuxParameters"] = {}
                if (
                    "devices"
                    not in job_definition["containerProperties"]["linuxParameters"]
                ):
                    job_definition["containerProperties"]["linuxParameters"][
                        "devices"
                    ] = []
                if (num_parallel or 0) > 1:
                    # Multi-node parallel jobs require the container path and permissions explicitly specified in Job definition
                    for i in range(int(efa)):
                        job_definition["containerProperties"]["linuxParameters"][
                            "devices"
                        ].append(
                            {
                                "hostPath": "/dev/infiniband/uverbs{}".format(i),
                                "containerPath": "/dev/infiniband/uverbs{}".format(i),
                                "permissions": ["READ", "WRITE", "MKNOD"],
                            }
                        )
                else:
                    # Single-node container jobs only require host path in job definition
                    job_definition["containerProperties"]["linuxParameters"][
                        "devices"
                    ].append({"hostPath": "/dev/infiniband/uverbs0"})

        self.num_parallel = num_parallel or 0
        if self.num_parallel >= 1:
            job_definition["type"] = "multinode"
            job_definition["nodeProperties"] = {
                "numNodes": self.num_parallel,
                "mainNode": 0,
            }
            job_definition["nodeProperties"]["nodeRangeProperties"] = [
                {
                    "targetNodes": "0:0",  # The properties are same for main node and others,
                    # but as we use nodeOverrides later for main and others
                    # differently, also the job definition must match those patterns
                    "container": job_definition["containerProperties"],
                },
            ]
            if self.num_parallel > 1:
                job_definition["nodeProperties"]["nodeRangeProperties"].append(
                    {
                        "targetNodes": "1:{}".format(self.num_parallel - 1),
                        "container": job_definition["containerProperties"],
                    }
                )

            del job_definition["containerProperties"]  # not used for multi-node

        # check if job definition already exists
        def_name = (
            "metaflow_%s"
            % hashlib.sha224(str(job_definition).encode("utf-8")).hexdigest()
        )
        payload = {"jobDefinitionName": def_name, "status": "ACTIVE"}
        response = self._client.describe_job_definitions(**payload)
        if len(response["jobDefinitions"]) > 0:
            return response["jobDefinitions"][0]["jobDefinitionArn"]

        # else create a job definition
        job_definition["jobDefinitionName"] = def_name
        try:
            response = self._client.register_job_definition(**job_definition)
        except Exception as ex:
            if type(ex).__name__ == "ParamValidationError" and (
                platform == "FARGATE" or platform == "FARGATE_SPOT"
            ):
                raise BatchJobException(
                    "%s \nPlease ensure you have installed boto3>=1.16.29 if "
                    "you intend to launch AWS Batch jobs on AWS Fargate "
                    "compute platform." % ex
                )
            else:
                raise ex
        return response["jobDefinitionArn"]

    def job_def(
        self,
        image,
        iam_role,
        job_queue,
        execution_role,
        shared_memory,
        max_swap,
        swappiness,
        inferentia,
        efa,
        memory,
        host_volumes,
        efs_volumes,
        use_tmpfs,
        tmpfs_tempdir,
        tmpfs_size,
        tmpfs_path,
        num_parallel,
        ephemeral_storage,
        log_driver,
        log_options,
    ):
        self.payload["jobDefinition"] = self._register_job_definition(
            image,
            iam_role,
            job_queue,
            execution_role,
            shared_memory,
            max_swap,
            swappiness,
            inferentia,
            efa,
            memory,
            host_volumes,
            efs_volumes,
            use_tmpfs,
            tmpfs_tempdir,
            tmpfs_size,
            tmpfs_path,
            num_parallel,
            ephemeral_storage,
            log_driver,
            log_options,
        )
        return self

    def job_name(self, job_name):
        self.payload["jobName"] = job_name
        return self

    def job_queue(self, job_queue):
        self.payload["jobQueue"] = job_queue
        return self

    def image(self, image):
        self._image = image
        return self

    def task_id(self, task_id):
        self._task_id = task_id
        return self

    def iam_role(self, iam_role):
        self._iam_role = iam_role
        return self

    def execution_role(self, execution_role):
        self._execution_role = execution_role
        return self

    def shared_memory(self, shared_memory):
        self._shared_memory = shared_memory
        return self

    def max_swap(self, max_swap):
        self._max_swap = max_swap
        return self

    def swappiness(self, swappiness):
        self._swappiness = swappiness
        return self

    def inferentia(self, inferentia):
        self._inferentia = inferentia
        return self

    def efa(self, efa):
        self._efa = efa
        return self

    def command(self, command):
        if "command" not in self.payload["containerOverrides"]:
            self.payload["containerOverrides"]["command"] = []
        self.payload["containerOverrides"]["command"].extend(command)
        return self

    def cpu(self, cpu):
        if not (isinstance(cpu, (int, unicode, basestring, float)) and float(cpu) > 0):
            raise BatchJobException(
                "Invalid CPU value ({}); it should be greater than 0".format(cpu)
            )
        if "resourceRequirements" not in self.payload["containerOverrides"]:
            self.payload["containerOverrides"]["resourceRequirements"] = []

        # %g will format the value without .0 if it doesn't have a fractional part
        #
        # While AWS Batch supports fractional values for fargate, it does not
        # seem to like seeing values like 2.0 for non-fargate environments.
        self.payload["containerOverrides"]["resourceRequirements"].append(
            {"value": "%g" % (float(cpu)), "type": "VCPU"}
        )
        return self

    def memory(self, mem):
        if not (isinstance(mem, (int, unicode, basestring, float)) and float(mem) > 0):
            raise BatchJobException(
                "Invalid memory value ({}); it should be greater than 0".format(mem)
            )
        if "resourceRequirements" not in self.payload["containerOverrides"]:
            self.payload["containerOverrides"]["resourceRequirements"] = []
        self.payload["containerOverrides"]["resourceRequirements"].append(
            {"value": str(int(float(mem))), "type": "MEMORY"}
        )
        return self

    def gpu(self, gpu):
        if not (isinstance(gpu, (int, unicode, basestring))):
            raise BatchJobException(
                "invalid gpu value: ({}) (should be 0 or greater)".format(gpu)
            )
        if float(gpu) > 0:
            if "resourceRequirements" not in self.payload["containerOverrides"]:
                self.payload["containerOverrides"]["resourceRequirements"] = []

            # Only integer values are supported but the value passed to us
            # could be a float-converted-to-string
            self.payload["containerOverrides"]["resourceRequirements"].append(
                {"type": "GPU", "value": str(int(float(gpu)))}
            )
        return self

    def environment_variable(self, name, value):
        if value is None:
            return self
        if "environment" not in self.payload["containerOverrides"]:
            self.payload["containerOverrides"]["environment"] = []
        value = str(value)
        if value.startswith("$$.") or value.startswith("$."):
            # Context Object substitution for AWS Step Functions
            # https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html
            self.payload["containerOverrides"]["environment"].append(
                {"name": name, "value.$": value}
            )
        else:
            self.payload["containerOverrides"]["environment"].append(
                {"name": name, "value": value}
            )
        return self

    def timeout_in_secs(self, timeout_in_secs):
        self.payload["timeout"]["attemptDurationSeconds"] = timeout_in_secs
        return self

    def tag(self, key, value):
        self.payload["tags"][key] = str(value)
        return self

    def parameter(self, key, value):
        self.payload["parameters"][key] = str(value)
        return self

    def attempts(self, attempts):
        self.payload["retryStrategy"]["attempts"] = attempts
        return self


class Throttle(object):
    def __init__(self, delta_in_secs=1, num_tries=20):
        self.delta_in_secs = delta_in_secs
        self.num_tries = num_tries
        self._now = None
        self._reset()

    def _reset(self):
        self._tries_left = self.num_tries
        self._wait = self.delta_in_secs

    def __call__(self, func):
        def wrapped(*args, **kwargs):
            now = time.time()
            if self._now is None or (now - self._now > self._wait):
                self._now = now
                try:
                    func(*args, **kwargs)
                    self._reset()
                except TriableException as ex:
                    self._tries_left -= 1
                    if self._tries_left == 0:
                        raise ex.ex
                    self._wait = (self.delta_in_secs * 1.2) ** (
                        self.num_tries - self._tries_left
                    ) + random.randint(0, 3 * self.delta_in_secs)

        return wrapped


class TriableException(Exception):
    def __init__(self, ex):
        self.ex = ex


class RunningJob(object):
    NUM_RETRIES = 8

    def __init__(self, id, client):
        self._id = id
        self._client = client
        self._data = {}

    def __repr__(self):
        return "{}('{}')".format(self.__class__.__name__, self._id)

    def _apply(self, data):
        self._data = data

    @Throttle()
    def _update(self):
        try:
            data = self._client.describe_jobs(jobs=[self._id])
        except self._client.exceptions.ClientError as err:
            code = err.response["ResponseMetadata"]["HTTPStatusCode"]
            if code == 429 or code >= 500:
                raise TriableException(err)
            raise err
        # There have been sporadic reports of empty responses to the
        # batch.describe_jobs API call, which can potentially happen if the
        # batch.submit_job API call is not strongly consistent(¯\_(ツ)_/¯).
        # We add a check here to guard against that. The `update()` call
        # will ensure that we poll `batch.describe_jobs` until we get a
        # satisfactory response at least once throughout the lifecycle of
        # the job.
        if len(data["jobs"]) == 1:
            self._apply(data["jobs"][0])

    def update(self):
        self._update()
        while not self._data:
            self._update()
        return self

    @property
    def id(self):
        return self._id

    @property
    def info(self):
        if not self._data:
            self.update()
        return self._data

    @property
    def job_name(self):
        return self.info["jobName"]

    @property
    def job_queue(self):
        return self.info["jobQueue"]

    @property
    def status(self):
        if not self.is_done:
            self.update()
        return self.info["status"]

    @property
    def status_reason(self):
        return self.info.get("statusReason")

    @property
    def created_at(self):
        return self.info["createdAt"]

    @property
    def stopped_at(self):
        return self.info.get("stoppedAt", 0)

    @property
    def is_done(self):
        if self.stopped_at == 0:
            self.update()
        return self.stopped_at > 0

    @property
    def is_running(self):
        return self.status == "RUNNING"

    @property
    def is_successful(self):
        return self.status == "SUCCEEDED"

    @property
    def is_crashed(self):
        # TODO: Check statusmessage to find if the job crashed instead of failing
        return self.status == "FAILED"

    @property
    def reason(self):
        if "container" in self.info:
            # single-node job
            return self.info["container"].get("reason")
        else:
            # multinode
            return self.info["statusReason"]

    @property
    def status_code(self):
        if not self.is_done:
            self.update()
        if "container" in self.info:
            return self.info["container"].get("exitCode")
        else:
            # multinode
            return self.info["attempts"][-1]["container"].get("exitCode")

    def kill(self):
        if not self.is_done:
            self._client.terminate_job(
                jobId=self._id, reason="Metaflow initiated job termination."
            )
        return self.update()
