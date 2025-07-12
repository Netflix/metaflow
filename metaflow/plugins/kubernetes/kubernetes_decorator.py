import json
import os
import platform
import sys
import time

from metaflow import current
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metadata_provider import MetaDatum
from metaflow.metadata_provider.util import sync_local_metadata_to_datastore
from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
    FEAT_ALWAYS_UPLOAD_CODE_PACKAGE,
    KUBERNETES_CONTAINER_IMAGE,
    KUBERNETES_CONTAINER_REGISTRY,
    KUBERNETES_CPU,
    KUBERNETES_DISK,
    KUBERNETES_FETCH_EC2_METADATA,
    KUBERNETES_GPU_VENDOR,
    KUBERNETES_IMAGE_PULL_POLICY,
    KUBERNETES_IMAGE_PULL_SECRETS,
    KUBERNETES_MEMORY,
    KUBERNETES_LABELS,
    KUBERNETES_ANNOTATIONS,
    KUBERNETES_NAMESPACE,
    KUBERNETES_NODE_SELECTOR,
    KUBERNETES_PERSISTENT_VOLUME_CLAIMS,
    KUBERNETES_PORT,
    KUBERNETES_SERVICE_ACCOUNT,
    KUBERNETES_SHARED_MEMORY,
    KUBERNETES_TOLERATIONS,
    KUBERNETES_QOS,
    KUBERNETES_CONDA_ARCH,
)
from metaflow.plugins.resources_decorator import ResourcesDecorator
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.sidecar import Sidecar
from metaflow.unbounded_foreach import UBF_CONTROL

from ..aws.aws_utils import get_docker_registry, get_ec2_instance_metadata
from .kubernetes import KubernetesException
from .kube_utils import validate_kube_labels, parse_kube_keyvalue_list

try:
    unicode
except NameError:
    unicode = str
    basestring = str

SUPPORTED_KUBERNETES_QOS_CLASSES = ["Guaranteed", "Burstable"]


class KubernetesDecorator(StepDecorator):
    """
    Specifies that this step should execute on Kubernetes.

    Parameters
    ----------
    cpu : int, default 1
        Number of CPUs required for this step. If `@resources` is
        also present, the maximum value from all decorators is used.
    memory : int, default 4096
        Memory size (in MB) required for this step. If
        `@resources` is also present, the maximum value from all decorators is
        used.
    disk : int, default 10240
        Disk size (in MB) required for this step. If
        `@resources` is also present, the maximum value from all decorators is
        used.
    image : str, optional, default None
        Docker image to use when launching on Kubernetes. If not specified, and
        METAFLOW_KUBERNETES_CONTAINER_IMAGE is specified, that image is used. If
        not, a default Docker image mapping to the current version of Python is used.
    image_pull_policy: str, default KUBERNETES_IMAGE_PULL_POLICY
        If given, the imagePullPolicy to be applied to the Docker image of the step.
    image_pull_secrets: List[str], default []
        The default is extracted from METAFLOW_KUBERNETES_IMAGE_PULL_SECRETS.
        Kubernetes image pull secrets to use when pulling container images
        in Kubernetes.
    service_account : str, default METAFLOW_KUBERNETES_SERVICE_ACCOUNT
        Kubernetes service account to use when launching pod in Kubernetes.
    secrets : List[str], optional, default None
        Kubernetes secrets to use when launching pod in Kubernetes. These
        secrets are in addition to the ones defined in `METAFLOW_KUBERNETES_SECRETS`
        in Metaflow configuration.
    node_selector: Union[Dict[str,str], str], optional, default None
        Kubernetes node selector(s) to apply to the pod running the task.
        Can be passed in as a comma separated string of values e.g.
        'kubernetes.io/os=linux,kubernetes.io/arch=amd64' or as a dictionary
        {'kubernetes.io/os': 'linux', 'kubernetes.io/arch': 'amd64'}
    namespace : str, default METAFLOW_KUBERNETES_NAMESPACE
        Kubernetes namespace to use when launching pod in Kubernetes.
    gpu : int, optional, default None
        Number of GPUs required for this step. A value of zero implies that
        the scheduled node should not have GPUs.
    gpu_vendor : str, default KUBERNETES_GPU_VENDOR
        The vendor of the GPUs to be used for this step.
    tolerations : List[str], default []
        The default is extracted from METAFLOW_KUBERNETES_TOLERATIONS.
        Kubernetes tolerations to use when launching pod in Kubernetes.
    labels: Dict[str, str], default: METAFLOW_KUBERNETES_LABELS
        Kubernetes labels to use when launching pod in Kubernetes.
    annotations: Dict[str, str], default: METAFLOW_KUBERNETES_ANNOTATIONS
        Kubernetes annotations to use when launching pod in Kubernetes.
    use_tmpfs : bool, default False
        This enables an explicit tmpfs mount for this step.
    tmpfs_tempdir : bool, default True
        sets METAFLOW_TEMPDIR to tmpfs_path if set for this step.
    tmpfs_size : int, optional, default: None
        The value for the size (in MiB) of the tmpfs mount for this step.
        This parameter maps to the `--tmpfs` option in Docker. Defaults to 50% of the
        memory allocated for this step.
    tmpfs_path : str, optional, default /metaflow_temp
        Path to tmpfs mount for this step.
    persistent_volume_claims : Dict[str, str], optional, default None
        A map (dictionary) of persistent volumes to be mounted to the pod for this step. The map is from persistent
        volumes to the path to which the volume is to be mounted, e.g., `{'pvc-name': '/path/to/mount/on'}`.
    shared_memory: int, optional
        Shared memory size (in MiB) required for this step
    port: int, optional
        Port number to specify in the Kubernetes job object
    compute_pool : str, optional, default None
        Compute pool to be used for for this step.
        If not specified, any accessible compute pool within the perimeter is used.
    hostname_resolution_timeout: int, default 10 * 60
        Timeout in seconds for the workers tasks in the gang scheduled cluster to resolve the hostname of control task.
        Only applicable when @parallel is used.
    qos: str, default: Burstable
        Quality of Service class to assign to the pod. Supported values are: Guaranteed, Burstable, BestEffort

    security_context: Dict[str, Any], optional, default None
        Container security context. Applies to the task container. Allows the following keys:
        - privileged: bool, optional, default None
        - allow_privilege_escalation: bool, optional, default None
        - run_as_user: int, optional, default None
        - run_as_group: int, optional, default None
        - run_as_non_root: bool, optional, default None
    """

    name = "kubernetes"
    defaults = {
        "cpu": "1",
        "memory": "4096",
        "disk": "10240",
        "image": None,
        "image_pull_policy": None,
        "image_pull_secrets": None,  # e.g., ["regcred"]
        "service_account": None,
        "secrets": None,  # e.g., mysecret
        "node_selector": None,  # e.g., kubernetes.io/os=linux
        "namespace": None,
        "gpu": None,  # value of 0 implies that the scheduled node should not have GPUs
        "gpu_vendor": None,
        "tolerations": None,  # e.g., [{"key": "arch", "operator": "Equal", "value": "amd"},
        #                              {"key": "foo", "operator": "Equal", "value": "bar"}]
        "labels": None,  # e.g. {"test-label": "value", "another-label":"value2"}
        "annotations": None,  # e.g. {"note": "value", "another-note": "value2"}
        "use_tmpfs": None,
        "tmpfs_tempdir": True,
        "tmpfs_size": None,
        "tmpfs_path": "/metaflow_temp",
        "persistent_volume_claims": None,  # e.g., {"pvc-name": "/mnt/vol", "another-pvc": "/mnt/vol2"}
        "shared_memory": None,
        "port": None,
        "compute_pool": None,
        "executable": None,
        "hostname_resolution_timeout": 10 * 60,
        "qos": KUBERNETES_QOS,
        "security_context": None,
    }
    package_metadata = None
    package_url = None
    package_sha = None
    run_time_limit = None

    # Conda environment support
    supports_conda_environment = True
    target_platform = KUBERNETES_CONDA_ARCH or "linux-64"

    def init(self):
        if not self.attributes["namespace"]:
            self.attributes["namespace"] = KUBERNETES_NAMESPACE
        if not self.attributes["service_account"]:
            self.attributes["service_account"] = KUBERNETES_SERVICE_ACCOUNT
        if not self.attributes["gpu_vendor"]:
            self.attributes["gpu_vendor"] = KUBERNETES_GPU_VENDOR
        if not self.attributes["node_selector"] and KUBERNETES_NODE_SELECTOR:
            self.attributes["node_selector"] = KUBERNETES_NODE_SELECTOR
        if not self.attributes["tolerations"] and KUBERNETES_TOLERATIONS:
            self.attributes["tolerations"] = json.loads(KUBERNETES_TOLERATIONS)
        if (
            not self.attributes["persistent_volume_claims"]
            and KUBERNETES_PERSISTENT_VOLUME_CLAIMS
        ):
            self.attributes["persistent_volume_claims"] = json.loads(
                KUBERNETES_PERSISTENT_VOLUME_CLAIMS
            )
        if not self.attributes["image_pull_policy"] and KUBERNETES_IMAGE_PULL_POLICY:
            self.attributes["image_pull_policy"] = KUBERNETES_IMAGE_PULL_POLICY
        if not self.attributes["image_pull_secrets"] and KUBERNETES_IMAGE_PULL_SECRETS:
            self.attributes["image_pull_secrets"] = json.loads(
                KUBERNETES_IMAGE_PULL_SECRETS
            )

        if isinstance(self.attributes["node_selector"], str):
            self.attributes["node_selector"] = parse_kube_keyvalue_list(
                self.attributes["node_selector"].split(",")
            )
        if self.attributes["compute_pool"]:
            if self.attributes["node_selector"] is None:
                self.attributes["node_selector"] = {}
            self.attributes["node_selector"].update(
                {"outerbounds.co/compute-pool": self.attributes["compute_pool"]}
            )

        if self.attributes["tolerations"]:
            try:
                from kubernetes.client import V1Toleration

                for toleration in self.attributes["tolerations"]:
                    try:
                        invalid_keys = [
                            k
                            for k in toleration.keys()
                            if k not in V1Toleration.attribute_map.keys()
                        ]
                        if len(invalid_keys) > 0:
                            raise KubernetesException(
                                "Tolerations parameter contains invalid keys: %s"
                                % invalid_keys
                            )
                    except AttributeError:
                        raise KubernetesException(
                            "Unable to parse tolerations: %s"
                            % self.attributes["tolerations"]
                        )
            except (NameError, ImportError):
                pass

        # parse the CPU, memory, disk, values from the KUBERNETES_ environment variable (you would need to export the METAFLOW_KUBERNETES_CPU, METAFLOW_KUBERNETES_MEMORY and/or METAFLOW_KUBERNTES_DISK environment variable with the desired values before running the flow)
        # find the values from the environment variables, then validate if the values are still the default ones, if so, then replace them with the values from the environment variables (otherwise, keep the values from the decorator)
        if self.attributes["cpu"] == self.defaults["cpu"] and KUBERNETES_CPU:
            self.attributes["cpu"] = KUBERNETES_CPU
        if self.attributes["memory"] == self.defaults["memory"] and KUBERNETES_MEMORY:
            self.attributes["memory"] = KUBERNETES_MEMORY
        if self.attributes["disk"] == self.defaults["disk"] and KUBERNETES_DISK:
            self.attributes["disk"] = KUBERNETES_DISK
        # Label source precedence (decreasing):
        # - System labels (set outside of decorator)
        # - Decorator labels: @kubernetes(labels={})
        # - Environment variable labels: METAFLOW_KUBERNETES_LABELS=
        deco_labels = {}
        if self.attributes["labels"] is not None:
            deco_labels = self.attributes["labels"]

        env_labels = {}
        if KUBERNETES_LABELS:
            env_labels = parse_kube_keyvalue_list(KUBERNETES_LABELS.split(","), False)

        self.attributes["labels"] = {**env_labels, **deco_labels}

        # Annotations
        # annotation precedence (decreasing):
        # - System annotations (set outside of decorator)
        # - Decorator annotations: @kubernetes(annotations={})
        # - Environment annotations: METAFLOW_KUBERNETES_ANNOTATIONS=
        deco_annotations = {}
        if self.attributes["annotations"] is not None:
            deco_annotations = self.attributes["annotations"]

        env_annotations = {}
        if KUBERNETES_ANNOTATIONS:
            env_annotations = parse_kube_keyvalue_list(
                KUBERNETES_ANNOTATIONS.split(","), False
            )

        self.attributes["annotations"] = {**env_annotations, **deco_annotations}

        # If no docker image is explicitly specified, impute a default image.
        if not self.attributes["image"]:
            # If metaflow-config specifies a docker image, just use that.
            if KUBERNETES_CONTAINER_IMAGE:
                self.attributes["image"] = KUBERNETES_CONTAINER_IMAGE
            # If metaflow-config doesn't specify a docker image, assign a
            # default docker image.
            else:
                # Default to vanilla Python image corresponding to major.minor
                # version of the Python interpreter launching the flow.
                self.attributes["image"] = "python:%s.%s" % (
                    platform.python_version_tuple()[0],
                    platform.python_version_tuple()[1],
                )
        # Assign docker registry URL for the image.
        if not get_docker_registry(self.attributes["image"]):
            if KUBERNETES_CONTAINER_REGISTRY:
                self.attributes["image"] = "%s/%s" % (
                    KUBERNETES_CONTAINER_REGISTRY.rstrip("/"),
                    self.attributes["image"],
                )
        # Check if TmpFS is enabled and set default tmpfs_size if missing.
        if self.attributes["use_tmpfs"] or (
            self.attributes["tmpfs_size"] and not self.attributes["use_tmpfs"]
        ):
            if not self.attributes["tmpfs_size"]:
                # default tmpfs behavior - https://man7.org/linux/man-pages/man5/tmpfs.5.html
                self.attributes["tmpfs_size"] = int(self.attributes["memory"]) // 2
        if not self.attributes["shared_memory"]:
            self.attributes["shared_memory"] = KUBERNETES_SHARED_MEMORY
        if not self.attributes["port"]:
            self.attributes["port"] = KUBERNETES_PORT

    # Refer https://github.com/Netflix/metaflow/blob/master/docs/lifecycle.png
    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # Executing Kubernetes jobs requires a non-local datastore.
        if flow_datastore.TYPE not in ("s3", "azure", "gs"):
            raise KubernetesException(
                "The *@kubernetes* decorator requires --datastore=s3 or --datastore=azure or --datastore=gs at the moment."
            )

        # Set internal state.
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        if (
            self.attributes["qos"] is not None
            # case insensitive matching.
            and self.attributes["qos"].lower()
            not in [c.lower() for c in SUPPORTED_KUBERNETES_QOS_CLASSES]
        ):
            raise MetaflowException(
                "*%s* is not a valid Kubernetes QoS class. Choose one of the following: %s"
                % (self.attributes["qos"], ", ".join(SUPPORTED_KUBERNETES_QOS_CLASSES))
            )

        if any([deco.name == "batch" for deco in decos]):
            raise MetaflowException(
                "Step *{step}* is marked for execution both on AWS Batch and "
                "Kubernetes. Please use one or the other.".format(step=step)
            )

        if any([deco.name == "parallel" for deco in decos]) and any(
            [deco.name == "catch" for deco in decos]
        ):
            raise MetaflowException(
                "Step *{step}* contains a @parallel decorator "
                "with the @catch decorator. @catch is not supported with @parallel on Kubernetes.".format(
                    step=step
                )
            )

        # Set run time limit for the Kubernetes job.
        self.run_time_limit = get_run_time_limit_for_task(decos)
        if self.run_time_limit < 60:
            raise KubernetesException(
                "The timeout for step *{step}* should be at least 60 seconds for "
                "execution on Kubernetes.".format(step=step)
            )

        for deco in decos:
            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    # If GPU count is specified, explicitly set it in self.attributes.
                    if k == "gpu" and v != None:
                        self.attributes["gpu"] = v

                    if k in self.attributes:
                        if self.defaults[k] is None:
                            # skip if expected value isn't an int/float
                            continue
                        # We use the larger of @resources and @batch attributes
                        # TODO: Fix https://github.com/Netflix/metaflow/issues/467
                        my_val = self.attributes.get(k)
                        if not (my_val is None and v is None):
                            self.attributes[k] = str(
                                max(float(my_val or 0), float(v or 0))
                            )

        # Check GPU vendor.
        if self.attributes["gpu_vendor"].lower() not in ("amd", "nvidia"):
            raise KubernetesException(
                "GPU vendor *{}* for step *{step}* is not currently supported.".format(
                    self.attributes["gpu_vendor"], step=step
                )
            )

        # CPU, Disk, and Memory values should be greater than 0.
        for attr in ["cpu", "disk", "memory"]:
            if not (
                isinstance(self.attributes[attr], (int, unicode, basestring, float))
                and float(self.attributes[attr]) > 0
            ):
                raise KubernetesException(
                    "Invalid {} value *{}* for step *{step}*; it should be greater than 0".format(
                        attr, self.attributes[attr], step=step
                    )
                )

        if self.attributes["gpu"] is not None and not (
            isinstance(self.attributes["gpu"], (int, unicode, basestring))
            and float(self.attributes["gpu"]).is_integer()
        ):
            raise KubernetesException(
                "Invalid GPU value *{}* for step *{step}*; it should be an integer".format(
                    self.attributes["gpu"], step=step
                )
            )

        if self.attributes["tmpfs_size"]:
            if not (
                isinstance(self.attributes["tmpfs_size"], (int, unicode, basestring))
                and int(self.attributes["tmpfs_size"]) > 0
            ):
                raise KubernetesException(
                    "Invalid tmpfs_size value: *{size}* for step *{step}* (should be an integer greater than 0)".format(
                        size=self.attributes["tmpfs_size"], step=step
                    )
                )

        if self.attributes["shared_memory"]:
            if not (
                isinstance(self.attributes["shared_memory"], int)
                and int(self.attributes["shared_memory"]) > 0
            ):
                raise KubernetesException(
                    "Invalid shared_memory value: *{size}* for step *{step}* (should be an integer greater than 0)".format(
                        size=self.attributes["shared_memory"], step=step
                    )
                )

        validate_kube_labels(self.attributes["labels"])
        # TODO: add validation to annotations as well?

    def package_init(self, flow, step_name, environment):
        try:
            # Kubernetes is a soft dependency.
            from kubernetes import client, config
        except (NameError, ImportError):
            raise KubernetesException(
                "Could not import module 'kubernetes'.\n\nInstall Kubernetes "
                "Python package (https://pypi.org/project/kubernetes/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install kubernetes\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )

    def runtime_init(self, flow, graph, package, run_id):
        # Set some more internal state.
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        # To execute the Kubernetes job, the job container needs to have
        # access to the code package. We store the package in the datastore
        # which the pod is able to download as part of it's entrypoint.
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if retry_count <= max_user_code_retries:
            # After all attempts to run the user code have failed, we don't need
            # to execute on Kubernetes anymore. We can execute possible fallback
            # code locally.
            cli_args.commands = ["kubernetes", "step"]
            cli_args.command_args.append(self.package_metadata)
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)

            # skip certain keys as CLI arguments
            _skip_keys = ["compute_pool", "hostname_resolution_timeout"]
            # --namespace is used to specify Metaflow namespace (a different
            # concept from k8s namespace).
            for k, v in self.attributes.items():
                if k in _skip_keys:
                    continue
                if k == "namespace":
                    cli_args.command_options["k8s_namespace"] = v
                elif k in {"node_selector"} and v:
                    cli_args.command_options[k] = [
                        "=".join([key, str(val)]) if val else key
                        for key, val in v.items()
                    ]
                elif k in [
                    "image_pull_secrets",
                    "tolerations",
                    "persistent_volume_claims",
                    "labels",
                    "annotations",
                    "security_context",
                ]:
                    cli_args.command_options[k] = json.dumps(v)
                else:
                    cli_args.command_options[k] = v
            cli_args.command_options["run-time-limit"] = self.run_time_limit
            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        self.metadata = metadata
        self.task_datastore = task_datastore

        # current.tempdir reflects the value of METAFLOW_TEMPDIR (the current working
        # directory by default), or the value of tmpfs_path if tmpfs_tempdir=False.
        if not self.attributes["tmpfs_tempdir"]:
            current._update_env({"tempdir": self.attributes["tmpfs_path"]})

        # task_pre_step may run locally if fallback is activated for @catch
        # decorator. In that scenario, we skip collecting Kubernetes execution
        # metadata. A rudimentary way to detect non-local execution is to
        # check for the existence of METAFLOW_KUBERNETES_WORKLOAD environment
        # variable.

        meta = {}
        if "METAFLOW_KUBERNETES_WORKLOAD" in os.environ:
            meta["kubernetes-pod-name"] = os.environ["METAFLOW_KUBERNETES_POD_NAME"]
            meta["kubernetes-pod-namespace"] = os.environ[
                "METAFLOW_KUBERNETES_POD_NAMESPACE"
            ]
            meta["kubernetes-pod-id"] = os.environ["METAFLOW_KUBERNETES_POD_ID"]
            meta["kubernetes-pod-service-account-name"] = os.environ[
                "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME"
            ]
            meta["kubernetes-node-ip"] = os.environ["METAFLOW_KUBERNETES_NODE_IP"]

            meta["kubernetes-jobset-name"] = os.environ.get(
                "METAFLOW_KUBERNETES_JOBSET_NAME"
            )

            # TODO (savin): Introduce equivalent support for Microsoft Azure and
            #               Google Cloud Platform
            # TODO: Introduce a way to detect Cloud Provider, so unnecessary requests
            # (and delays) can be avoided by not having to try out all providers.
            if KUBERNETES_FETCH_EC2_METADATA:
                instance_meta = get_ec2_instance_metadata()
                meta.update(instance_meta)

            # Unfortunately, there doesn't seem to be any straight forward way right
            # now to attach the Batch/v1 name - While we can rely on a hacky approach
            # given we know that the pod name is simply a unique suffix with a hyphen
            # delimiter to the Batch/v1 name - this approach will fail if the Batch/v1
            # name is closer to 63 chars where the pod name will truncate the Batch/v1
            # name.
            # if "ARGO_WORKFLOW_NAME" not in os.environ:
            #     meta["kubernetes-job-name"] = os.environ[
            #         "METAFLOW_KUBERNETES_POD_NAME"
            #     ].rpartition("-")[0]

            # Start MFLog sidecar to collect task logs.
            self._save_logs_sidecar = Sidecar("save_logs_periodically")
            self._save_logs_sidecar.start()

            # Start spot termination monitor sidecar.
            current._update_env(
                {"spot_termination_notice": "/tmp/spot_termination_notice"}
            )
            self._spot_monitor_sidecar = Sidecar("spot_termination_monitor")
            self._spot_monitor_sidecar.start()

        num_parallel = None
        if hasattr(flow, "_parallel_ubf_iter"):
            num_parallel = flow._parallel_ubf_iter.num_parallel

        if num_parallel and num_parallel > 1:
            _setup_multinode_environment(
                ubf_context, self.attributes["hostname_resolution_timeout"]
            )
            # current.parallel.node_index will be correctly available over here.
            meta.update({"parallel-node-index": current.parallel.node_index})
            if ubf_context == UBF_CONTROL:
                flow._control_mapper_tasks = [
                    "{}/{}/{}".format(run_id, step_name, task_id)
                    for task_id in [task_id]
                    + [
                        "%s-worker-%d" % (task_id, idx)
                        for idx in range(num_parallel - 1)
                    ]
                ]
                flow._control_task_is_mapper_zero = True

        if len(meta) > 0:
            entries = [
                MetaDatum(
                    field=k,
                    value=v,
                    type=k,
                    tags=["attempt_id:{0}".format(retry_count)],
                )
                for k, v in meta.items()
                if v is not None
            ]
            # Register book-keeping metadata for debugging.
            metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        # task_finished may run locally if fallback is activated for @catch
        # decorator.
        if "METAFLOW_KUBERNETES_WORKLOAD" in os.environ:
            # If `local` metadata is configured, we would need to copy task
            # execution metadata from the AWS Batch container to user's
            # local file system after the user code has finished execution.
            # This happens via datastore as a communication bridge.

            # TODO:  There is no guarantee that task_pre_step executes before
            #        task_finished is invoked.
            # For now we guard against the missing metadata object in this case.
            if hasattr(self, "metadata") and self.metadata.TYPE == "local":
                # Note that the datastore is *always* Amazon S3 (see
                # runtime_task_created function).
                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )

        try:
            self._save_logs_sidecar.terminate()
            self._spot_monitor_sidecar.terminate()
        except:
            # Best effort kill
            pass

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            if not FEAT_ALWAYS_UPLOAD_CODE_PACKAGE:
                cls.package_url, cls.package_sha = flow_datastore.save_data(
                    [package.blob], len_hint=1
                )[0]
                cls.package_metadata = package.package_metadata
            else:
                # Blocks until the package is uploaded
                cls.package_url = package.package_url()
                cls.package_sha = package.package_sha()
                cls.package_metadata = package.package_metadata


# TODO: Unify this method with the multi-node setup in @batch
def _setup_multinode_environment(ubf_context, hostname_resolution_timeout):
    import socket

    def _wait_for_hostname_resolution(max_wait_timeout=10 * 60):
        """
        keep trying to resolve the hostname of the control task until the hostname is resolved
        or the max_wait_timeout is reached. This is a workaround for the issue where the control
        task is not scheduled before the worker task and the worker task fails because it cannot
        resolve the hostname of the control task.
        """
        start_time = time.time()
        while True:
            try:
                return socket.gethostbyname(os.environ["MF_MASTER_ADDR"])
            except socket.gaierror:
                if time.time() - start_time > max_wait_timeout:
                    raise MetaflowException(
                        "Failed to get host by name for MF_MASTER_ADDR after waiting for {} seconds.".format(
                            max_wait_timeout
                        )
                    )
                time.sleep(1)

    try:
        # Even if Kubernetes may deploy control pods before worker pods, there is always a
        # possibility that the worker pods may start before the control. In the case that this happens,
        # the worker pods will not be able to resolve the control pod's IP address and this will cause
        # the worker pods to fail. So if the worker pods are requesting a hostname resolution, we will
        # make it wait for the name to be resolved within a reasonable timeout period.
        if ubf_context != UBF_CONTROL:
            os.environ["MF_PARALLEL_MAIN_IP"] = _wait_for_hostname_resolution(
                hostname_resolution_timeout
            )
        else:
            os.environ["MF_PARALLEL_MAIN_IP"] = socket.gethostbyname(
                os.environ["MF_MASTER_ADDR"]
            )

        os.environ["MF_PARALLEL_NUM_NODES"] = os.environ["MF_WORLD_SIZE"]
        os.environ["MF_PARALLEL_NODE_INDEX"] = (
            str(0)
            if "MF_CONTROL_INDEX" in os.environ
            else str(int(os.environ["MF_WORKER_REPLICA_INDEX"]) + 1)
        )
    except KeyError as e:
        raise MetaflowException("Environment variable {} is missing.".format(e))
    except socket.gaierror:
        raise MetaflowException("Failed to get host by name for MF_MASTER_ADDR.")
    except ValueError:
        raise MetaflowException("Invalid value for MF_WORKER_REPLICA_INDEX.")
