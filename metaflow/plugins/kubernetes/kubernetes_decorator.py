import json
import os
import platform
import sys

from metaflow import current
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metadata import MetaDatum
from metaflow.metadata.util import sync_local_metadata_to_datastore
from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
    KUBERNETES_CONTAINER_IMAGE,
    KUBERNETES_CONTAINER_REGISTRY,
    KUBERNETES_FETCH_EC2_METADATA,
    KUBERNETES_IMAGE_PULL_POLICY,
    KUBERNETES_GPU_VENDOR,
    KUBERNETES_NAMESPACE,
    KUBERNETES_NODE_SELECTOR,
    KUBERNETES_PERSISTENT_VOLUME_CLAIMS,
    KUBERNETES_TOLERATIONS,
    KUBERNETES_SERVICE_ACCOUNT,
)
from metaflow.plugins.resources_decorator import ResourcesDecorator
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.sidecar import Sidecar

from ..aws.aws_utils import get_docker_registry, get_ec2_instance_metadata
from .kubernetes import KubernetesException, parse_kube_keyvalue_list

try:
    unicode
except NameError:
    unicode = str
    basestring = str


class KubernetesDecorator(StepDecorator):
    """
    Specifies that this step should execute on Kubernetes.

    Parameters
    ----------
    cpu : int, default: 1
        Number of CPUs required for this step. If `@resources` is
        also present, the maximum value from all decorators is used.
    memory : int, default: 4096
        Memory size (in MB) required for this step. If
        `@resources` is also present, the maximum value from all decorators is
        used.
    disk : int, default: 10240
        Disk size (in MB) required for this step. If
        `@resources` is also present, the maximum value from all decorators is
        used.
    image : str, optional
        Docker image to use when launching on Kubernetes. If not specified, and
        METAFLOW_KUBERNETES_CONTAINER_IMAGE is specified, that image is used. If
        not, a default Docker image mapping to the current version of Python is used.
    image_pull_policy: str, default: KUBERNETES_IMAGE_PULL_POLICY
        If given, the imagePullPolicy to be applied to the Docker image of the step.
    service_account : str, default: METAFLOW_KUBERNETES_SERVICE_ACCOUNT
        Kubernetes service account to use when launching pod in Kubernetes.
    secrets : List[str], optional
        Kubernetes secrets to use when launching pod in Kubernetes. These
        secrets are in addition to the ones defined in `METAFLOW_KUBERNETES_SECRETS`
        in Metaflow configuration.
    namespace : str, default: METAFLOW_KUBERNETES_NAMESPACE
        Kubernetes namespace to use when launching pod in Kubernetes.
    gpu: int, optional
        Number of GPUs required for this step. A value of zero implies that
        the scheduled node should not have GPUs.
    gpu_vendor: str, default: KUBERNETES_GPU_VENDOR
        The vendor of the GPUs to be used for this step.
    tolerations : List[str], default: METAFLOW_KUBERNETES_TOLERATIONS
        Kubernetes tolerations to use when launching pod in Kubernetes.
    use_tmpfs: bool, default: False
        This enables an explicit tmpfs mount for this step.
    tmpfs_tempdir: bool, default: True
        sets METAFLOW_TEMPDIR to tmpfs_path if set for this step.
    tmpfs_size: int, optional
        The value for the size (in MiB) of the tmpfs mount for this step.
        This parameter maps to the `--tmpfs` option in Docker. Defaults to 50% of the
        memory allocated for this step.
    tmpfs_path: string, optional
        Path to tmpfs mount for this step. Defaults to /metaflow_temp.
    persistent_volume_claims: Dict[str, str], optional
        A map (dictionary) of persistent volumes to be mounted to the pod for this step. The map is from persistent
        volumes to the path to which the volume is to be mounted, e.g., `{'pvc-name': '/path/to/mount/on'}`.
    """

    name = "kubernetes"
    defaults = {
        "cpu": "1",
        "memory": "4096",
        "disk": "10240",
        "image": None,
        "image_pull_policy": None,
        "service_account": None,
        "secrets": None,  # e.g., mysecret
        "node_selector": None,  # e.g., kubernetes.io/os=linux
        "namespace": None,
        "gpu": None,  # value of 0 implies that the scheduled node should not have GPUs
        "gpu_vendor": None,
        "tolerations": None,  # e.g., [{"key": "arch", "operator": "Equal", "value": "amd"},
        #                              {"key": "foo", "operator": "Equal", "value": "bar"}]
        "use_tmpfs": None,
        "tmpfs_tempdir": True,
        "tmpfs_size": None,
        "tmpfs_path": "/metaflow_temp",
        "persistent_volume_claims": None,  # e.g., {"pvc-name": "/mnt/vol", "another-pvc": "/mnt/vol2"}
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(KubernetesDecorator, self).__init__(attributes, statically_defined)

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

        if isinstance(self.attributes["node_selector"], str):
            self.attributes["node_selector"] = parse_kube_keyvalue_list(
                self.attributes["node_selector"].split(",")
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

        if any([deco.name == "batch" for deco in decos]):
            raise MetaflowException(
                "Step *{step}* is marked for execution both on AWS Batch and "
                "Kubernetes. Please use one or the other.".format(step=step)
            )

        for deco in decos:
            if getattr(deco, "IS_PARALLEL", False):
                raise KubernetesException(
                    "@kubernetes does not support parallel execution currently."
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
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)

            # --namespace is used to specify Metaflow namespace (a different
            # concept from k8s namespace).
            for k, v in self.attributes.items():
                if k == "namespace":
                    cli_args.command_options["k8s_namespace"] = v
                elif k in {"node_selector"} and v:
                    cli_args.command_options[k] = [
                        "=".join([key, str(val)]) if val else key
                        for key, val in v.items()
                    ]
                elif k in ["tolerations", "persistent_volume_claims"]:
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

        if "METAFLOW_KUBERNETES_WORKLOAD" in os.environ:
            meta = {}
            meta["kubernetes-pod-name"] = os.environ["METAFLOW_KUBERNETES_POD_NAME"]
            meta["kubernetes-pod-namespace"] = os.environ[
                "METAFLOW_KUBERNETES_POD_NAMESPACE"
            ]
            meta["kubernetes-pod-id"] = os.environ["METAFLOW_KUBERNETES_POD_ID"]
            meta["kubernetes-pod-service-account-name"] = os.environ[
                "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME"
            ]
            meta["kubernetes-node-ip"] = os.environ["METAFLOW_KUBERNETES_NODE_IP"]

            # TODO (savin): Introduce equivalent support for Microsoft Azure and
            #               Google Cloud Platform
            # TODO: Introduce a way to detect Cloud Provider, so unnecessary requests (and delays)
            # can be avoided by not having to try out all providers.
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

            entries = [
                MetaDatum(field=k, value=v, type=k, tags=[])
                for k, v in meta.items()
                if v is not None
            ]
            # Register book-keeping metadata for debugging.
            metadata.register_metadata(run_id, step_name, task_id, entries)

            # Start MFLog sidecar to collect task logs.
            self._save_logs_sidecar = Sidecar("save_logs_periodically")
            self._save_logs_sidecar.start()

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

            # TODO:  There is no guarantee that task_prestep executes before
            #        task_finished is invoked. That will result in AttributeError:
            #        'KubernetesDecorator' object has no attribute 'metadata' error.
            if self.metadata.TYPE == "local":
                # Note that the datastore is *always* Amazon S3 (see
                # runtime_task_created function).
                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )

        try:
            self._save_logs_sidecar.terminate()
        except:
            # Best effort kill
            pass

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]
