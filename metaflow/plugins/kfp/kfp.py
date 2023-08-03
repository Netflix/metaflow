import base64
import inspect
import json
import marshal
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Text, Tuple, Union
import yaml

import kfp
from kfp import dsl
from kfp.dsl import ContainerOp, PipelineConf, PipelineVolume, ResourceOp
from kfp.dsl._container_op import _get_cpu_number, _get_resource_number
from kfp.dsl._pipeline_param import sanitize_k8s_name
from kubernetes.client import (
    V1Affinity,
    V1EnvVar,
    V1EnvVarSource,
    V1EmptyDirVolumeSource,
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1ObjectFieldSelector,
    V1ObjectMeta,
    V1OwnerReference,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1ResourceRequirements,
    V1Toleration,
    V1Volume,
)

from metaflow.decorators import FlowDecorator
from metaflow.metaflow_config import (
    DATASTORE_SYSROOT_S3,
    KFP_TTL_SECONDS_AFTER_FINISHED,
    KUBERNETES_SERVICE_ACCOUNT,
    METAFLOW_USER,
    ZILLOW_INDIVIDUAL_NAMESPACE,
    ZILLOW_ZODIAC_SERVICE,
    ZILLOW_ZODIAC_TEAM,
    from_conf,
)
from metaflow.plugins import EnvironmentDecorator, KfpInternalDecorator
from metaflow.plugins.kfp.kfp_constants import (
    S3_SENSOR_RETRY_COUNT,
    EXIT_HANDLER_RETRY_COUNT,
)
from metaflow.plugins.kfp.kfp_decorator import KfpException
from .accelerator_decorator import AcceleratorDecorator
from .argo_client import ArgoClient
from .interruptible_decorator import interruptibleDecorator
from .kfp_foreach_splits import graph_to_task_ids
from ..aws.batch.batch_decorator import BatchDecorator
from ..aws.step_functions.schedule_decorator import ScheduleDecorator
from ...graph import DAGNode
from ...metaflow_environment import MetaflowEnvironment
from ...plugins.resources_decorator import ResourcesDecorator

# TODO: @schedule
UNSUPPORTED_DECORATORS = (
    BatchDecorator,
    ScheduleDecorator,
)


@dataclass
class FlowVariables:
    flow_name: str
    environment: str
    event_logger: str
    monitor: str
    namespace: str
    tags: List[str]
    sys_tags: List[str]
    package_commands: str


@dataclass
class StepVariables:
    step_name: str
    volume_dir: str
    is_split_index: bool
    task_id: str
    user_code_retries: int


METAFLOW_RUN_ID = "argo-{{workflow.name}}"
FLOW_PARAMETERS_JSON = "{{workflow.parameters}}"


class KfpComponent(object):
    def __init__(
        self,
        step_name: str,
        resource_requirements: Dict[str, str],
        kfp_decorator: KfpInternalDecorator,
        accelerator_decorator: AcceleratorDecorator,
        interruptible_decorator: interruptibleDecorator,
        environment_decorator: EnvironmentDecorator,
        total_retries: int,
    ):
        self.step_name = step_name
        self.resource_requirements = resource_requirements
        self.kfp_decorator = kfp_decorator
        self.accelerator_decorator = accelerator_decorator
        self.interruptible_decorator = interruptible_decorator
        self.environment_decorator = environment_decorator
        self.total_retries = total_retries

        self.preceding_kfp_func: Callable = (
            kfp_decorator.attributes.get("preceding_component", None)
            if kfp_decorator
            else None
        )

        def bindings(binding_name: str) -> List[str]:
            if kfp_decorator:
                binding_fields = kfp_decorator.attributes[binding_name]
                if isinstance(binding_fields, str):
                    return binding_fields.split(" ")
                else:
                    return binding_fields
            else:
                return []

        self.preceding_component_inputs: List[str] = bindings(
            "preceding_component_inputs"
        )
        self.preceding_component_outputs: List[str] = bindings(
            "preceding_component_outputs"
        )


class KubeflowPipelines(object):
    def __init__(
        self,
        name,
        graph,
        flow,
        code_package,
        code_package_url,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        base_image=None,
        s3_code_package=True,
        tags=None,
        sys_tags=None,
        experiment=None,
        namespace=None,
        username=None,
        max_parallelism=None,
        workflow_timeout=None,
        notify=False,
        notify_on_error=None,
        notify_on_success=None,
        sqs_url_on_error=None,
        sqs_role_arn_on_error=None,
        **kwargs,
    ):
        """
        Analogous to step_functions_cli.py
        """
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package = code_package
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.sys_tags = sys_tags
        self.experiment = experiment
        self.namespace = namespace
        self.username = username
        self.base_image = base_image
        self.s3_code_package = s3_code_package
        self.max_parallelism = max_parallelism
        self.workflow_timeout = (
            workflow_timeout if workflow_timeout else 0  # 0 is unlimited
        )
        self.notify = notify
        self.notify_on_error = notify_on_error
        self.notify_on_success = notify_on_success
        self.sqs_url_on_error = sqs_url_on_error
        self.sqs_role_arn_on_error = sqs_role_arn_on_error
        self._client = None

    def deploy(
        self,
        kubernetes_namespace: str,
        name: Optional[str],
        flow_parameters: Dict,
        recurring_run_enable: Optional[bool] = None,
        recurring_run_cron: Optional[str] = None,
        recurring_run_policy: Optional[str] = None,
        max_run_concurrency: Optional[int] = 10,
    ) -> Dict[str, Any]:
        try:
            # Step 1: Create the resources definitions
            workflow_template: Dict[str, Any] = self._create_workflow_yaml(
                flow_parameters=flow_parameters,
                kind="WorkflowTemplate",
                name=name,
            )

            config_map: Dict[str, Any] = KubeflowPipelines._config_map(
                sanitize_k8s_name(self.name), max_run_concurrency
            )

            cron_workflow: Dict[str, Any] = KubeflowPipelines._cron_workflow(
                sanitize_k8s_name(self.name),
                schedule=recurring_run_cron,
                concurrency=recurring_run_policy,
                recurring_run_enable=recurring_run_enable,
            )

            # Step 2: Deploy the resources definitions
            argo_workflow_name = workflow_template["metadata"]["name"]

            ArgoClient(namespace=kubernetes_namespace).create_workflow_config_map(
                argo_workflow_name, config_map
            )

            k8s_workflow = ArgoClient(
                namespace=kubernetes_namespace
            ).register_workflow_template(argo_workflow_name, workflow_template)

            ArgoClient(namespace=kubernetes_namespace).create_cron_workflow(
                argo_workflow_name, cron_workflow
            )

            return k8s_workflow
        except Exception as e:
            raise KfpException(str(e))

    @classmethod
    def trigger(cls, kubernetes_namespace: str, name: str, parameters=None):
        if parameters is None:
            parameters = {}
        try:
            workflow_template = ArgoClient(
                namespace=kubernetes_namespace
            ).get_workflow_template(name)
        except Exception as e:
            raise KfpException(str(e))
        if workflow_template is None:
            raise KfpException(
                f"The workflow *{name}* doesn't exist on Argo Workflows in namespace *{kubernetes_namespace}*. "
                "Please deploy your flow first."
            )
        try:
            return ArgoClient(namespace=kubernetes_namespace).trigger_workflow_template(
                name, parameters
            )
        except Exception as e:
            raise KfpException(str(e))

    def _create_workflow_yaml(
        self,
        flow_parameters: Dict,
        kind: str,
        max_run_concurrency: Optional[int] = 10,
        name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Creates a new Argo Workflow pipeline YAML using `kfp.compiler.Compiler()`.
        Note: Intermediate pipeline YAML is saved at `pipeline_file_path`
        """
        pipeline_func, pipeline_conf = self.create_kfp_pipeline_from_flow_graph(
            flow_parameters
        )
        workflow: Dict[Text, Any] = kfp.compiler.Compiler()._create_workflow(
            pipeline_func=pipeline_func,
            pipeline_conf=pipeline_conf,
        )

        workflow["spec"]["arguments"]["parameters"] = [
            dict(name=k, value=json.dumps(v) if isinstance(v, dict) else v)
            for k, v in flow_parameters.items()
        ]

        if kind == "Workflow":
            # Output of KFP compiler already has workflow["kind"] = "Workflow".

            # Keep generateName - Argo Workflow is usually used in single run.

            # Service account is added through webhooks.
            workflow["spec"].pop("serviceAccountName", None)
        elif kind == "WorkflowTemplate":
            workflow["kind"] = "WorkflowTemplate"
            workflow["spec"]["serviceAccountName"] = (
                KUBERNETES_SERVICE_ACCOUNT or "default-editor"
            )

            # Use static name to make referencing easier.
            # Note the name has to follow k8s format.
            # self.name is typically CamelCase as it's python class name.
            # generateName contains a sanitized version of self.name from kfp.compiler
            workflow["metadata"]["name"] = (
                name if name else workflow["metadata"].pop("generateName").rstrip("-")
            )

            # Service account is added through webhooks.
            workflow["spec"].pop("serviceAccountName", None)
        else:
            raise NotImplementedError(f"Unsupported output format {kind}.")

        if max_run_concurrency and max_run_concurrency > 0:
            workflow["spec"]["synchronization"] = {
                "semaphore": {
                    "configMapKeyRef": {
                        "name": sanitize_k8s_name(self.name),
                        "key": "max_run_concurrency",
                    }
                }
            }

        return workflow

    @staticmethod
    def _config_map(workflow_name: str, max_run_concurrency: int):
        if not max_run_concurrency or max_run_concurrency <= 0:
            raise KfpException(f"{max_run_concurrency=} must be > 0.")

        config_map = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": workflow_name},
            "data": {"max_run_concurrency": str(max_run_concurrency)},
        }
        return config_map

    @staticmethod
    def _cron_workflow(
        name: str,
        schedule: Optional[str] = None,
        concurrency: Optional[str] = None,
        recurring_run_enable: Optional[bool] = False,
    ) -> Dict[str, Any]:
        body = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "CronWorkflow",
            "metadata": {"name": name},
            "spec": {
                "suspend": not recurring_run_enable,
                "schedule": (
                    schedule if schedule else "* * 0 * *"
                ),  # Day of month: 0 (invalid day) will never run
                "concurrencyPolicy": concurrency,
                "workflowSpec": {"workflowTemplateRef": {"name": name}},
            },
        }

        return body

    def run_workflow_on_argo(
        self,
        kubernetes_namespace: str,
        flow_parameters: dict,
        max_run_concurrency: Optional[int] = 10,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Creates a new run on Argo using the `KubernetesClient()`.
        """
        workflow: Dict[str, Any] = self._create_workflow_yaml(
            flow_parameters, kind="Workflow", max_run_concurrency=max_run_concurrency
        )
        argo_workflow_name: str = sanitize_k8s_name(self.name)

        config_map: Dict[str, Any] = KubeflowPipelines._config_map(
            argo_workflow_name, max_run_concurrency
        )

        try:
            # Create the Argo synchronization ConfigMap
            config = ArgoClient(
                namespace=kubernetes_namespace
            ).create_workflow_config_map(argo_workflow_name, config_map)

            # Create/Run the Argo Workflow
            running_workflow = ArgoClient(namespace=kubernetes_namespace).run_workflow(
                workflow
            )
            return running_workflow, config
        except Exception as e:
            raise KfpException(str(e))

    def write_workflow_kind(
        self,
        output_path: str,
        kind: str,
        flow_parameters: Optional[dict] = None,
        name: Optional[str] = None,
        recurring_run_enable: Optional[bool] = None,
        recurring_run_cron: Optional[str] = None,
        recurring_run_policy: Optional[str] = None,
        max_run_concurrency: Optional[int] = 10,
    ) -> str:
        if kind in ["Workflow", "WorkflowTemplate"]:
            workflow: Dict[str, Any] = self._create_workflow_yaml(
                flow_parameters,
                kind,
                max_run_concurrency,
                name,
            )
            kfp.compiler.Compiler()._write_workflow(workflow, output_path)
        elif kind == "CronWorkflow":
            cron_workflow: Dict[str, Any] = KubeflowPipelines._cron_workflow(
                sanitize_k8s_name(self.name),
                schedule=recurring_run_cron,
                concurrency=recurring_run_policy,
                recurring_run_enable=recurring_run_enable,
            )
            with open(output_path, "w") as yaml_file:
                yaml.safe_dump(cron_workflow, yaml_file, default_flow_style=False)
        elif kind == "ConfigMap":
            config_map = KubeflowPipelines._config_map(
                sanitize_k8s_name(self.name), max_run_concurrency
            )
            with open(output_path, "w") as yaml_file:
                yaml.safe_dump(config_map, yaml_file, default_flow_style=False)
        else:
            raise NotImplementedError(f"Unsupported output format {kind}.")

        return os.path.abspath(output_path)

    @staticmethod
    def _get_retries(node: DAGNode) -> Tuple[int, int]:
        """
        Analogous to step_functions_cli.py
        """
        max_user_code_retries = 0
        max_error_retries = 0
        # Different decorators may have different retrying strategies, so take
        # the max of them.
        for deco in node.decorators:
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        return max_user_code_retries, max_user_code_retries + max_error_retries

    @staticmethod
    def _get_resource_requirements(node: DAGNode) -> Dict[str, str]:
        """
        Get resources for a Metaflow step (node) set by @resources decorator.

        Supported parameters: 'cpu', 'gpu', 'gpu_vendor', 'memory'

        Eventually resource request and limits link back to kubernetes, see
        https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/

        For 'cpu' and 'memory', the provided value becomes both the
        resource request and resource limit.

        Default unit for memory is megabyte, aligning with existing resource decorator usage.

        Example using resource decorator:
            @resource(cpu=0.5, gpu=1, memory=300)
            @step
            def my_kfp_step(): ...
        """

        def to_k8s_resource_format(resource: str, value: Union[int, float, str]) -> str:
            value = str(value)

            # Defaults memory unit to megabyte
            if (
                resource
                in [
                    "memory",
                    "volume",
                ]
                and value.isnumeric()
            ):
                value = f"{value}M"
            return value

        resource_requirements = {}
        for deco in node.decorators:
            if isinstance(deco, ResourcesDecorator):
                if deco.attributes.get("local_storage") is not None:
                    raise ValueError(  # Not using DeprecationWarning to hard block the run before triggering.
                        "`local_storage` option is deprecated over cluster stability concerns. "
                        "Please use `volume` for storage request."
                    )

                for attr_key, attr_value in deco.attributes.items():
                    if attr_value is not None:
                        resource_requirements[attr_key] = to_k8s_resource_format(
                            attr_key, attr_value
                        )

        return resource_requirements

    def _create_flow_variables(self) -> FlowVariables:
        flow_variables = FlowVariables(
            flow_name=self.flow.name,
            environment=self.environment.TYPE,
            event_logger=self.event_logger.logger_type,
            monitor=self.monitor.monitor_type,
            namespace=self.namespace,
            tags=list(self.tags),
            sys_tags=list(self.sys_tags),
            package_commands=self._get_package_commands(
                code_package_url=self.code_package_url,
                environment=self.environment,
            ),
        )
        return flow_variables

    def _get_package_commands(
        self,
        code_package_url: str,
        environment: MetaflowEnvironment,
    ) -> str:
        if self.s3_code_package:
            cmd: List[str] = [
                "mkdir -p /opt/metaflow_volume/metaflow_logs",
                "export MFLOG_STDOUT=/opt/metaflow_volume/metaflow_logs/mflog_stdout",
            ]
            cmd.extend(
                environment.get_package_commands(code_package_url, is_kfp_plugin=True)
            )
            return " && ".join(cmd)
        else:
            return " cd " + str(Path(inspect.getabsfile(self.flow.__class__)).parent)

    def _create_step_variables(self, node: DAGNode) -> StepVariables:
        """
        Returns the Metaflow Node StepVariables, which is
        used to run Metaflow on KFP "kfp_metaflow_step()"
        """

        task_id: str = graph_to_task_ids(self.graph)[node.name]
        user_code_retries, total_retries = KubeflowPipelines._get_retries(node)
        resource_requirements: Dict[str, str] = self._get_resource_requirements(node)

        is_split_index: bool = (
            True
            if any(self.graph[n].type == "foreach" for n in node.in_funcs)
            else False
        )
        volume_dir: str = (
            ""  # simulating passing None type object to command line
            if "volume_dir" not in resource_requirements
            else resource_requirements["volume_dir"]
        )

        return StepVariables(
            step_name=node.name,
            volume_dir=volume_dir,
            is_split_index=is_split_index,
            task_id=task_id,
            user_code_retries=user_code_retries,
        )

    def _create_kfp_components_from_graph(self) -> Dict[str, KfpComponent]:
        """
        Returns a map of steps to their corresponding KfpComponent.
        The KfpComponent defines the component attributes
        and step command to be used to run that particular step.
        """

        def build_kfp_component(node: DAGNode, task_id: str) -> KfpComponent:
            """
            Returns the KfpComponent for each step.
            """

            for deco in node.decorators:
                if isinstance(deco, UNSUPPORTED_DECORATORS):
                    raise KfpException(
                        f"{type(deco)} in {node.name} step is not yet supported by kfp"
                    )

            user_code_retries, total_retries = KubeflowPipelines._get_retries(node)
            resource_requirements = self._get_resource_requirements(node)

            return KfpComponent(
                step_name=node.name,
                resource_requirements=resource_requirements,
                kfp_decorator=next(
                    (
                        deco
                        for deco in node.decorators
                        if isinstance(deco, KfpInternalDecorator)
                    ),
                    None,  # default
                ),
                accelerator_decorator=next(
                    (
                        deco
                        for deco in node.decorators
                        if isinstance(deco, AcceleratorDecorator)
                    ),
                    None,  # default
                ),
                interruptible_decorator=next(
                    (
                        deco
                        for deco in node.decorators
                        if isinstance(deco, interruptibleDecorator)
                    ),
                    None,  # default
                ),
                environment_decorator=next(
                    (
                        deco
                        for deco in node.decorators
                        if isinstance(deco, EnvironmentDecorator)
                    ),
                    None,  # default
                ),
                total_retries=total_retries,
            )

        # Mapping of steps to their KfpComponent
        task_ids: Dict[str, str] = graph_to_task_ids(self.graph)
        step_name_to_kfp_component: Dict[str, KfpComponent] = {}
        for step_name, task_id in task_ids.items():
            node = self.graph[step_name]
            step_name_to_kfp_component[step_name] = build_kfp_component(node, task_id)

        return step_name_to_kfp_component

    @staticmethod
    def _create_resource_based_node_type_toleration(
        cpu: float, memory: float
    ) -> Optional[V1Toleration]:
        """Allow large enough pod to use higher cost nodes by adding toleration.

        Together with taint added at cluster side,
        this is a temporary solution to fix "r5.12xlarge host not scaling down" issue,
        caused by smaller pods keep being scheduled onto large nodes.
        TODO: Replace using AIP-5264 MutatingWebHook (or Validating) for AIP pod scheduling policies

        The following node types are considered for setting the threshold:
        c5.4xlarge: 16 vCPU, 32 GB
        r5.12xlarge: 48 vCPU, 384 GB
        Resource threshold are lower than machine resource boundary to take overheads into
        account.

        Toleration allows pods to utilize larger nodes without enforcement.
        Setting a low threshold for using larger host allow more freedom for scheduler
        and potentially higher utilization rate.

        cpu: number of vCPU requested. Fractions are allowed.
        memory: memory requested in GB (not GiB)
        """
        # Analysis on Oct. 21, 2021:
        # c5.4xlarge is the default CPU pods
        # Base on observed resource data available resource per c5.4xlarge node:
        #   Memory: 20.24GB = 18.85 GiB = 27.72 GiB (allocatable) - 8.87 GiB (DaemonSet)
        #   CPU: 10.34 vCPU = 15.89 (allocatable) - 5.55 (DaemonSet)
        # Argo additionally adds a "wait" container to pods per step, taking default resources

        # Threshold should leave significant margin below estimated available resource
        # Pods will be unschedulable if its resource requirement falls in range
        #   [available resource, thresholdfor large node)

        # Using 50% of node total resource on c5.4xlarge (16vCPU, 32GB memory)
        memory_threshold = 16  # GB
        cpu_threshold = 8  # vCPU

        if memory >= memory_threshold or cpu >= cpu_threshold:
            return V1Toleration(
                effect="NoSchedule",
                key="node.k8s.zgtools.net/purpose",
                operator="Equal",
                value="high-memory",
            )
        else:
            return None

    def _set_container_volume(
        self,
        container_op: ContainerOp,
        kfp_component: KfpComponent,
        workflow_uid: str,
        shared_volumes: Dict[str, Dict[str, Tuple[ResourceOp, PipelineVolume]]],
    ) -> ResourceOp:
        resource_requirements: Dict[str, Any] = kfp_component.resource_requirements
        resource_op: Optional[ResourceOp] = None

        if "volume" in resource_requirements:
            mode = resource_requirements["volume_mode"]
            volume_dir = resource_requirements["volume_dir"]

            if mode == "ReadWriteMany":
                # ReadWriteMany shared volumes are created way before
                (resource_op, volume) = shared_volumes[kfp_component.step_name]
                container_op.add_pvolumes(volume)
            else:
                (resource_op, volume) = self._create_volume(
                    step_name=kfp_component.step_name,
                    size=resource_requirements["volume"],
                    workflow_uid=workflow_uid,
                    mode=mode,
                    volume_type=resource_requirements.get("volume_type"),
                )
                container_op.add_pvolumes({volume_dir: volume})

        return resource_op

    @staticmethod
    def _set_container_resources(
        container_op: ContainerOp, kfp_component: KfpComponent
    ):
        resource_requirements: Dict[str, Any] = kfp_component.resource_requirements
        if "memory" in resource_requirements:
            container_op.container.set_memory_request(resource_requirements["memory"])
            container_op.container.set_memory_limit(resource_requirements["memory"])
        if "cpu" in resource_requirements:
            container_op.container.set_cpu_request(resource_requirements["cpu"])
            container_op.container.set_cpu_limit(resource_requirements["cpu"])
        if "gpu" in resource_requirements:
            # TODO(yunw)(AIP-2048): Support mixture of GPU from different vendors.
            gpu_vendor = resource_requirements.get("gpu_vendor", None)
            container_op.container.set_gpu_limit(
                resource_requirements["gpu"],
                vendor=gpu_vendor if gpu_vendor else "nvidia",
            )

        if "shared_memory" in resource_requirements:
            memory_volume = PipelineVolume(
                volume=V1Volume(
                    # k8s volume name must consist of lower case alphanumeric characters or '-',
                    # and must start and end with an alphanumeric character,
                    # but step name is python function name that tends to be alphanumeric chars with '_'
                    name=f"{kfp_component.step_name.lower().replace('_', '-')}-shm",
                    empty_dir=V1EmptyDirVolumeSource(
                        medium="Memory",
                        size_limit=resource_requirements["shared_memory"],
                    ),
                )
            )
            container_op.add_pvolumes({"dev/shm": memory_volume})

        affinity_match_expressions: List[V1NodeSelectorRequirement] = []

        if kfp_component.accelerator_decorator:
            accelerator_type: Optional[
                str
            ] = kfp_component.accelerator_decorator.attributes["type"]

            if accelerator_type:
                # ensures we only select a node with the correct accelerator type (based on selector)
                affinity_match_expressions.append(
                    V1NodeSelectorRequirement(
                        key="k8s.amazonaws.com/accelerator",
                        operator="In",
                        values=[accelerator_type],
                    )
                )
                # ensures the pod created has the correct toleration corresponding to the taint
                # on the accelerator node for it to be scheduled on that node
                toleration = V1Toleration(
                    # the `effect` parameter must be specified at the top!
                    # otherwise, there is undefined behavior
                    effect="NoSchedule",
                    key="k8s.amazonaws.com/accelerator",
                    operator="Equal",
                    value=accelerator_type,
                )
                container_op.add_toleration(toleration)

        elif "gpu" not in resource_requirements:
            # Memory and cpu value already validated by set_memory_request and set_cpu_request
            toleration = KubeflowPipelines._create_resource_based_node_type_toleration(
                cpu=_get_cpu_number(resource_requirements.get("cpu", "0")),
                memory=_get_resource_number(resource_requirements.get("memory", "0")),
            )
            if toleration:
                container_op.add_toleration(toleration)

        if kfp_component.interruptible_decorator:
            affinity_match_expressions.append(
                V1NodeSelectorRequirement(
                    key="node.k8s.zgtools.net/capacity-type",
                    operator="In",
                    values=["spot"],
                )
            )

            # ensures the pod created has the correct toleration corresponding to the taint
            # on the spot node for it to be scheduled on that node
            toleration = V1Toleration(
                # the `effect` parameter must be specified at the top!
                # otherwise, there is undefined behavior
                effect="NoSchedule",
                key="node.k8s.zgtools.net/capacity-type",
                operator="Equal",
                value="spot",
            )
            # container_op.add_affinity(affinity)
            container_op.add_toleration(toleration)

        if len(affinity_match_expressions) > 0:
            node_selector = V1NodeSelector(
                node_selector_terms=[
                    V1NodeSelectorTerm(match_expressions=affinity_match_expressions)
                ]
            )
            node_affinity = V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=node_selector
            )
            affinity = V1Affinity(node_affinity=node_affinity)
            container_op.add_affinity(affinity)

    # used by the workflow_uid_op and the s3_sensor_op to tighten resources
    # to ensure customers don't bear unnecesarily large costs
    @staticmethod
    def _set_minimal_container_resources(
        container_op: ContainerOp, memory: str = "200M"
    ):
        container_op.container.set_cpu_request("0.5")
        container_op.container.set_cpu_limit("0.5")
        container_op.container.set_memory_request(memory)
        container_op.container.set_memory_limit(memory)

    def _create_volume(
        self,
        step_name: str,
        size: str,
        workflow_uid: str,
        mode: str,
        volume_type: Optional[str],
    ) -> Tuple[ResourceOp, PipelineVolume]:
        volume_name = (
            sanitize_k8s_name(step_name) if mode == "ReadWriteMany" else "{{pod.name}}"
        )
        attribute_outputs = {"size": "{.status.capacity.storage}"}
        requested_resources = V1ResourceRequirements(requests={"storage": size})

        # AIP-6788(talebz): Add volume_type to PVC
        #  to support faster storage classes (ex: EBS volume types)
        pvc_spec = V1PersistentVolumeClaimSpec(
            access_modes=dsl.VOLUME_MODE_RWO,
            resources=requested_resources,
            storage_class_name=volume_type,
        )
        owner_reference = V1OwnerReference(
            api_version="argoproj.io/v1alpha1",
            controller=True,
            kind="Workflow",
            name="{{workflow.name}}",
            uid=workflow_uid,
        )
        owner_references = [owner_reference]
        pvc_metadata = V1ObjectMeta(
            name=f"{{{{workflow.name}}}}-{volume_name}-pvc",
            owner_references=owner_references,
        )
        k8s_resource = V1PersistentVolumeClaim(
            api_version="v1",
            kind="PersistentVolumeClaim",
            metadata=pvc_metadata,
            spec=pvc_spec,
        )
        resource = ResourceOp(
            name=f"create-{step_name}-volume",
            k8s_resource=k8s_resource,
            attribute_outputs=attribute_outputs,
        )

        self._set_container_labels(resource)

        volume = PipelineVolume(
            name=f"{volume_name}-volume", pvc=resource.outputs["name"]
        )
        return (resource, volume)

    def _set_container_labels(self, container_op: ContainerOp):
        # TODO(talebz): A Metaflow plugin framework to customize tags, labels, etc.
        container_op.add_pod_label("aip.zillowgroup.net/kfp-pod-default", "true")

        # https://github.com/argoproj/argo-workflows/issues/4525
        # all argo workflows need istio-injection disabled, else the workflow hangs.
        container_op.add_pod_label("sidecar.istio.io/inject", "false")

        prefix = "metaflow.org"
        container_op.add_pod_annotation(f"{prefix}/flow_name", self.name)
        container_op.add_pod_annotation(f"{prefix}/step", container_op.name)
        container_op.add_pod_annotation(f"{prefix}/run_id", METAFLOW_RUN_ID)
        if self.experiment:
            container_op.add_pod_annotation(f"{prefix}/experiment", self.experiment)
        all_tags = list()
        all_tags += self.tags if self.tags else []
        all_tags += self.sys_tags if self.sys_tags else []
        for tag in all_tags:
            if ":" in tag:  # Metaflow commonly uses <name>:<value> as tag format
                tag_info = tag.split(":", 1)
                annotation_name = f"{prefix}/tag_{tag_info[0]}"
                annotation_value = tag_info[1]
            else:
                annotation_name = f"{prefix}/tag_{tag}"
                annotation_value = "true"

            if len(annotation_name) > 63:
                raise ValueError(
                    f"Tag name {annotation_name} must be no more than 63 characters"
                )
            container_op.add_pod_annotation(annotation_name, annotation_value)

        # tags.ledger.zgtools.net/* pod labels required for the ZGCP Costs Ledger
        container_op.add_pod_label("tags.ledger.zgtools.net/ai-flow-name", self.name)
        container_op.add_pod_label(
            "tags.ledger.zgtools.net/ai-step-name", container_op.name
        )
        if self.experiment:
            container_op.add_pod_label(
                "tags.ledger.zgtools.net/ai-experiment-name", self.experiment
            )

        # - In context of Zillow CICD self.username == "cicd_compile"
        # - In the context of a Zillow NB self.username == METAFLOW_USER (user_alias)
        # - In the context of Metaflow integration tests self.username == USER=$GITLAB_USER_EMAIL
        owner = self.username
        if "@" in owner:
            owner = owner.split("@")[0]
        container_op.add_pod_label("zodiac.zillowgroup.net/owner", owner)

        # Add in Zodiac service and team labels to the kfp pods if the environment variable is
        # present in the notebook (individual profile notebooks only) and set them. These labels
        # are not being added by poddefaults as they were removed. Workflows launched in project
        # profiles still get these labels added via poddefaults. Also adds in logging topic
        # annotation as this value is specific to zodiac service as well.
        if ZILLOW_ZODIAC_SERVICE and ZILLOW_ZODIAC_TEAM:
            container_op.add_pod_label(
                "zodiac.zillowgroup.net/service", ZILLOW_ZODIAC_SERVICE
            )
            container_op.add_pod_label(
                "zodiac.zillowgroup.net/team", ZILLOW_ZODIAC_TEAM
            )
            container_op.add_pod_annotation(
                "logging.zgtools.net/topic",
                f"log.fluentd-z1.{ZILLOW_ZODIAC_SERVICE}.dev",
            )

    def create_kfp_pipeline_from_flow_graph(
        self,
        flow_parameters: Dict,
    ) -> Tuple[Callable, PipelineConf]:
        """
        Returns a KFP DSL Pipeline function by walking the Metaflow Graph
        and constructing the KFP Pipeline using the KFP DSL.
        """
        step_name_to_kfp_component: Dict[
            str, KfpComponent
        ] = self._create_kfp_components_from_graph()
        flow_variables: FlowVariables = self._create_flow_variables()

        def pipeline_transform(op: ContainerOp):
            if isinstance(op, ContainerOp):
                self._set_container_labels(op)

                # Disable caching because Metaflow doesn't have memoization
                op.execution_options.caching_strategy.max_cache_staleness = "P0D"
                env_vars = {
                    "MF_POD_NAME": "metadata.name",
                    "MF_POD_NAMESPACE": "metadata.namespace",
                    "MF_ARGO_NODE_NAME": "metadata.annotations['workflows.argoproj.io/node-name']",
                    "MF_ARGO_WORKFLOW_NAME": "metadata.labels['workflows.argoproj.io/workflow']",
                    "ZODIAC_SERVICE": "metadata.labels['zodiac.zillowgroup.net/service']",
                    "ZODIAC_TEAM": "metadata.labels['zodiac.zillowgroup.net/team']",
                    "ZODIAC_OWNER": "metadata.labels['zodiac.zillowgroup.net/owner']",
                }
                for name, resource in env_vars.items():
                    op.container.add_env_variable(
                        V1EnvVar(
                            name=name,
                            value_from=V1EnvVarSource(
                                field_ref=V1ObjectFieldSelector(field_path=resource)
                            ),
                        )
                    )
                # adding in additional env variable for spark to identify if workflow was
                # launched from a notebook in an individual namespace.
                env_vars = {
                    "INDIVIDUAL_NAMESPACE": ZILLOW_INDIVIDUAL_NAMESPACE,
                }
                # add in env variable for ServiceAccount for Zillow Spark solution
                if KUBERNETES_SERVICE_ACCOUNT:
                    env_vars[
                        "METAFLOW_KUBERNETES_SERVICE_ACCOUNT"
                    ] = KUBERNETES_SERVICE_ACCOUNT
                # need to be added separately from above as there is no valueFrom/fieldRef from the env
                # var. leaving as a list format in the event future env variables need to be added without
                # a fieldRef value_from similar to this env variable.
                for name, resource in env_vars.items():
                    op.container.add_env_variable(
                        V1EnvVar(
                            name=name,
                            value=resource,
                        )
                    )

        pipeline_conf = None  # return variable

        @dsl.pipeline(name=self.name, description=self.graph.doc)
        def kfp_pipeline_from_flow(**kwargs):
            """
            **kwargs is defined to allow keyword signature modification
            """
            visited: Dict[str, ContainerOp] = {}
            visited_resource_ops: Dict[str, ResourceOp] = {}

            def build_kfp_dag(
                node: DAGNode,
                passed_in_split_indexes: str = "",
                preceding_kfp_component_op: ContainerOp = None,
                preceding_component_outputs_dict: Dict[str, dsl.PipelineParam] = None,
                workflow_uid: str = None,
                shared_volumes: Dict[
                    str, Dict[str, Tuple[ResourceOp, PipelineVolume]]
                ] = None,
            ):
                if node.name in visited:
                    return

                if preceding_component_outputs_dict is None:
                    preceding_component_outputs_dict = {}

                if shared_volumes is None:
                    shared_volumes = {}

                # If any of this node's children has a preceding_kfp_func then
                # create (kfp_decorator_component, preceding_component_inputs)
                next_kfp_decorator_component: Optional[KfpComponent] = None
                preceding_component_inputs: List[str] = []
                if any(
                    step_name_to_kfp_component[child].preceding_kfp_func
                    for child in node.out_funcs
                ):
                    next_kfp_decorator_component: KfpComponent = (
                        step_name_to_kfp_component[node.out_funcs[0]]
                    )
                    # fields to return from Flow state to KFP
                    preceding_component_inputs: List[
                        str
                    ] = next_kfp_decorator_component.preceding_component_inputs

                kfp_component: KfpComponent = step_name_to_kfp_component[node.name]
                step_variables: StepVariables = self._create_step_variables(node)
                # capture metaflow configs from client to be used at runtime
                # client configs have the highest precedence
                metaflow_configs = dict(
                    METAFLOW_DATASTORE_SYSROOT_S3=DATASTORE_SYSROOT_S3,
                    METAFLOW_USER=METAFLOW_USER,
                )

                metaflow_step_op: ContainerOp = self._create_metaflow_step_op(
                    node,
                    kfp_component,
                    step_variables,
                    flow_variables,
                    metaflow_configs,
                    flow_parameters,
                    passed_in_split_indexes,
                    preceding_component_inputs,
                    preceding_component_outputs_dict,
                )
                visited[node.name] = metaflow_step_op

                if kfp_component.environment_decorator:
                    envs = kfp_component.environment_decorator.attributes[
                        "kubernetes_vars"
                    ]
                    for env in envs if envs else []:
                        metaflow_step_op.container.add_env_variable(env)

                if kfp_component.total_retries and kfp_component.total_retries > 0:
                    metaflow_step_op.set_retry(
                        kfp_component.total_retries, policy="Always"
                    )

                if preceding_kfp_component_op:
                    metaflow_step_op.after(preceding_kfp_component_op)

                # If any of this node's children has a preceding_kfp_func then
                # create (next_preceding_component_outputs_dict, next_kfp_component_op)
                # to pass along to next step
                next_kfp_component_op: Optional[ContainerOp] = None
                next_preceding_component_outputs_dict: Dict[str, dsl.PipelineParam] = {}
                if next_kfp_decorator_component:
                    next_kfp_component_op: ContainerOp = next_kfp_decorator_component.preceding_kfp_func(
                        *[
                            metaflow_step_op.outputs[mf_field]
                            for mf_field in next_kfp_decorator_component.preceding_component_inputs
                        ]
                    )

                    next_kfp_component_op.after(metaflow_step_op)

                    num_outputs = len(
                        next_kfp_decorator_component.preceding_component_outputs
                    )
                    next_preceding_component_outputs_dict = {
                        name: (
                            next_kfp_component_op.outputs[name]
                            if num_outputs > 1
                            else next_kfp_component_op.output
                        )
                        for name in next_kfp_decorator_component.preceding_component_outputs
                    }

                KubeflowPipelines._set_container_resources(
                    metaflow_step_op, kfp_component
                )
                resource_op: ResourceOp = self._set_container_volume(
                    metaflow_step_op, kfp_component, workflow_uid, shared_volumes
                )
                if resource_op:
                    visited_resource_ops[node.name] = resource_op

                if node.type == "foreach":
                    # Please see nested_parallelfor.ipynb for how this works
                    next_step_name = node.out_funcs[0]
                    with kfp.dsl.ParallelFor(
                        metaflow_step_op.outputs["foreach_splits"]
                    ) as split_index:
                        # build_kfp_dag() will halt when a foreach join is
                        # reached.
                        # NOTE: A Metaflow foreach node can only have one child
                        #  or one out_func
                        build_kfp_dag(
                            self.graph[next_step_name],
                            split_index,
                            preceding_kfp_component_op=next_kfp_component_op,
                            preceding_component_outputs_dict=next_preceding_component_outputs_dict,
                            workflow_uid=workflow_uid,
                            shared_volumes=shared_volumes,
                        )

                    # Handle the ParallelFor join step, and pass in
                    # passed_in_split_indexes of parent context
                    build_kfp_dag(
                        self.graph[node.matching_join],
                        passed_in_split_indexes,
                        preceding_kfp_component_op=next_kfp_component_op,
                        preceding_component_outputs_dict=next_preceding_component_outputs_dict,
                        workflow_uid=workflow_uid,
                        shared_volumes=shared_volumes,
                    )
                else:
                    for step in node.out_funcs:
                        step_node = self.graph[step]
                        if (
                            step_node.type == "join"
                            and self.graph[step_node.split_parents[-1]].type
                            == "foreach"
                        ):
                            # halt with a foreach join is reached
                            # see the ParallelFor and adjacent call to build_kfp_dag()
                            # which handles the ParallelFor join.
                            return
                        else:
                            build_kfp_dag(
                                step_node,
                                passed_in_split_indexes,
                                preceding_kfp_component_op=next_kfp_component_op,
                                preceding_component_outputs_dict=next_preceding_component_outputs_dict,
                                workflow_uid=workflow_uid,
                                shared_volumes=shared_volumes,
                            )

            def call_build_kfp_dag(workflow_uid_op: ContainerOp):
                build_kfp_dag(
                    self.graph["start"],
                    workflow_uid=workflow_uid_op.output if workflow_uid_op else None,
                    shared_volumes=self.create_shared_volumes(
                        step_name_to_kfp_component, workflow_uid_op
                    ),
                )

            if self.notify or self.sqs_url_on_error:
                with dsl.ExitHandler(
                    self._create_exit_handler_op(
                        flow_variables.package_commands, flow_parameters
                    )
                ):
                    s3_sensor_op: Optional[ContainerOp] = self.create_s3_sensor_op(
                        flow_variables,
                    )
                    workflow_uid_op: Optional[
                        ContainerOp
                    ] = self._create_workflow_uid_op(
                        s3_sensor_op.output if s3_sensor_op else "",
                        step_name_to_kfp_component,
                        flow_variables.package_commands,
                    )
                    call_build_kfp_dag(workflow_uid_op)
            else:
                # TODO: can this and above duplicated code be in a function?
                s3_sensor_op: Optional[ContainerOp] = self.create_s3_sensor_op(
                    flow_variables,
                )
                workflow_uid_op: Optional[ContainerOp] = self._create_workflow_uid_op(
                    s3_sensor_op.output if s3_sensor_op else "",
                    step_name_to_kfp_component,
                    flow_variables.package_commands,
                )
                call_build_kfp_dag(workflow_uid_op)

            # Instruct KFP of the DAG order by iterating over the Metaflow
            # graph nodes.  Each Metaflow graph node has in_funcs (nodes that
            # point to this node), and we use that to instruct to KFP of the
            # order.
            # NOTE: It is the Metaflow compiler's job to check for cycles and a
            #   correctly constructed DAG (ex: splits and foreaches are joined).
            for step in self.graph.nodes:
                node = self.graph[step]
                for parent_step in node.in_funcs:
                    visited[node.name].after(visited[parent_step])
                    if node.name in visited_resource_ops:
                        visited_resource_ops[node.name].after(visited[parent_step])

            if s3_sensor_op:
                visited["start"].after(s3_sensor_op)

            dsl.get_pipeline_conf().add_op_transformer(pipeline_transform)
            dsl.get_pipeline_conf().set_parallelism(self.max_parallelism)
            dsl.get_pipeline_conf().set_timeout(self.workflow_timeout)
            if (
                KFP_TTL_SECONDS_AFTER_FINISHED is not None
            ):  # if None, KFP falls back to the Argo defaults
                dsl.get_pipeline_conf().set_ttl_seconds_after_finished(
                    KFP_TTL_SECONDS_AFTER_FINISHED
                )
            pipeline_conf = dsl.get_pipeline_conf()

        # replace the pipeline signature parameters with flow_parameters
        # and the pipeline name
        kfp_pipeline_from_flow.__name__ = self.name
        kfp_pipeline_from_flow.__signature__ = inspect.signature(
            kfp_pipeline_from_flow
        ).replace(
            parameters=[
                inspect.Parameter(
                    key, kind=inspect.Parameter.KEYWORD_ONLY, default=value
                )
                for key, value in flow_parameters.items()
            ]
        )
        return kfp_pipeline_from_flow, pipeline_conf

    def create_shared_volumes(
        self,
        step_name_to_kfp_component: Dict[str, KfpComponent],
        workflow_uid_op: ContainerOp,
    ) -> Dict[str, Dict[str, Tuple[ResourceOp, PipelineVolume]]]:
        """
        A volume to be shared across foreach split nodes, but not downstream steps.
        An example use case is PyTorch distributed training where gradients are communicated
        via the shared volume.
        Returns: Dict[step_name, Dict[volume_dir, PipelineVolume]]
        """
        shared_volumes: Dict[str, Dict[str, Tuple[ResourceOp, PipelineVolume]]] = {}

        for kfp_component in step_name_to_kfp_component.values():
            resources = kfp_component.resource_requirements
            if (
                "volume_mode" in resources
                and resources["volume_mode"] == "ReadWriteMany"
            ):
                volume_dir = resources["volume_dir"]
                (resource_op, volume) = self._create_volume(
                    step_name=f"{kfp_component.step_name}-shared",
                    size=resources["volume"],
                    workflow_uid=workflow_uid_op.output,
                    mode=resources["volume_mode"],
                    volume_type=resources.get("volume_type"),
                )
                shared_volumes[kfp_component.step_name] = (
                    resource_op,
                    {volume_dir: volume},
                )

        return shared_volumes

    def _create_metaflow_step_op(
        self,
        node: DAGNode,
        kfp_component: KfpComponent,
        step_variables: StepVariables,
        flow_variables: FlowVariables,
        metaflow_configs: Dict[str, str],
        flow_parameters: Dict,
        passed_in_split_indexes: str,
        preceding_component_inputs: List[str],
        preceding_component_outputs_dict: Dict[str, dsl.PipelineParam],
    ) -> ContainerOp:
        # TODO (hariharans): https://zbrt.atl.zillow.net/browse/AIP-5406
        #   (Title: Clean up output formatting of workflow and pod specs in container op)
        # double json.dumps() to ensure we have the correct quotation marks
        # on the outside of the string to be passed as a command line environment
        # and still be a valid JSON string when loaded by the Python module.
        metaflow_execution_cmd: str = (
            " && python -m metaflow.plugins.kfp.kfp_metaflow_step"
            f' --volume_dir "{step_variables.volume_dir}"'
            f" --environment {flow_variables.environment}"
            f" --event_logger {flow_variables.event_logger}"
            f" --flow_name {flow_variables.flow_name}"
            f" --metaflow_configs_json {json.dumps(json.dumps(metaflow_configs))}"
            f" --metaflow_run_id {METAFLOW_RUN_ID}"
            f" --monitor {flow_variables.monitor}"
            f' --passed_in_split_indexes "{passed_in_split_indexes}"'
            f" --preceding_component_inputs_json {json.dumps(json.dumps(preceding_component_inputs))}"
            f" --preceding_component_outputs_json {json.dumps(json.dumps(kfp_component.preceding_component_outputs))}"
            f" --script_name {os.path.basename(sys.argv[0])}"
            f" --step_name {step_variables.step_name}"
            f" --tags_json {json.dumps(json.dumps(flow_variables.tags))}"
            f" --sys_tags_json {json.dumps(json.dumps(flow_variables.sys_tags))}"
            f" --task_id {step_variables.task_id}"
            f" --user_code_retries {step_variables.user_code_retries}"
            + (
                " --is-interruptible "
                if kfp_component.interruptible_decorator
                else " --not-interruptible "
            )
            + " --workflow_name {{workflow.name}}"
        )

        if node.name == "start":
            metaflow_execution_cmd += f" --flow_parameters_json '{FLOW_PARAMETERS_JSON if flow_parameters else []}'"
        if node.type == "foreach":
            metaflow_execution_cmd += f" --is_foreach_step"
        if flow_variables.namespace:
            metaflow_execution_cmd += f" --namespace {flow_variables.namespace}"
        if step_variables.is_split_index:
            metaflow_execution_cmd += " --is_split_index"

        metaflow_execution_cmd += ' --preceding_component_outputs_dict "'
        for key in preceding_component_outputs_dict:
            # TODO: understand how KFP maps the parameter
            metaflow_execution_cmd += f"{key}={preceding_component_outputs_dict[key]},"
        metaflow_execution_cmd += '"'

        # bash -ec used because Docker starts a single process and thus to run
        # multiple bash commands, we use bash -ec to chain them.
        command = [
            "bash",
            "-ec",
            (f"{flow_variables.package_commands}" f"{metaflow_execution_cmd}"),
        ]

        if (
            kfp_component.kfp_decorator
            and kfp_component.kfp_decorator.attributes["image"]
        ):
            step_image = kfp_component.kfp_decorator.attributes["image"]
        else:
            step_image = self.base_image

        artifact_argument_paths: Optional[Dict[str, str]] = (
            None if node.name == "start" else {"flow_parameters_json": "None"}
        )

        file_outputs: Dict[str, str] = {}
        if node.type == "foreach":
            file_outputs["foreach_splits"] = "/tmp/outputs/foreach_splits/data"
        for preceding_component_input in preceding_component_inputs:
            file_outputs[
                preceding_component_input
            ] = f"/tmp/outputs/{preceding_component_input}/data"

        container_op = dsl.ContainerOp(
            name=node.name,
            image=step_image,
            command=command,
            artifact_argument_paths=artifact_argument_paths,
            file_outputs=file_outputs,
        ).set_display_name(node.name)
        return container_op

    def _create_workflow_uid_op(
        self,
        s3_sensor_path: str,
        step_name_to_kfp_component: Dict[str, KfpComponent],
        package_commands: str,
    ) -> Optional[ContainerOp]:
        if any(
            "volume" in s.resource_requirements
            for s in step_name_to_kfp_component.values()
        ):
            get_workflow_uid_command = [
                "bash",
                "-ec",
                (
                    f"{package_commands}"
                    " && python -m metaflow.plugins.kfp.kfp_get_workflow_uid"
                    f" --s3_sensor_path '{s3_sensor_path}'"
                    " --workflow_name {{workflow.name}}"
                ),
            ]
            workflow_uid_op: ContainerOp = dsl.ContainerOp(
                name="get_workflow_uid",
                image=self.base_image,
                command=get_workflow_uid_command,
                file_outputs={"Output": "/tmp/outputs/Output/data"},
            ).set_display_name("get_workflow_uid")
            KubeflowPipelines._set_minimal_container_resources(workflow_uid_op)
            return workflow_uid_op
        else:
            return None

    def create_s3_sensor_op(
        self,
        flow_variables: FlowVariables,
    ):
        s3_sensor_deco: Optional[FlowDecorator] = self.flow._flow_decorators.get(
            "s3_sensor"
        )
        if s3_sensor_deco:
            return self._create_s3_sensor_op(
                s3_sensor_deco=s3_sensor_deco,
                package_commands=flow_variables.package_commands,
            )
        else:
            return None

    def _create_s3_sensor_op(
        self,
        s3_sensor_deco: FlowDecorator,
        package_commands: str,
    ) -> ContainerOp:
        path = s3_sensor_deco.path
        timeout_seconds = s3_sensor_deco.timeout_seconds
        polling_interval_seconds = s3_sensor_deco.polling_interval_seconds
        path_formatter = s3_sensor_deco.path_formatter
        os_expandvars = s3_sensor_deco.os_expandvars

        # see https://github.com/kubeflow/pipelines/pull/1946/files
        # KFP does not support the serialization of Python functions directly. The KFP team took
        # the approach of using base64 encoding + pickle. Pickle didn't quite work out
        # in this case because pickling a function directly stores references to the function's path,
        # which couldn't be resolved when the path_formatter function was unpickled within the running
        # container. Instead, we took the approach of marshalling just the code of the path_formatter
        # function, and reconstructing the function within the kfp_s3_sensor.py code.
        if path_formatter:
            path_formatter_code_encoded = base64.b64encode(
                marshal.dumps(path_formatter.__code__)
            ).decode("ascii")
        else:
            path_formatter_code_encoded = ""

        s3_sensor_command = [
            "bash",
            "-ec",
            (
                f"{package_commands}"
                " && python -m metaflow.plugins.kfp.kfp_s3_sensor"
                " --run_id argo-{{workflow.name}}"
                f" --flow_name {self.name}"
                f" --flow_parameters_json '{FLOW_PARAMETERS_JSON}'"
                f" --path {path}"
                f" --path_formatter_code_encoded '{path_formatter_code_encoded}'"
                f" --polling_interval_seconds {polling_interval_seconds}"
                f" --timeout_seconds {timeout_seconds}"
            ),
        ]
        if os_expandvars:
            s3_sensor_command[-1] += " --os_expandvars"

        s3_sensor_op = dsl.ContainerOp(
            name="s3_sensor",
            image=self.base_image,
            command=s3_sensor_command,
            file_outputs={"Output": "/tmp/outputs/Output/data"},
        ).set_display_name("s3_sensor")

        KubeflowPipelines._set_minimal_container_resources(s3_sensor_op)
        s3_sensor_op.set_retry(S3_SENSOR_RETRY_COUNT, policy="Always")
        return s3_sensor_op

    def _create_exit_handler_op(
        self,
        package_commands: str,
        flow_parameters: Dict,
    ) -> ContainerOp:
        notify_variables: dict = {
            key: from_conf(key)
            for key in [
                "METAFLOW_NOTIFY_EMAIL_FROM",
                "METAFLOW_NOTIFY_EMAIL_SMTP_HOST",
                "METAFLOW_NOTIFY_EMAIL_SMTP_PORT",
                "METAFLOW_NOTIFY_EMAIL_BODY",
                "ARGO_RUN_URL_PREFIX",
            ]
            if from_conf(key)
        }

        if self.notify_on_error:
            notify_variables["METAFLOW_NOTIFY_ON_ERROR"] = self.notify_on_error

        if self.notify_on_success:
            notify_variables["METAFLOW_NOTIFY_ON_SUCCESS"] = self.notify_on_success

        if self.sqs_url_on_error:
            notify_variables["METAFLOW_SQS_URL_ON_ERROR"] = self.sqs_url_on_error

        if self.sqs_role_arn_on_error:
            notify_variables[
                "METAFLOW_SQS_ROLE_ARN_ON_ERROR"
            ] = self.sqs_role_arn_on_error

        # when there are no flow parameters argo complains
        # that {{workflow.parameters}} failed to resolve
        # see https://github.com/argoproj/argo-workflows/issues/6036
        flow_parameters_json = f"'{FLOW_PARAMETERS_JSON}'"
        exit_handler_command = [
            "bash",
            "-ec",
            (
                f"{package_commands}"
                " && python -m metaflow.plugins.kfp.kfp_exit_handler"
                f" --flow_name {self.name}"
                " --run_id {{workflow.name}}"
                f" --notify_variables_json {json.dumps(json.dumps(notify_variables))}"
                f" --flow_parameters_json {flow_parameters_json if flow_parameters else '{}'}"
                "  --status {{workflow.status}}"
            ),
        ]

        return (
            dsl.ContainerOp(
                name="exit_handler",
                image=self.base_image,
                command=exit_handler_command,
            )
            .set_display_name("exit_handler")
            .set_retry(EXIT_HANDLER_RETRY_COUNT, policy="Always")
        )
