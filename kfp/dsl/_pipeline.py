# Copyright 2018-2019 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import enum
from typing import Callable, Optional, Union

from kubernetes.client.models import V1PodDNSConfig
from kfp.dsl import _container_op
from kfp.dsl import _resource_op
from kfp.dsl import _ops_group
from kfp.dsl import _component_bridge
from kfp.components import _components
from kfp.components import _naming
import sys

# This handler is called whenever the @pipeline decorator is applied.
# It can be used by command-line DSL compiler to inject code that runs for every
# pipeline definition.
_pipeline_decorator_handler = None


class PipelineExecutionMode(enum.Enum):
    # Compile to Argo YAML without support for metadata-enabled components.
    V1_LEGACY = 1
    # Compiles to Argo YAML with support for metadata-enabled components.
    # Pipelines compiled using this mode aim to be compatible with v2 semantics.
    V2_COMPATIBLE = 2
    # Compiles to KFP v2 IR for execution using the v2 engine.
    # This option is unsupported right now.
    V2_ENGINE = 3


def pipeline(name: Optional[str] = None,
             description: Optional[str] = None,
             pipeline_root: Optional[str] = None):
    """Decorator of pipeline functions.

    Example
      ::

        @pipeline(
          name='my-pipeline',
          description='My ML Pipeline.'
          pipeline_root='gs://my-bucket/my-output-path'
        )
        def my_pipeline(a: PipelineParam, b: PipelineParam):
          ...

    Args:
      name: The pipeline name. Default to a sanitized version of the function
        name.
      description: Optionally, a human-readable description of the pipeline.
      pipeline_root: The root directory to generate input/output URI under this
        pipeline. This is required if input/output URI placeholder is used in this
        pipeline.
    """

    def _pipeline(func: Callable):
        if name:
            func._component_human_name = name
        if description:
            func._component_description = description
        if pipeline_root:
            func.pipeline_root = pipeline_root

        if _pipeline_decorator_handler:
            return _pipeline_decorator_handler(func) or func
        else:
            return func

    return _pipeline


class PipelineConf():
    """PipelineConf contains pipeline level settings."""

    def __init__(self):
        self.image_pull_secrets = []
        self.timeout = 0
        self.ttl_seconds_after_finished = -1
        self._pod_disruption_budget_min_available = None
        self.op_transformers = []
        self.default_pod_node_selector = {}
        self.image_pull_policy = None
        self.parallelism = None
        self._data_passing_method = None
        self.dns_config = None

    def set_image_pull_secrets(self, image_pull_secrets):
        """Configures the pipeline level imagepullsecret.

        Args:
          image_pull_secrets: a list of Kubernetes V1LocalObjectReference For
            detailed description, check Kubernetes V1LocalObjectReference definition
            https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1LocalObjectReference.md
        """
        self.image_pull_secrets = image_pull_secrets
        return self

    def set_timeout(self, seconds: int):
        """Configures the pipeline level timeout.

        Args:
          seconds: number of seconds for timeout
        """
        self.timeout = seconds
        return self

    def set_parallelism(self, max_num_pods: int):
        """Configures the max number of total parallel pods that can execute at
        the same time in a workflow.

        Args:
          max_num_pods: max number of total parallel pods.
        """
        if max_num_pods < 1:
            raise ValueError(
                'Pipeline max_num_pods set to < 1, allowed values are > 0')

        self.parallelism = max_num_pods
        return self

    def set_ttl_seconds_after_finished(self, seconds: int):
        """Configures the ttl after the pipeline has finished.

        Args:
          seconds: number of seconds for the workflow to be garbage collected after
            it is finished.
        """
        self.ttl_seconds_after_finished = seconds
        return self

    def set_pod_disruption_budget(self, min_available: Union[int, str]):
        """PodDisruptionBudget holds the number of concurrent disruptions that
        you allow for pipeline Pods.

        Args:
          min_available (Union[int, str]):  An eviction is allowed if at least
            "minAvailable" pods selected by "selector" will still be available after
            the eviction, i.e. even in the absence of the evicted pod.  So for
            example you can prevent all voluntary evictions by specifying "100%".
            "minAvailable" can be either an absolute number or a percentage.
        """
        self._pod_disruption_budget_min_available = min_available
        return self

    def set_default_pod_node_selector(self, label_name: str, value: str):
        """Add a constraint for nodeSelector for a pipeline.

        Each constraint is a key-value pair label.

        For the container to be eligible to run on a node, the node must have each
        of the constraints appeared as labels.

        Args:
          label_name: The name of the constraint label.
          value: The value of the constraint label.
        """
        self.default_pod_node_selector[label_name] = value
        return self

    def set_image_pull_policy(self, policy: str):
        """Configures the default image pull policy.

        Args:
          policy: the pull policy, has to be one of: Always, Never, IfNotPresent.
            For more info:
            https://github.com/kubernetes-client/python/blob/10a7f95435c0b94a6d949ba98375f8cc85a70e5a/kubernetes/docs/V1Container.md
        """
        self.image_pull_policy = policy
        return self

    def add_op_transformer(self, transformer):
        """Configures the op_transformers which will be applied to all ops in
        the pipeline. The ops can be ResourceOp, VolumeOp, or ContainerOp.

        Args:
          transformer: A function that takes a kfp Op as input and returns a kfp Op
        """
        self.op_transformers.append(transformer)

    def set_dns_config(self, dns_config: V1PodDNSConfig):
        """Set the dnsConfig to be given to each pod.

        Args:
          dns_config: Kubernetes V1PodDNSConfig For detailed description, check
            Kubernetes V1PodDNSConfig definition
            https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1PodDNSConfig.md

        Example:
          ::

            import kfp
            from kubernetes.client.models import V1PodDNSConfig, V1PodDNSConfigOption
            pipeline_conf = kfp.dsl.PipelineConf()
            pipeline_conf.set_dns_config(dns_config=V1PodDNSConfig(
                nameservers=["1.2.3.4"],
                options=[V1PodDNSConfigOption(name="ndots", value="2")],
            ))
        """
        self.dns_config = dns_config

    @property
    def data_passing_method(self):
        return self._data_passing_method

    @data_passing_method.setter
    def data_passing_method(self, value):
        """Sets the object representing the method used for intermediate data
        passing.

        Example:
          ::

            from kfp.dsl import PipelineConf, data_passing_methods
            from kubernetes.client.models import V1Volume, V1PersistentVolumeClaimVolumeSource
            pipeline_conf = PipelineConf()
            pipeline_conf.data_passing_method =
            data_passing_methods.KubernetesVolume(
                volume=V1Volume(
                    name='data',
                    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource('data-volume'),
                ),
                path_prefix='artifact_data/',
            )
        """
        self._data_passing_method = value


def get_pipeline_conf():
    """Configure the pipeline level setting to the current pipeline
    Note: call the function inside the user defined pipeline function.
  """
    return Pipeline.get_default_pipeline().conf


# TODO: Pipeline is in fact an opsgroup, refactor the code.
class Pipeline():
    """A pipeline contains a list of operators.

    This class is not supposed to be used by pipeline authors since pipeline
    authors can use pipeline functions (decorated with @pipeline) to reference
    their pipelines.
    This class is useful for implementing a compiler. For example, the compiler
    can use the following to get the pipeline object and its ops:

    Example:
      ::

        with Pipeline() as p:
          pipeline_func(*args_list)

        traverse(p.ops)
    """

    # _default_pipeline is set when it (usually a compiler) runs "with Pipeline()"
    _default_pipeline = None

    @staticmethod
    def get_default_pipeline():
        """Get default pipeline."""
        return Pipeline._default_pipeline

    @staticmethod
    def add_pipeline(name, description, func):
        """Add a pipeline function with the specified name and description."""
        # Applying the @pipeline decorator to the pipeline function
        func = pipeline(name=name, description=description)(func)

    def __init__(self, name: str):
        """Create a new instance of Pipeline.

        Args:
          name: the name of the pipeline. Once deployed, the name will show up in
            Pipeline System UI.
        """
        self.name = name
        self.ops = {}
        # Add the root group.
        self.groups = [_ops_group.OpsGroup('pipeline', name=name)]
        self.group_id = 0
        self.conf = PipelineConf()
        self._metadata = None

    def __enter__(self):
        if Pipeline._default_pipeline:
            raise Exception('Nested pipelines are not allowed.')

        Pipeline._default_pipeline = self
        self._old_container_task_constructor = (
            _components._container_task_constructor)
        _components._container_task_constructor = (
            _component_bridge._create_container_op_from_component_and_arguments)

        def register_op_and_generate_id(op):
            return self.add_op(op, op.is_exit_handler)

        self._old__register_op_handler = _container_op._register_op_handler
        _container_op._register_op_handler = register_op_and_generate_id
        return self

    def __exit__(self, *args):
        Pipeline._default_pipeline = None
        _container_op._register_op_handler = self._old__register_op_handler
        _components._container_task_constructor = (
            self._old_container_task_constructor)

    def add_op(self, op: _container_op.BaseOp, define_only: bool):
        """Add a new operator.

        Args:
          op: An operator of ContainerOp, ResourceOp or their inherited types.
            Returns
          op_name: a unique op name.
        """
        # Sanitizing the op name.
        # Technically this could be delayed to the compilation stage, but string
        # serialization of PipelineParams make unsanitized names problematic.
        op_name = _naming._sanitize_python_function_name(op.human_name).replace(
            '_', '-')
        #If there is an existing op with this name then generate a new name.
        op_name = _naming._make_name_unique_by_adding_index(
            op_name, list(self.ops.keys()), '-')
        if op_name == '':
            op_name = _naming._make_name_unique_by_adding_index(
                'task', list(self.ops.keys()), '-')

        self.ops[op_name] = op
        if not define_only:
            self.groups[-1].ops.append(op)

        return op_name

    def push_ops_group(self, group: _ops_group.OpsGroup):
        """Push an OpsGroup into the stack.

        Args:
          group: An OpsGroup. Typically it is one of ExitHandler, Branch, and Loop.
        """
        self.groups[-1].groups.append(group)
        self.groups.append(group)

    def pop_ops_group(self):
        """Remove the current OpsGroup from the stack."""
        del self.groups[-1]

    def remove_op_from_groups(self, op):
        for group in self.groups:
            group.remove_op_recursive(op)

    def get_next_group_id(self):
        """Get next id for a new group."""

        self.group_id += 1
        return self.group_id

    def _set_metadata(self, metadata):
        """_set_metadata passes the containerop the metadata information.

        Args:
          metadata (ComponentMeta): component metadata
        """
        self._metadata = metadata
