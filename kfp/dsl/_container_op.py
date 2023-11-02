# Copyright 2019 The Kubeflow Authors
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

import re
import warnings
from typing import (Any, Callable, Dict, List, Optional, Sequence, Tuple,
                    TypeVar, Union)

import kfp
from kfp.components import _components, _structures
from kfp.dsl import _pipeline_param, dsl_utils
from kfp.pipeline_spec import pipeline_spec_pb2
from kubernetes.client import V1Affinity, V1Toleration
from kubernetes.client.models import (V1Container, V1ContainerPort,
                                      V1EnvFromSource, V1EnvVar, V1Lifecycle,
                                      V1Probe, V1ResourceRequirements,
                                      V1SecurityContext, V1Volume,
                                      V1VolumeDevice, V1VolumeMount)

# generics
T = TypeVar('T')
# type alias: either a string or a list of string
StringOrStringList = Union[str, List[str]]
ContainerOpArgument = Union[str, int, float, bool,
                            _pipeline_param.PipelineParam]
ArgumentOrArguments = Union[ContainerOpArgument, List]

ALLOWED_RETRY_POLICIES = (
    'Always',
    'OnError',
    'OnFailure',
    'OnTransientError',
)

# Shorthand for PipelineContainerSpec
_PipelineContainerSpec = pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec

# Unit constants for k8s size string.
_E = 10**18  # Exa
_EI = 1 << 60  # Exa: power-of-two approximate
_P = 10**15  # Peta
_PI = 1 << 50  # Peta: power-of-two approximate
# noinspection PyShadowingBuiltins
_T = 10**12  # Tera
_TI = 1 << 40  # Tera: power-of-two approximate
_G = 10**9  # Giga
_GI = 1 << 30  # Giga: power-of-two approximate
_M = 10**6  # Mega
_MI = 1 << 20  # Mega: power-of-two approximate
_K = 10**3  # Kilo
_KI = 1 << 10  # Kilo: power-of-two approximate

_GKE_ACCELERATOR_LABEL = 'cloud.google.com/gke-accelerator'

_DEFAULT_CUSTOM_JOB_MACHINE_TYPE = 'n1-standard-4'


# util functions
def deprecation_warning(func: Callable, op_name: str,
                        container_name: str) -> Callable:
    """Decorator function to give a pending deprecation warning."""

    def _wrapped(*args, **kwargs):
        warnings.warn(
            '`dsl.ContainerOp.%s` will be removed in future releases. '
            'Use `dsl.ContainerOp.container.%s` instead.' %
            (op_name, container_name), PendingDeprecationWarning)
        return func(*args, **kwargs)

    return _wrapped


def _create_getter_setter(prop):
    """Create a tuple of getter and setter methods for a property in
    `Container`."""

    def _getter(self):
        return getattr(self._container, prop)

    def _setter(self, value):
        return setattr(self._container, prop, value)

    return _getter, _setter


def _proxy_container_op_props(cls: 'ContainerOp'):
    """Takes the `ContainerOp` class and proxy the PendingDeprecation
    properties in `ContainerOp` to the `Container` instance."""
    # properties mapping to proxy: ContainerOps.<prop> => Container.<prop>
    prop_map = dict(image='image', env_variables='env')
    # itera and create class props
    for op_prop, container_prop in prop_map.items():
        # create getter and setter
        _getter, _setter = _create_getter_setter(container_prop)
        # decorate with deprecation warning
        getter = deprecation_warning(_getter, op_prop, container_prop)
        setter = deprecation_warning(_setter, op_prop, container_prop)
        # update attribites with properties
        setattr(cls, op_prop, property(getter, setter))
    return cls


def as_string_list(
        list_or_str: Optional[Union[Any, Sequence[Any]]]) -> List[str]:
    """Convert any value except None to a list if not already a list."""
    if list_or_str is None:
        return None
    if isinstance(list_or_str, Sequence) and not isinstance(list_or_str, str):
        list_value = list_or_str
    else:
        list_value = [list_or_str]
    return [str(item) for item in list_value]


def create_and_append(current_list: Union[List[T], None], item: T) -> List[T]:
    """Create a list (if needed) and appends an item to it."""
    current_list = current_list or []
    current_list.append(item)
    return current_list


class Container(V1Container):
    """A wrapper over k8s container definition object
    (io.k8s.api.core.v1.Container), which is used to represent the `container`
    property in argo's workflow template
    (io.argoproj.workflow.v1alpha1.Template).

    `Container` class also comes with utility functions to set and update the
    the various properties for a k8s container definition.

    NOTE: A notable difference is that `name` is not required and will not be
    processed for `Container` (in contrast to `V1Container` where `name` is a
    required property).

    See:
    *
    https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_container.py
    * https://github.com/argoproj/argo-workflows/blob/master/api/openapi-spec/swagger.json

    Example::

      from kfp.dsl import ContainerOp
      from kubernetes.client.models import V1EnvVar


      # creates a operation
      op = ContainerOp(name='bash-ops',
                      image='busybox:latest',
                      command=['echo'],
                      arguments=['$MSG'])

      # returns a `Container` object from `ContainerOp`
      # and add an environment variable to `Container`
      op.container.add_env_variable(V1EnvVar(name='MSG', value='hello world'))

    Attributes:
      attribute_map (dict): The key is attribute name and the value is json key
        in definition.
    """
    # remove `name` from attribute_map, swagger_types and openapi_types so `name` is not generated in the JSON

    if hasattr(V1Container, 'swagger_types'):
        swagger_types = {
            key: value
            for key, value in V1Container.swagger_types.items()
            if key != 'name'
        }
    if hasattr(V1Container, 'openapi_types'):
        openapi_types = {
            key: value
            for key, value in V1Container.openapi_types.items()
            if key != 'name'
        }
    attribute_map = {
        key: value
        for key, value in V1Container.attribute_map.items()
        if key != 'name'
    }

    def __init__(self, image: str, command: List[str], args: List[str],
                 **kwargs):
        """Creates a new instance of `Container`.

        Args:
          image {str}: image to use, e.g. busybox:latest
          command {List[str]}: entrypoint array.  Not executed within a shell.
          args {List[str]}: arguments to entrypoint.
          **kwargs: keyword arguments for `V1Container`
        """
        # set name to '' if name is not provided
        # k8s container MUST have a name
        # argo workflow template does not need a name for container def
        if not kwargs.get('name'):
            kwargs['name'] = ''

        # v2 container_spec
        self._container_spec = None

        self.env_dict = {}

        super(Container, self).__init__(
            image=image, command=command, args=args, **kwargs)

    def _validate_size_string(self, size_string):
        """Validate a given string is valid for memory/ephemeral-storage
        request or limit."""

        if isinstance(size_string, _pipeline_param.PipelineParam):
            if size_string.value:
                size_string = size_string.value
            else:
                return

        if re.match(r'^[0-9]+(E|Ei|P|Pi|T|Ti|G|Gi|M|Mi|K|Ki){0,1}$',
                    size_string) is None:
            raise ValueError(
                'Invalid memory string. Should be an integer, or integer followed '
                'by one of "E|Ei|P|Pi|T|Ti|G|Gi|M|Mi|K|Ki"')

    def _validate_cpu_string(self, cpu_string):
        """Validate a given string is valid for cpu request or limit."""

        if isinstance(cpu_string, _pipeline_param.PipelineParam):
            if cpu_string.value:
                cpu_string = cpu_string.value
            else:
                return

        if re.match(r'^[0-9]+m$', cpu_string) is not None:
            return

        try:
            float(cpu_string)
        except ValueError:
            raise ValueError(
                'Invalid cpu string. Should be float or integer, or integer followed '
                'by "m".')

    def _validate_positive_number(self, str_value, param_name):
        """Validate a given string is in positive integer format."""

        if isinstance(str_value, _pipeline_param.PipelineParam):
            if str_value.value:
                str_value = str_value.value
            else:
                return

        try:
            int_value = int(str_value)
        except ValueError:
            raise ValueError(
                'Invalid {}. Should be integer.'.format(param_name))

        if int_value <= 0:
            raise ValueError('{} must be positive integer.'.format(param_name))

    def add_resource_limit(self, resource_name, value) -> 'Container':
        """Add the resource limit of the container.

        Args:
          resource_name: The name of the resource. It can be cpu, memory, etc.
          value: The string value of the limit.
        """

        self.resources = self.resources or V1ResourceRequirements()
        self.resources.limits = self.resources.limits or {}
        self.resources.limits.update({resource_name: value})
        return self

    def add_resource_request(self, resource_name, value) -> 'Container':
        """Add the resource request of the container.

        Args:
          resource_name: The name of the resource. It can be cpu, memory, etc.
          value: The string value of the request.
        """

        self.resources = self.resources or V1ResourceRequirements()
        self.resources.requests = self.resources.requests or {}
        self.resources.requests.update({resource_name: value})
        return self

    def get_resource_limit(self, resource_name: str) -> Optional[str]:
        """Get the resource limit of the container.

        Args:
          resource_name: The name of the resource. It can be cpu, memory, etc.
        """

        if not self.resources or not self.resources.limits:
            return None
        return self.resources.limits.get(resource_name)

    def get_resource_request(self, resource_name: str) -> Optional[str]:
        """Get the resource request of the container.

        Args:
          resource_name: The name of the resource. It can be cpu, memory, etc.
        """

        if not self.resources or not self.resources.requests:
            return None
        return self.resources.requests.get(resource_name)

    def set_memory_request(
            self, memory: Union[str,
                                _pipeline_param.PipelineParam]) -> 'Container':
        """Set memory request (minimum) for this operator.

        Args:
          memory(Union[str, PipelineParam]): a string which can be a number or a number followed by one of
            "E", "P", "T", "G", "M", "K".
        """

        if not isinstance(memory, _pipeline_param.PipelineParam):
            self._validate_size_string(memory)
        return self.add_resource_request('memory', memory)

    def set_memory_limit(
            self, memory: Union[str,
                                _pipeline_param.PipelineParam]) -> 'Container':
        """Set memory limit (maximum) for this operator.

        Args:
          memory(Union[str, PipelineParam]): a string which can be a number or a number followed by one of
            "E", "P", "T", "G", "M", "K".
        """
        if not isinstance(memory, _pipeline_param.PipelineParam):
            self._validate_size_string(memory)
            if self._container_spec:
                self._container_spec.resources.memory_limit = _get_resource_number(
                    memory)
        return self.add_resource_limit('memory', memory)

    def set_ephemeral_storage_request(self, size) -> 'Container':
        """Set ephemeral-storage request (minimum) for this operator.

        Args:
          size: a string which can be a number or a number followed by one of
            "E", "P", "T", "G", "M", "K".
        """
        self._validate_size_string(size)
        return self.add_resource_request('ephemeral-storage', size)

    def set_ephemeral_storage_limit(self, size) -> 'Container':
        """Set ephemeral-storage request (maximum) for this operator.

        Args:
          size: a string which can be a number or a number followed by one of
            "E", "P", "T", "G", "M", "K".
        """
        self._validate_size_string(size)
        return self.add_resource_limit('ephemeral-storage', size)

    def set_cpu_request(
            self, cpu: Union[str,
                             _pipeline_param.PipelineParam]) -> 'Container':
        """Set cpu request (minimum) for this operator.

        Args:
          cpu(Union[str, PipelineParam]): A string which can be a number or a number followed by "m", which
            means 1/1000.
        """
        if not isinstance(cpu, _pipeline_param.PipelineParam):
            self._validate_cpu_string(cpu)
        return self.add_resource_request('cpu', cpu)

    def set_cpu_limit(
            self, cpu: Union[str,
                             _pipeline_param.PipelineParam]) -> 'Container':
        """Set cpu limit (maximum) for this operator.

        Args:
          cpu(Union[str, PipelineParam]): A string which can be a number or a number followed by "m", which
            means 1/1000.
        """

        if not isinstance(cpu, _pipeline_param.PipelineParam):
            self._validate_cpu_string(cpu)
            if self._container_spec:
                self._container_spec.resources.cpu_limit = _get_cpu_number(cpu)
        return self.add_resource_limit('cpu', cpu)

    def set_gpu_limit(
        self,
        gpu: Union[str, _pipeline_param.PipelineParam],
        vendor: Union[str, _pipeline_param.PipelineParam] = 'nvidia'
    ) -> 'Container':
        """Set gpu limit for the operator.

        This function add '<vendor>.com/gpu' into resource limit.
        Note that there is no need to add GPU request. GPUs are only supposed to
        be specified in the limits section. See
        https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/.

        Args:
          gpu(Union[str, PipelineParam]): A string which must be a positive number.
          vendor(Union[str, PipelineParam]): Optional. A string which is the vendor of the requested gpu.
            The supported values are: 'nvidia' (default), and 'amd'. The value is
            ignored in v2.
        """

        if not isinstance(gpu, _pipeline_param.PipelineParam) or not isinstance(
                gpu, _pipeline_param.PipelineParam):
            self._validate_positive_number(gpu, 'gpu')

            if self._container_spec:
                # For backforward compatibiliy, allow `gpu` to be a string.
                self._container_spec.resources.accelerator.count = int(gpu)

            if vendor != 'nvidia' and vendor != 'amd':
                raise ValueError('vendor can only be nvidia or amd.')

            return self.add_resource_limit('%s.com/gpu' % vendor, gpu)

        return self.add_resource_limit(vendor, gpu)

    def add_volume_mount(self, volume_mount) -> 'Container':
        """Add volume to the container.

        Args:
          volume_mount: Kubernetes volume mount For detailed spec, check volume
            mount definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_volume_mount.py
        """

        if not isinstance(volume_mount, V1VolumeMount):
            raise ValueError(
                'invalid argument. Must be of instance `V1VolumeMount`.')

        self.volume_mounts = create_and_append(self.volume_mounts, volume_mount)
        return self

    def add_volume_devices(self, volume_device) -> 'Container':
        """Add a block device to be used by the container.

        Args:
          volume_device: Kubernetes volume device For detailed spec, volume
            device definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_volume_device.py
        """

        if not isinstance(volume_device, V1VolumeDevice):
            raise ValueError(
                'invalid argument. Must be of instance `V1VolumeDevice`.')

        self.volume_devices = create_and_append(self.volume_devices,
                                                volume_device)
        return self

    def set_env_variable(self, name: str, value: str) -> 'Container':
        """Sets environment variable to the container (v2 only).

        Args:
          name: The name of the environment variable.
          value: The value of the environment variable.
        """

        if not kfp.COMPILING_FOR_V2:
            raise ValueError(
                'set_env_variable is v2 only. Use add_env_variable for v1.')

        # Merge with any existing environment varaibles
        self.env_dict = {
            env.name: env.value for env in self._container_spec.env or []
        }
        self.env_dict[name] = value

        del self._container_spec.env[:]
        self._container_spec.env.extend([
            _PipelineContainerSpec.EnvVar(name=name, value=value)
            for name, value in self.env_dict.items()
        ])
        return self

    def add_env_variable(self, env_variable) -> 'Container':
        """Add environment variable to the container.

        Args:
          env_variable: Kubernetes environment variable For detailed spec, check
            environment variable definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_env_var.py
        """

        if not isinstance(env_variable, V1EnvVar):
            raise ValueError(
                'invalid argument. Must be of instance `V1EnvVar`.')

        self.env = create_and_append(self.env, env_variable)
        return self

    def add_env_from(self, env_from) -> 'Container':
        """Add a source to populate environment variables int the container.

        Args:
          env_from: Kubernetes environment from source For detailed spec, check
            environment from source definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_env_var_source.py
        """

        if not isinstance(env_from, V1EnvFromSource):
            raise ValueError(
                'invalid argument. Must be of instance `V1EnvFromSource`.')

        self.env_from = create_and_append(self.env_from, env_from)
        return self

    def set_image_pull_policy(self, image_pull_policy) -> 'Container':
        """Set image pull policy for the container.

        Args:
          image_pull_policy: One of `Always`, `Never`, `IfNotPresent`.
        """
        if image_pull_policy not in ['Always', 'Never', 'IfNotPresent']:
            raise ValueError(
                'Invalid imagePullPolicy. Must be one of `Always`, `Never`, `IfNotPresent`.'
            )

        self.image_pull_policy = image_pull_policy
        return self

    def add_port(self, container_port) -> 'Container':
        """Add a container port to the container.

        Args:
          container_port: Kubernetes container port For detailed spec, check
            container port definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_container_port.py
        """

        if not isinstance(container_port, V1ContainerPort):
            raise ValueError(
                'invalid argument. Must be of instance `V1ContainerPort`.')

        self.ports = create_and_append(self.ports, container_port)
        return self

    def set_security_context(self, security_context) -> 'Container':
        """Set security configuration to be applied on the container.

        Args:
          security_context: Kubernetes security context For detailed spec, check
            security context definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_security_context.py
        """

        if not isinstance(security_context, V1SecurityContext):
            raise ValueError(
                'invalid argument. Must be of instance `V1SecurityContext`.')

        self.security_context = security_context
        return self

    def set_stdin(self, stdin=True) -> 'Container':
        """Whether this container should allocate a buffer for stdin in the
        container runtime. If this is not set, reads from stdin in the
        container will always result in EOF.

        Args:
          stdin: boolean flag
        """

        self.stdin = stdin
        return self

    def set_stdin_once(self, stdin_once=True) -> 'Container':
        """Whether the container runtime should close the stdin channel after
        it has been opened by a single attach. When stdin is true the stdin
        stream will remain open across multiple attach sessions. If stdinOnce
        is set to true, stdin is opened on container start, is empty until the
        first client attaches to stdin, and then remains open and accepts data
        until the client disconnects, at which time stdin is closed and remains
        closed until the container is restarted. If this flag is false, a
        container processes that reads from stdin will never receive an EOF.

        Args:
          stdin_once: boolean flag
        """

        self.stdin_once = stdin_once
        return self

    def set_termination_message_path(self,
                                     termination_message_path) -> 'Container':
        """Path at which the file to which the container's termination message
        will be written is mounted into the container's filesystem. Message
        written is intended to be brief final status, such as an assertion
        failure message. Will be truncated by the node if greater than 4096
        bytes. The total message length across all containers will be limited
        to 12kb.

        Args:
          termination_message_path: path for the termination message
        """
        self.termination_message_path = termination_message_path
        return self

    def set_termination_message_policy(
            self, termination_message_policy) -> 'Container':
        """Indicate how the termination message should be populated. File will
        use the contents of terminationMessagePath to populate the container
        status message on both success and failure. FallbackToLogsOnError will
        use the last chunk of container log output if the termination message
        file is empty and the container exited with an error. The log output is
        limited to 2048 bytes or 80 lines, whichever is smaller.

        Args:
          termination_message_policy: `File` or `FallbackToLogsOnError`
        """
        if termination_message_policy not in ['File', 'FallbackToLogsOnError']:
            raise ValueError(
                'terminationMessagePolicy must be `File` or `FallbackToLogsOnError`'
            )
        self.termination_message_policy = termination_message_policy
        return self

    def set_tty(self, tty: bool = True) -> 'Container':
        """Whether this container should allocate a TTY for itself, also
        requires 'stdin' to be true.

        Args:
          tty: boolean flag
        """

        self.tty = tty
        return self

    def set_readiness_probe(self, readiness_probe) -> 'Container':
        """Set a readiness probe for the container.

        Args:
          readiness_probe: Kubernetes readiness probe For detailed spec, check
            probe definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_probe.py
        """

        if not isinstance(readiness_probe, V1Probe):
            raise ValueError('invalid argument. Must be of instance `V1Probe`.')

        self.readiness_probe = readiness_probe
        return self

    def set_liveness_probe(self, liveness_probe) -> 'Container':
        """Set a liveness probe for the container.

        Args:
          liveness_probe: Kubernetes liveness probe For detailed spec, check
            probe definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_probe.py
        """

        if not isinstance(liveness_probe, V1Probe):
            raise ValueError('invalid argument. Must be of instance `V1Probe`.')

        self.liveness_probe = liveness_probe
        return self

    def set_lifecycle(self, lifecycle) -> 'Container':
        """Setup a lifecycle config for the container.

        Args:
          lifecycle: Kubernetes lifecycle For detailed spec, lifecycle
            definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_lifecycle.py
        """

        if not isinstance(lifecycle, V1Lifecycle):
            raise ValueError(
                'invalid argument. Must be of instance `V1Lifecycle`.')

        self.lifecycle = lifecycle
        return self


class UserContainer(Container):
    """Represents an argo workflow UserContainer
    (io.argoproj.workflow.v1alpha1.UserContainer) to be used in `UserContainer`
    property in argo's workflow template
    (io.argoproj.workflow.v1alpha1.Template).

    `UserContainer` inherits from `Container` class with an addition of
    `mirror_volume_mounts`
    attribute (`mirrorVolumeMounts` property).

    See
    https://github.com/argoproj/argo-workflows/blob/master/api/openapi-spec/swagger.json

    Args:
      name: unique name for the user container
      image: image to use for the user container, e.g. redis:alpine
      command: entrypoint array.  Not executed within a shell.
      args: arguments to the entrypoint.
      mirror_volume_mounts: MirrorVolumeMounts will mount the same volumes
        specified in the main container to the container (including
        artifacts), at the same mountPaths. This enables dind daemon to
        partially see the same filesystem as the main container in order to
        use features such as docker volume binding
      **kwargs: keyword arguments available for `Container`

    Attributes:
      swagger_types (dict): The key is attribute name and the value is attribute
        type.

    Example ::

      from kfp.dsl import ContainerOp, UserContainer
      # creates a `ContainerOp` and adds a redis init container
      op = (ContainerOp(name='foo-op', image='busybox:latest')
         .add_initContainer(UserContainer(name='redis', image='redis:alpine')))
    """
    # adds `mirror_volume_mounts` to `UserContainer` swagger definition
    # NOTE inherits definition from `V1Container` rather than `Container`
    #      because `Container` has no `name` property.
    if hasattr(V1Container, 'swagger_types'):
        swagger_types = dict(
            **V1Container.swagger_types, mirror_volume_mounts='bool')
    if hasattr(V1Container, 'openapi_types'):
        openapi_types = dict(
            **V1Container.openapi_types, mirror_volume_mounts='bool')
    attribute_map = dict(
        **V1Container.attribute_map, mirror_volume_mounts='mirrorVolumeMounts')

    def __init__(self,
                 name: str,
                 image: str,
                 command: StringOrStringList = None,
                 args: StringOrStringList = None,
                 mirror_volume_mounts: bool = None,
                 **kwargs):
        super().__init__(
            name=name,
            image=image,
            command=as_string_list(command),
            args=as_string_list(args),
            **kwargs)

        self.mirror_volume_mounts = mirror_volume_mounts

    def set_mirror_volume_mounts(self, mirror_volume_mounts=True):
        """Setting mirrorVolumeMounts to true will mount the same volumes
        specified in the main container to the container (including artifacts),
        at the same mountPaths. This enables dind daemon to partially see the
        same filesystem as the main container in order to use features such as
        docker volume binding.

        Args:
            mirror_volume_mounts: boolean flag
        """

        self.mirror_volume_mounts = mirror_volume_mounts
        return self

    @property
    def inputs(self):
        """A list of PipelineParam found in the UserContainer object."""
        return _pipeline_param.extract_pipelineparams_from_any(self)


class Sidecar(UserContainer):
    """Creates a new instance of `Sidecar`.

    Args:
      name: unique name for the sidecar container
      image: image to use for the sidecar container, e.g. redis:alpine
      command: entrypoint array.  Not executed within a shell.
      args: arguments to the entrypoint.
      mirror_volume_mounts: MirrorVolumeMounts will mount the same volumes
        specified in the main container to the sidecar (including artifacts),
        at the same mountPaths. This enables dind daemon to partially see the
        same filesystem as the main container in order to use features such as
        docker volume binding
      **kwargs: keyword arguments available for `Container`
    """

    def __init__(self,
                 name: str,
                 image: str,
                 command: StringOrStringList = None,
                 args: StringOrStringList = None,
                 mirror_volume_mounts: bool = None,
                 **kwargs):
        super().__init__(
            name=name,
            image=image,
            command=command,
            args=args,
            mirror_volume_mounts=mirror_volume_mounts,
            **kwargs)


def _make_hash_based_id_for_op(op):
    # Generating a unique ID for Op. For class instances, the hash is the object's memory address which is unique.
    return op.human_name + ' ' + hex(2**63 + hash(op))[2:]


# Pointer to a function that generates a unique ID for the Op instance (Possibly by registering the Op instance in some system).
_register_op_handler = _make_hash_based_id_for_op


class BaseOp(object):
    """Base operator.

    Args:
      name: the name of the op. It does not have to be unique within a
        pipeline because the pipeline will generates a unique new name in case
        of conflicts.
      init_containers: the list of `UserContainer` objects describing the
        InitContainer to deploy before the `main` container.
      sidecars: the list of `Sidecar` objects describing the sidecar
        containers to deploy together with the `main` container.
      is_exit_handler: Deprecated.
    """

    # list of attributes that might have pipeline params - used to generate
    # the input parameters during compilation.
    # Excludes `file_outputs` and `outputs` as they are handled separately
    # in the compilation process to generate the DAGs and task io parameters.
    attrs_with_pipelineparams = [
        'node_selector', 'volumes', 'pod_annotations', 'pod_labels',
        'num_retries', 'init_containers', 'sidecars', 'tolerations'
    ]

    def __init__(self,
                 name: str,
                 init_containers: List[UserContainer] = None,
                 sidecars: List[Sidecar] = None,
                 is_exit_handler: bool = False):

        if is_exit_handler:
            warnings.warn('is_exit_handler=True is no longer needed.',
                          DeprecationWarning)

        self.is_exit_handler = is_exit_handler

        # human_name must exist to construct operator's name
        self.human_name = name
        self.display_name = None  #TODO Set display_name to human_name
        # ID of the current Op. Ideally, it should be generated by the compiler that sees the bigger context.
        # However, the ID is used in the task output references (PipelineParams) which can be serialized to strings.
        # Because of this we must obtain a unique ID right now.
        self.name = _register_op_handler(self)

        # TODO: proper k8s definitions so that `convert_k8s_obj_to_json` can be used?
        # `io.argoproj.workflow.v1alpha1.Template` properties
        self.node_selector = {}
        self.volumes = []
        self.tolerations = []
        self.affinity = {}
        self.pod_annotations = {}
        self.pod_labels = {}

        # Retry strategy
        self.num_retries = 0
        self.retry_policy = None
        self.backoff_factor = None
        self.backoff_duration = None
        self.backoff_max_duration = None

        self.timeout = 0
        self.init_containers = init_containers or []
        self.sidecars = sidecars or []

        # used to mark this op with loop arguments
        self.loop_args = None

        # Placeholder for inputs when adding ComponentSpec metadata to this
        # ContainerOp. This holds inputs defined in ComponentSpec that have
        # a corresponding PipelineParam.
        self._component_spec_inputs_with_pipeline_params = []

        # attributes specific to `BaseOp`
        self._inputs = []
        self.dependent_names = []

        # Caching option, default to True
        self.enable_caching = True

    @property
    def inputs(self):
        """List of PipelineParams that will be converted into input parameters
        (io.argoproj.workflow.v1alpha1.Inputs) for the argo workflow."""
        # Iterate through and extract all the `PipelineParam` in Op when
        # called the 1st time (because there are in-place updates to `PipelineParam`
        # during compilation - remove in-place updates for easier debugging?)
        if not self._inputs:
            self._inputs = self._component_spec_inputs_with_pipeline_params or []
            # TODO replace with proper k8s obj?
            for key in self.attrs_with_pipelineparams:
                self._inputs += _pipeline_param.extract_pipelineparams_from_any(
                    getattr(self, key))
            # keep only unique
            self._inputs = list(set(self._inputs))
        return self._inputs

    @inputs.setter
    def inputs(self, value):
        # to support in-place updates
        self._inputs = value

    def apply(self, mod_func):
        """Applies a modifier function to self.

        The function should return the passed object.
        This is needed to chain "extention methods" to this class.

        Example::

          from kfp.gcp import use_gcp_secret
          task = (
              train_op(...)
                  .set_memory_request('1G')
                  .apply(use_gcp_secret('user-gcp-sa'))
                  .set_memory_limit('2G')
          )
        """
        return mod_func(self) or self

    def after(self, *ops):
        """Specify explicit dependency on other ops."""
        for op in ops:
            self.dependent_names.append(op.name)
        return self

    def add_volume(self, volume):
        """Add K8s volume to the container.

        Args:
          volume: Kubernetes volumes For detailed spec, check volume definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_volume.py
        """
        self.volumes.append(volume)
        return self

    def add_toleration(self, tolerations: V1Toleration):
        """Add K8s tolerations.

        Args:
          tolerations: Kubernetes toleration For detailed spec, check toleration
            definition
            https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_toleration.py
        """
        self.tolerations.append(tolerations)
        return self

    def add_affinity(self, affinity: V1Affinity):
        """Add K8s Affinity.

        Args:
          affinity: Kubernetes affinity For detailed spec, check affinity
            definition
          https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/v1_affinity.py

        Example::

          V1Affinity(
              node_affinity=V1NodeAffinity(
                  required_during_scheduling_ignored_during_execution=V1NodeSelector(
                      node_selector_terms=[V1NodeSelectorTerm(
                          match_expressions=[V1NodeSelectorRequirement(
                              key='beta.kubernetes.io/instance-type',
                              operator='In',
                              values=['p2.xlarge'])])])))
        """
        self.affinity = affinity
        return self

    def add_node_selector_constraint(
            self, label_name: Union[str, _pipeline_param.PipelineParam],
            value: Union[str, _pipeline_param.PipelineParam]):
        """Add a constraint for nodeSelector.

        Each constraint is a key-value pair label.
        For the container to be eligible to run on a node, the node must have each
        of the constraints appeared as labels.

        Args:
          label_name(Union[str, PipelineParam]): The name of the constraint label.
          value(Union[str, PipelineParam]): The value of the constraint label.
        """

        self.node_selector[label_name] = value
        return self

    def add_pod_annotation(self, name: str, value: str):
        """Adds a pod's metadata annotation.

        Args:
          name: The name of the annotation.
          value: The value of the annotation.
        """

        self.pod_annotations[name] = value
        return self

    def add_pod_label(self, name: str, value: str):
        """Adds a pod's metadata label.

        Args:
          name: The name of the label.
          value: The value of the label.
        """

        self.pod_labels[name] = value
        return self

    def set_retry(self,
                  num_retries: int,
                  policy: Optional[str] = None,
                  backoff_duration: Optional[str] = None,
                  backoff_factor: Optional[float] = None,
                  backoff_max_duration: Optional[str] = None):
        """Sets the number of times the task is retried until it's declared
        failed.

        Args:
          num_retries: Number of times to retry on failures.
          policy: Retry policy name.
          backoff_duration: The time interval between retries. Defaults to an
            immediate retry. In case you specify a simple number, the unit
            defaults to seconds. You can also specify a different unit, for
            instance, 2m (2 minutes), 1h (1 hour).
          backoff_factor: The exponential backoff factor applied to
            backoff_duration. For example, if backoff_duration="60"
            (60 seconds) and backoff_factor=2, the first retry will happen
            after 60 seconds, then after 120, 240, and so on.
          backoff_max_duration: The maximum interval that can be reached with
            the backoff strategy.
        """
        if policy is not None and policy not in ALLOWED_RETRY_POLICIES:
            raise ValueError('policy must be one of: %r' %
                             (ALLOWED_RETRY_POLICIES,))

        self.num_retries = num_retries
        self.retry_policy = policy
        self.backoff_factor = backoff_factor
        self.backoff_duration = backoff_duration
        self.backoff_max_duration = backoff_max_duration
        return self

    def set_timeout(self, seconds: int):
        """Sets the timeout for the task in seconds.

        Args:
          seconds: Number of seconds.
        """

        self.timeout = seconds
        return self

    def add_init_container(self, init_container: UserContainer):
        """Add a init container to the Op.

        Args:
          init_container: UserContainer object.
        """

        self.init_containers.append(init_container)
        return self

    def add_sidecar(self, sidecar: Sidecar):
        """Add a sidecar to the Op.

        Args:
          sidecar: SideCar object.
        """

        self.sidecars.append(sidecar)
        return self

    def set_display_name(self, name: str):
        self.display_name = name
        return self

    def set_caching_options(self, enable_caching: bool) -> 'BaseOp':
        """Sets caching options for the Op.

        Args:
          enable_caching: Whether or not to enable caching for this task.

        Returns:
          Self return to allow chained setting calls.
        """
        self.enable_caching = enable_caching
        return self

    def __repr__(self):
        return str({self.__class__.__name__: self.__dict__})


from ._pipeline_volume import \
    PipelineVolume  # The import is here to prevent circular reference problems.


class InputArgumentPath:

    def __init__(self, argument, input=None, path=None):
        self.argument = argument
        self.input = input
        self.path = path


def _is_legacy_output_name(output_name: str) -> Tuple[bool, str]:
    normalized_output_name = re.sub('[^a-zA-Z0-9]', '-', output_name.lower())
    if normalized_output_name in [
            'mlpipeline-ui-metadata',
            'mlpipeline-metrics',
    ]:
        return True, normalized_output_name
    return False, output_name


class ContainerOp(BaseOp):
    """Represents an op implemented by a container image.

    Args:
      name: the name of the op. It does not have to be unique within a
        pipeline because the pipeline will generates a unique new name in case
        of conflicts.
      image: the container image name, such as 'python:3.5-jessie'
      command: the command to run in the container. If None, uses default CMD
        in defined in container.
      arguments: the arguments of the command. The command can include "%s"
        and supply a PipelineParam as the string replacement. For example,
        ('echo %s' % input_param). At container run time the argument will be
        'echo param_value'.
      init_containers: the list of `UserContainer` objects describing the
        InitContainer to deploy before the `main` container.
      sidecars: the list of `Sidecar` objects describing the sidecar
        containers to deploy together with the `main` container.
      container_kwargs: the dict of additional keyword arguments to pass to
        the op's `Container` definition.
      artifact_argument_paths: Optional. Maps input artifact arguments (values
        or references) to the local file paths where they'll be placed. At
        pipeline run time, the value of the artifact argument is saved to a
        local file with specified path. This parameter is only needed when the
        input file paths are hard-coded in the program. Otherwise it's better
        to pass input artifact placement paths by including artifact arguments
        in the command-line using the InputArgumentPath class instances.
      file_outputs: Maps output names to container local output file paths.
        The system will take the data from those files and will make it
        available for passing to downstream tasks. For each output in the
        file_outputs map there will be a corresponding output reference
        available in the task.outputs dictionary. These output references can
        be passed to the other tasks as arguments. The following output names
        are handled specially by the frontend and
            backend: "mlpipeline-ui-metadata" and "mlpipeline-metrics".
      output_artifact_paths: Deprecated. Maps output artifact labels to local
        artifact file paths. Deprecated: Use file_outputs instead. It now
          supports big data outputs.
      is_exit_handler: Deprecated. This is no longer needed.
      pvolumes: Dictionary for the user to match a path on the op's fs with a
        V1Volume or it inherited type.
          E.g {"/my/path": vol, "/mnt": other_op.pvolumes["/output"]}.

    Example::

      from kfp import dsl
      from kubernetes.client.models import V1EnvVar, V1SecretKeySelector
      @dsl.pipeline(
          name='foo',
          description='hello world')
      def foo_pipeline(tag: str, pull_image_policy: str):
        # any attributes can be parameterized (both serialized string or actual PipelineParam)
        op = dsl.ContainerOp(name='foo',
                            image='busybox:%s' % tag,
                            # pass in init_container list
                            init_containers=[dsl.UserContainer('print', 'busybox:latest', command='echo "hello"')],
                            # pass in sidecars list
                            sidecars=[dsl.Sidecar('print', 'busybox:latest', command='echo "hello"')],
                            # pass in k8s container kwargs
                            container_kwargs={'env': [V1EnvVar('foo', 'bar')]},
        )
        # set `imagePullPolicy` property for `container` with `PipelineParam`
        op.container.set_image_pull_policy(pull_image_policy)
        # add sidecar with parameterized image tag
        # sidecar follows the argo sidecar swagger spec
        op.add_sidecar(dsl.Sidecar('redis', 'redis:%s' % tag).set_image_pull_policy('Always'))
    """

    # list of attributes that might have pipeline params - used to generate
    # the input parameters during compilation.
    # Excludes `file_outputs` and `outputs` as they are handled separately
    # in the compilation process to generate the DAGs and task io parameters.

    _DISABLE_REUSABLE_COMPONENT_WARNING = False

    def __init__(
        self,
        name: str,
        image: str,
        command: Optional[StringOrStringList] = None,
        arguments: Optional[ArgumentOrArguments] = None,
        init_containers: Optional[List[UserContainer]] = None,
        sidecars: Optional[List[Sidecar]] = None,
        container_kwargs: Optional[Dict] = None,
        artifact_argument_paths: Optional[List[InputArgumentPath]] = None,
        file_outputs: Optional[Dict[str, str]] = None,
        output_artifact_paths: Optional[Dict[str, str]] = None,
        is_exit_handler: bool = False,
        pvolumes: Optional[Dict[str, V1Volume]] = None,
    ):
        super().__init__(
            name=name,
            init_containers=init_containers,
            sidecars=sidecars,
            is_exit_handler=is_exit_handler)

        self.attrs_with_pipelineparams = BaseOp.attrs_with_pipelineparams + [
            '_container', 'artifact_arguments', '_parameter_arguments'
        ]  #Copying the BaseOp class variable!

        input_artifact_paths = {}
        artifact_arguments = {}
        file_outputs = dict(file_outputs or {})  # Making a copy
        output_artifact_paths = dict(output_artifact_paths or
                                     {})  # Making a copy

        self._is_v2 = False

        def resolve_artifact_argument(artarg):
            from ..components._components import _generate_input_file_name
            if not isinstance(artarg, InputArgumentPath):
                return artarg
            input_name = getattr(
                artarg.input, 'name',
                artarg.input) or ('input-' + str(len(artifact_arguments)))
            input_path = artarg.path or _generate_input_file_name(input_name)
            input_artifact_paths[input_name] = input_path
            artifact_arguments[input_name] = str(artarg.argument)
            return input_path

        for artarg in artifact_argument_paths or []:
            resolve_artifact_argument(artarg)

        if isinstance(command, Sequence) and not isinstance(command, str):
            command = list(map(resolve_artifact_argument, command))
        if isinstance(arguments, Sequence) and not isinstance(arguments, str):
            arguments = list(map(resolve_artifact_argument, arguments))

        # convert to list if not a list
        command = as_string_list(command)
        arguments = as_string_list(arguments)

        if (not ContainerOp._DISABLE_REUSABLE_COMPONENT_WARNING) and (
                '--component_launcher_class_path' not in (arguments or [])):
            # The warning is suppressed for pipelines created using the TFX SDK.
            warnings.warn(
                'Please create reusable components instead of constructing ContainerOp instances directly.'
                ' Reusable components are shareable, portable and have compatibility and support guarantees.'
                ' Please see the documentation: https://www.kubeflow.org/docs/pipelines/sdk/component-development/#writing-your-component-definition-file'
                ' The components can be created manually (or, in case of python, using kfp.components.create_component_from_func or func_to_container_op)'
                ' and then loaded using kfp.components.load_component_from_file, load_component_from_uri or load_component_from_text: '
                'https://kubeflow-pipelines.readthedocs.io/en/stable/source/kfp.components.html#kfp.components.load_component_from_file',
                category=FutureWarning,
            )
            if kfp.COMPILING_FOR_V2:
                raise RuntimeError(
                    'Constructing ContainerOp instances directly is deprecated and not '
                    'supported when compiling to v2 (using v2 compiler or v1 compiler '
                    'with V2_COMPATIBLE or V2_ENGINE mode).')

        # `container` prop in `io.argoproj.workflow.v1alpha1.Template`
        container_kwargs = container_kwargs or {}
        self._container = Container(
            image=image, args=arguments, command=command, **container_kwargs)

        # NOTE for backward compatibility (remove in future?)
        # proxy old ContainerOp callables to Container

        # attributes to NOT proxy
        ignore_set = frozenset(['to_dict', 'to_str'])

        # decorator func to proxy a method in `Container` into `ContainerOp`
        def _proxy(proxy_attr):
            """Decorator func to proxy to ContainerOp.container."""

            def _decorated(*args, **kwargs):
                # execute method
                ret = getattr(self._container, proxy_attr)(*args, **kwargs)
                if ret == self._container:
                    return self
                return ret

            return deprecation_warning(_decorated, proxy_attr, proxy_attr)

        # iter thru container and attach a proxy func to the container method
        for attr_to_proxy in dir(self._container):
            func = getattr(self._container, attr_to_proxy)
            # ignore private methods, and bypass method overrided by subclasses.
            if (not hasattr(self, attr_to_proxy) and
                    hasattr(func, '__call__') and (attr_to_proxy[0] != '_') and
                (attr_to_proxy not in ignore_set)):
                # only proxy public callables
                setattr(self, attr_to_proxy, _proxy(attr_to_proxy))

        if output_artifact_paths:
            warnings.warn(
                'The output_artifact_paths parameter is deprecated since SDK v0.1.32. '
                'Use the file_outputs parameter instead. file_outputs now supports '
                'outputting big data.', DeprecationWarning)

        # Skip the special handling that is unnecessary in v2.
        if not kfp.COMPILING_FOR_V2:
            # Special handling for the mlpipeline-ui-metadata and mlpipeline-metrics
            # outputs that should always be saved as artifacts
            # TODO: Remove when outputs are always saved as artifacts
            for output_name, path in dict(file_outputs).items():
                is_legacy_name, normalized_name = _is_legacy_output_name(
                    output_name)
                if is_legacy_name:
                    output_artifact_paths[normalized_name] = path
                    del file_outputs[output_name]

        # attributes specific to `ContainerOp`
        self.input_artifact_paths = input_artifact_paths
        self.artifact_arguments = artifact_arguments
        self.file_outputs = file_outputs
        self.output_artifact_paths = output_artifact_paths or {}

        self._metadata = None
        self._parameter_arguments = None

        self.execution_options = _structures.ExecutionOptionsSpec(
            caching_strategy=_structures.CachingStrategySpec(),)

        self.outputs = {}
        if file_outputs:
            self.outputs = {
                name: _pipeline_param.PipelineParam(name, op_name=self.name)
                for name in file_outputs.keys()
            }

        self._set_single_output_attribute()

        self.pvolumes = {}
        self.add_pvolumes(pvolumes)

    def _set_single_output_attribute(self):
        # Syntactic sugar: Add task.output attribute if the component has a single
        # output.
        # TODO: Currently the "MLPipeline UI Metadata" output is removed from
        # outputs to preserve backwards compatibility. Maybe stop excluding it from
        # outputs, but rather exclude it from unique_outputs.
        unique_outputs = set(self.outputs.values())
        if len(unique_outputs) == 1:
            self.output = list(unique_outputs)[0]
        else:
            self.output = _MultipleOutputsError()

    @property
    def is_v2(self):
        return self._is_v2

    @is_v2.setter
    def is_v2(self, is_v2: bool):
        self._is_v2 = is_v2

    # v2 container spec
    @property
    def container_spec(self):
        return self._container._container_spec

    @container_spec.setter
    def container_spec(self, spec: _PipelineContainerSpec):
        if not isinstance(spec, _PipelineContainerSpec):
            raise TypeError('container_spec can only be PipelineContainerSpec. '
                            'Got: {}'.format(spec))
        self._container._container_spec = spec

    @property
    def command(self):
        return self._container.command

    @command.setter
    def command(self, value):
        self._container.command = as_string_list(value)

    @property
    def arguments(self):
        return self._container.args

    @arguments.setter
    def arguments(self, value):
        self._container.args = as_string_list(value)

    @property
    def container(self):
        """`Container` object that represents the `container` property in
        `io.argoproj.workflow.v1alpha1.Template`. Can be used to update the
        container configurations.

        Example::

          import kfp.dsl as dsl
          from kubernetes.client.models import V1EnvVar

          @dsl.pipeline(name='example_pipeline')
          def immediate_value_pipeline():
            op1 = (dsl.ContainerOp(name='example', image='nginx:alpine')
                    .container
                        .add_env_variable(V1EnvVar(name='HOST',
                        value='foo.bar'))
                        .add_env_variable(V1EnvVar(name='PORT', value='80'))
                        .parent # return the parent `ContainerOp`
                    )
        """
        return self._container

    def _set_metadata(self,
                      metadata,
                      arguments: Optional[Dict[str, Any]] = None):
        """Passes the ContainerOp the metadata information and configures the
        right output.

        Args:
          metadata (ComponentSpec): component metadata
          arguments: Dictionary of input arguments to the component.
        """
        if not isinstance(metadata, _structures.ComponentSpec):
            raise ValueError('_set_metadata is expecting ComponentSpec.')

        self._metadata = metadata

        if self._metadata.outputs:
            declared_outputs = {
                output.name:
                _pipeline_param.PipelineParam(output.name, op_name=self.name)
                for output in self._metadata.outputs
            }
            self.outputs.update(declared_outputs)

            for output in self._metadata.outputs:
                if output.name in self.file_outputs:
                    continue
                is_legacy_name, normalized_name = _is_legacy_output_name(
                    output.name)
                if is_legacy_name and normalized_name in self.output_artifact_paths:
                    output_filename = self.output_artifact_paths[normalized_name]
                else:
                    output_filename = _components._generate_output_file_name(
                        output.name)
                self.file_outputs[output.name] = output_filename

            if not kfp.COMPILING_FOR_V2:
                for output_name, path in dict(self.file_outputs).items():
                    is_legacy_name, normalized_name = _is_legacy_output_name(
                        output_name)
                    if is_legacy_name:
                        self.output_artifact_paths[normalized_name] = path
                        del self.file_outputs[output_name]
                        del self.outputs[output_name]

        if arguments is not None:
            for input_name, value in arguments.items():
                self.artifact_arguments[input_name] = str(value)
                if (isinstance(value, _pipeline_param.PipelineParam)):
                    self._component_spec_inputs_with_pipeline_params.append(
                        value)

                if input_name not in self.input_artifact_paths:
                    input_artifact_path = _components._generate_input_file_name(
                        input_name)
                    self.input_artifact_paths[input_name] = input_artifact_path

        if self.file_outputs:
            for output in self.file_outputs.keys():
                output_type = self.outputs[output].param_type
                for output_meta in self._metadata.outputs:
                    if output_meta.name == output:
                        output_type = output_meta.type
                self.outputs[output].param_type = output_type

        self._set_single_output_attribute()

    def add_pvolumes(self, pvolumes: Dict[str, V1Volume] = None):
        """Updates the existing pvolumes dict, extends volumes and
        volume_mounts and redefines the pvolume attribute.

        Args:
          pvolumes: Dictionary. Keys are mount paths, values are Kubernetes
            volumes or inherited types (e.g. PipelineVolumes).
        """
        if pvolumes:
            for mount_path, pvolume in pvolumes.items():
                if hasattr(pvolume, 'dependent_names'):
                    self.dependent_names.extend(pvolume.dependent_names)
                else:
                    pvolume = PipelineVolume(volume=pvolume)
                pvolume = pvolume.after(self)
                self.pvolumes[mount_path] = pvolume
                self.add_volume(pvolume)
                self._container.add_volume_mount(
                    V1VolumeMount(name=pvolume.name, mount_path=mount_path))

        self.pvolume = None
        if len(self.pvolumes) == 1:
            self.pvolume = list(self.pvolumes.values())[0]
        return self

    def add_node_selector_constraint(
            self, label_name: Union[str, _pipeline_param.PipelineParam],
            value: Union[str, _pipeline_param.PipelineParam]) -> 'ContainerOp':
        """Sets accelerator type requirement for this task.

        When compiling for v2, this function can be optionally used with
        set_gpu_limit to set the number of accelerator required. Otherwise, by
        default the number requested will be 1.

        Args:
          label_name: The name of the constraint label.
            For v2, only 'cloud.google.com/gke-accelerator' is supported now.
          value: The name of the accelerator.
            For v2, available values include 'nvidia-tesla-k80', 'tpu-v3'.

        Returns:
          self return to allow chained call with other resource specification.
        """
        if self.container_spec and not (
                isinstance(label_name, _pipeline_param.PipelineParam) or
                isinstance(value, _pipeline_param.PipelineParam)):
            accelerator_cnt = 1
            if self.container_spec.resources.accelerator.count > 1:
                # Reserve the number if already set.
                accelerator_cnt = self.container_spec.resources.accelerator.count

            accelerator_config = _PipelineContainerSpec.ResourceSpec.AcceleratorConfig(
                type=_sanitize_gpu_type(value), count=accelerator_cnt)
            self.container_spec.resources.accelerator.CopyFrom(
                accelerator_config)

        super(ContainerOp, self).add_node_selector_constraint(label_name, value)
        return self


# proxy old ContainerOp properties to ContainerOp.container
# with PendingDeprecationWarning.
ContainerOp = _proxy_container_op_props(ContainerOp)


class _MultipleOutputsError:

    @staticmethod
    def raise_error():
        raise RuntimeError(
            'This task has multiple outputs. Use `task.outputs[<output name>]` '
            'dictionary to refer to the one you need.')

    def __getattribute__(self, name):
        _MultipleOutputsError.raise_error()

    def __str__(self):
        _MultipleOutputsError.raise_error()


def _get_cpu_number(cpu_string: str) -> float:
    """Converts the cpu string to number of vCPU core."""
    # dsl.ContainerOp._validate_cpu_string guaranteed that cpu_string is either
    # 1) a string can be converted to a float; or
    # 2) a string followed by 'm', and it can be converted to a float.
    if cpu_string.endswith('m'):
        return float(cpu_string[:-1]) / 1000
    else:
        return float(cpu_string)


def _get_resource_number(resource_string: str) -> float:
    """Converts the resource string to number of resource in GB."""
    # dsl.ContainerOp._validate_size_string guaranteed that memory_string
    # represents an integer, optionally followed by one of (E, Ei, P, Pi, T, Ti,
    # G, Gi, M, Mi, K, Ki).
    # See the meaning of different suffix at
    # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory
    # Also, ResourceSpec in pipeline IR expects a number in GB.
    if resource_string.endswith('E'):
        return float(resource_string[:-1]) * _E / _G
    elif resource_string.endswith('Ei'):
        return float(resource_string[:-2]) * _EI / _G
    elif resource_string.endswith('P'):
        return float(resource_string[:-1]) * _P / _G
    elif resource_string.endswith('Pi'):
        return float(resource_string[:-2]) * _PI / _G
    elif resource_string.endswith('T'):
        return float(resource_string[:-1]) * _T / _G
    elif resource_string.endswith('Ti'):
        return float(resource_string[:-2]) * _TI / _G
    elif resource_string.endswith('G'):
        return float(resource_string[:-1])
    elif resource_string.endswith('Gi'):
        return float(resource_string[:-2]) * _GI / _G
    elif resource_string.endswith('M'):
        return float(resource_string[:-1]) * _M / _G
    elif resource_string.endswith('Mi'):
        return float(resource_string[:-2]) * _MI / _G
    elif resource_string.endswith('K'):
        return float(resource_string[:-1]) * _K / _G
    elif resource_string.endswith('Ki'):
        return float(resource_string[:-2]) * _KI / _G
    else:
        # By default interpret as a plain integer, in the unit of Bytes.
        return float(resource_string) / _G


def _sanitize_gpu_type(gpu_type: str) -> str:
    """Converts the GPU type to conform the enum style."""
    return gpu_type.replace('-', '_').upper()
