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
from typing import List, Dict
from kubernetes.client.models import (V1ObjectMeta, V1ResourceRequirements,
                                      V1PersistentVolumeClaimSpec,
                                      V1PersistentVolumeClaim,
                                      V1TypedLocalObjectReference)

from ._resource_op import ResourceOp
from ._pipeline_param import (PipelineParam, match_serialized_pipelineparam,
                              sanitize_k8s_name)
from ._pipeline_volume import PipelineVolume

VOLUME_MODE_RWO = ["ReadWriteOnce"]
VOLUME_MODE_RWM = ["ReadWriteMany"]
VOLUME_MODE_ROM = ["ReadOnlyMany"]


class VolumeOp(ResourceOp):
    """Represents an op which will be translated into a resource template which
    will be creating a PVC.

    TODO(https://github.com/kubeflow/pipelines/issues/4822): Determine the
        stability level of this feature.

    Args:
      resource_name: A desired name for the PVC which will be created
      size: The size of the PVC which will be created
      storage_class: The storage class to use for the dynamically created PVC
      modes: The access modes for the PVC
      annotations: Annotations to be patched in the PVC
      data_source: May be a V1TypedLocalObjectReference, and then it is used
        in the data_source field of the PVC as is. Can also be a
        string/PipelineParam, and in that case it will be used as a
        VolumeSnapshot name (Alpha feature)
      volume_name: VolumeName is the binding reference to the PersistentVolume
        backing this claim.
      generate_unique_name: Generate unique name for the PVC
      kwargs: See :py:class:`kfp.dsl.ResourceOp`

    Raises:
      ValueError: if k8s_resource is provided along with other arguments
                  if k8s_resource is not a V1PersistentVolumeClaim
                  if size is None
                  if size is an invalid memory string (when not a
                      PipelineParam)
                  if data_source is not one of (str, PipelineParam,
                      V1TypedLocalObjectReference)
    """

    def __init__(self,
                 resource_name: str = None,
                 size: str = None,
                 storage_class: str = None,
                 modes: List[str] = None,
                 annotations: Dict[str, str] = None,
                 data_source=None,
                 volume_name=None,
                 generate_unique_name: bool = True,
                 **kwargs):
        # Add size to attribute outputs
        self.attribute_outputs = {"size": "{.status.capacity.storage}"}

        if "k8s_resource" in kwargs:
            if resource_name or size or storage_class or modes or annotations:
                raise ValueError("You cannot provide k8s_resource along with "
                                 "other arguments.")
            if not isinstance(kwargs["k8s_resource"], V1PersistentVolumeClaim):
                raise ValueError("k8s_resource in VolumeOp must be an instance"
                                 " of V1PersistentVolumeClaim")
            super().__init__(**kwargs)
            self.volume = PipelineVolume(
                name=sanitize_k8s_name(self.name), pvc=self.outputs["name"])
            return

        if not size:
            raise ValueError("Please provide size")
        elif not match_serialized_pipelineparam(str(size)):
            self._validate_memory_string(size)

        if data_source and not isinstance(
                data_source, (str, PipelineParam, V1TypedLocalObjectReference)):
            raise ValueError("data_source can be one of (str, PipelineParam, "
                             "V1TypedLocalObjectReference).")
        if data_source and isinstance(data_source, (str, PipelineParam)):
            data_source = V1TypedLocalObjectReference(
                api_group="snapshot.storage.k8s.io",
                kind="VolumeSnapshot",
                name=data_source)

        # Set the k8s_resource
        if not match_serialized_pipelineparam(str(resource_name)):
            resource_name = sanitize_k8s_name(resource_name)
        pvc_metadata = V1ObjectMeta(
            name="{{workflow.name}}-%s" % resource_name if generate_unique_name else resource_name,
            annotations=annotations)
        requested_resources = V1ResourceRequirements(requests={"storage": size})
        pvc_spec = V1PersistentVolumeClaimSpec(
            access_modes=modes or VOLUME_MODE_RWM,
            resources=requested_resources,
            storage_class_name=storage_class,
            data_source=data_source,
            volume_name=volume_name)
        k8s_resource = V1PersistentVolumeClaim(
            api_version="v1",
            kind="PersistentVolumeClaim",
            metadata=pvc_metadata,
            spec=pvc_spec)

        super().__init__(
            k8s_resource=k8s_resource,
            **kwargs,
        )
        self.volume = PipelineVolume(
            name=sanitize_k8s_name(self.name), pvc=self.outputs["name"])

    def _validate_memory_string(self, memory_string):
        """Validate a given string is valid for memory request or limit."""
        if re.match(r"^[0-9]+(E|Ei|P|Pi|T|Ti|G|Gi|M|Mi|K|Ki){0,1}$",
                    memory_string) is None:
            raise ValueError("Invalid memory string. Should be an integer, " +
                             "or integer followed by one of " +
                             '"E|Ei|P|Pi|T|Ti|G|Gi|M|Mi|K|Ki"')
