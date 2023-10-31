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

from typing import Dict
from kubernetes.client.models import (V1Volume, V1TypedLocalObjectReference,
                                      V1ObjectMeta)

from ._resource_op import ResourceOp
from ._pipeline_param import match_serialized_pipelineparam, sanitize_k8s_name


class VolumeSnapshotOp(ResourceOp):
    """Represents an op which will be translated into a resource template which
    will be creating a VolumeSnapshot.

    TODO(https://github.com/kubeflow/pipelines/issues/4822): Determine the
        stability level of this feature.

    Args:
      resource_name: A desired name for the VolumeSnapshot which will be
        created
      pvc: The name of the PVC which will be snapshotted
      snapshot_class: The snapshot class to use for the dynamically created
        VolumeSnapshot
      annotations: Annotations to be patched in the VolumeSnapshot
      volume: An instance of V1Volume
      kwargs: See :py:class:`kfp.dsl.ResourceOp`

    Raises:
      ValueError: if k8s_resource is provided along with other arguments
                  if k8s_resource is not a VolumeSnapshot
                  if pvc and volume are None
                  if pvc and volume are not None
                  if volume does not reference a PVC
    """

    def __init__(self,
                 resource_name: str = None,
                 pvc: str = None,
                 snapshot_class: str = None,
                 annotations: Dict[str, str] = None,
                 volume: V1Volume = None,
                 api_version: str = "snapshot.storage.k8s.io/v1alpha1",
                 **kwargs):
        # Add size to output params
        self.attribute_outputs = {"size": "{.status.restoreSize}"}
        # Add default success_condition if None provided
        if "success_condition" not in kwargs:
            kwargs["success_condition"] = "status.readyToUse == true"

        if "k8s_resource" in kwargs:
            if resource_name or pvc or snapshot_class or annotations or volume:
                raise ValueError("You cannot provide k8s_resource along with "
                                 "other arguments.")
            # TODO: Check if is VolumeSnapshot
            super().__init__(**kwargs)
            self.snapshot = V1TypedLocalObjectReference(
                api_group="snapshot.storage.k8s.io",
                kind="VolumeSnapshot",
                name=self.outputs["name"])
            return

        if not (pvc or volume):
            raise ValueError("You must provide a pvc or a volume.")
        elif pvc and volume:
            raise ValueError("You can't provide both pvc and volume.")

        source = None
        deps = []
        if pvc:
            source = V1TypedLocalObjectReference(
                kind="PersistentVolumeClaim", name=pvc)
        else:
            if not hasattr(volume, "persistent_volume_claim"):
                raise ValueError("The volume must be referencing a PVC.")
            if hasattr(volume,
                       "dependent_names"):  #TODO: Replace with type check
                deps = list(volume.dependent_names)
            source = V1TypedLocalObjectReference(
                kind="PersistentVolumeClaim",
                name=volume.persistent_volume_claim.claim_name)

        # Set the k8s_resource
        # TODO: Use VolumeSnapshot
        if not match_serialized_pipelineparam(str(resource_name)):
            resource_name = sanitize_k8s_name(resource_name)
        snapshot_metadata = V1ObjectMeta(
            name="{{workflow.name}}-%s" % resource_name,
            annotations=annotations)
        k8s_resource = {
            "apiVersion": api_version,
            "kind": "VolumeSnapshot",
            "metadata": snapshot_metadata,
            "spec": {
                "source": source
            }
        }
        if snapshot_class:
            k8s_resource["spec"]["snapshotClassName"] = snapshot_class

        super().__init__(k8s_resource=k8s_resource, **kwargs)
        self.dependent_names.extend(deps)
        self.snapshot = V1TypedLocalObjectReference(
            api_group="snapshot.storage.k8s.io",
            kind="VolumeSnapshot",
            name=self.outputs["name"])
