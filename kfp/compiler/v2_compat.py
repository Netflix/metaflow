# Copyright 2021 The Kubeflow Authors
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
"""Utility functions for enabling v2-compatible pipelines in v1."""
import collections
import json
from typing import Optional

from kfp import dsl
from kfp.compiler import _default_transformers
from kfp.pipeline_spec import pipeline_spec_pb2
from kfp.v2 import compiler
from kubernetes import client as k8s_client

_DEFAULT_LAUNCHER_IMAGE = "gcr.io/ml-pipeline/kfp-launcher:1.8.7"


def update_op(op: dsl.ContainerOp,
              pipeline_name: dsl.PipelineParam,
              pipeline_root: dsl.PipelineParam,
              launcher_image: Optional[str] = None) -> None:
    """Updates the passed in Op for running in v2-compatible mode.

    Args:
      op: The Op to update.
      pipeline_spec: The PipelineSpec for the pipeline under which `op`
        runs.
      pipeline_root: The root output directory for pipeline artifacts.
      launcher_image: An optional launcher image. Useful for tests.
    """
    op.is_v2 = True
    # Inject the launcher binary and overwrite the entrypoint.
    image_name = launcher_image or _DEFAULT_LAUNCHER_IMAGE
    launcher_container = dsl.UserContainer(
        name="kfp-launcher",
        image=image_name,
        command=["launcher", "--copy", "/kfp-launcher/launch"],
        mirror_volume_mounts=True)

    op.add_init_container(launcher_container)
    op.add_volume(k8s_client.V1Volume(name='kfp-launcher'))
    op.add_volume_mount(
        k8s_client.V1VolumeMount(
            name='kfp-launcher', mount_path='/kfp-launcher'))

    # op.command + op.args will have the following sections:
    # 1. args passed to kfp-launcher
    # 2. a separator "--"
    # 3. parameters in format "key1=value1", "key2=value2", ...
    # 4. a separator "--" as end of arguments passed to launcher
    # 5. (start of op.args) arguments of the original user program command + args
    #
    # example:
    # - command:
    # - /kfp-launcher/launch
    # - '--mlmd_server_address'
    # - $(METADATA_GRPC_SERVICE_HOST)
    # - '--mlmd_server_port'
    # - $(METADATA_GRPC_SERVICE_PORT)
    # - ... # more launcher params
    # - '--pipeline_task_id'
    # - $(KFP_POD_NAME)
    # - '--pipeline_root'
    # - ''
    # - '--' # start of parameter values
    # - first=first
    # - second=second
    # - '--' # start of user command and args
    # args:
    # - sh
    # - '-ec'
    # - |
    #     program_path=$(mktemp)
    #     printf "%s" "$0" > "$program_path"
    #     python3 -u "$program_path" "$@"
    # - >
    #     import json
    #     import xxx
    #     ...
    op.command = [
        "/kfp-launcher/launch",
        "--mlmd_server_address",
        "$(METADATA_GRPC_SERVICE_HOST)",
        "--mlmd_server_port",
        "$(METADATA_GRPC_SERVICE_PORT)",
        "--runtime_info_json",
        "$(KFP_V2_RUNTIME_INFO)",
        "--container_image",
        "$(KFP_V2_IMAGE)",
        "--task_name",
        op.name,
        "--pipeline_name",
        pipeline_name,
        "--run_id",
        "$(KFP_RUN_ID)",
        "--run_resource",
        "workflows.argoproj.io/$(WORKFLOW_ID)",
        "--namespace",
        "$(KFP_NAMESPACE)",
        "--pod_name",
        "$(KFP_POD_NAME)",
        "--pod_uid",
        "$(KFP_POD_UID)",
        "--pipeline_root",
        pipeline_root,
        "--enable_caching",
        "$(ENABLE_CACHING)",
    ]

    # Mount necessary environment variables.
    op.apply(_default_transformers.add_kfp_pod_env)
    op.container.add_env_variable(
        k8s_client.V1EnvVar(name="KFP_V2_IMAGE", value=op.container.image))

    config_map_ref = k8s_client.V1ConfigMapEnvSource(
        name='metadata-grpc-configmap', optional=True)
    op.container.add_env_from(
        k8s_client.V1EnvFromSource(config_map_ref=config_map_ref))

    op.arguments = list(op.container_spec.command) + list(
        op.container_spec.args)

    runtime_info = {
        "inputParameters": collections.OrderedDict(),
        "inputArtifacts": collections.OrderedDict(),
        "outputParameters": collections.OrderedDict(),
        "outputArtifacts": collections.OrderedDict(),
    }

    op.command += ["--"]
    component_spec = op.component_spec
    for parameter, spec in sorted(
            component_spec.input_definitions.parameters.items()):
        parameter_info = {
            "type":
                pipeline_spec_pb2.PrimitiveType.PrimitiveTypeEnum.Name(spec.type
                                                                      ),
        }
        op.command += [f"{parameter}={op._parameter_arguments[parameter]}"]
        runtime_info["inputParameters"][parameter] = parameter_info
    op.command += ["--"]

    for artifact_name, spec in sorted(
            component_spec.input_definitions.artifacts.items()):
        artifact_info = {
            "metadataPath": op.input_artifact_paths[artifact_name],
            "schemaTitle": spec.artifact_type.schema_title,
            "instanceSchema": spec.artifact_type.instance_schema,
            "schemaVersion": spec.artifact_type.schema_version,
        }
        runtime_info["inputArtifacts"][artifact_name] = artifact_info

    for parameter, spec in sorted(
            component_spec.output_definitions.parameters.items()):
        parameter_info = {
            "type":
                pipeline_spec_pb2.PrimitiveType.PrimitiveTypeEnum.Name(spec.type
                                                                      ),
            "path":
                op.file_outputs[parameter],
        }
        runtime_info["outputParameters"][parameter] = parameter_info

    for artifact_name, spec in sorted(
            component_spec.output_definitions.artifacts.items()):
        # TODO: Assert instance_schema.
        artifact_info = {
            # Type used to register output artifacts.
            "schemaTitle": spec.artifact_type.schema_title,
            "instanceSchema": spec.artifact_type.instance_schema,
            "schemaVersion": spec.artifact_type.schema_version,
            # File used to write out the registered artifact ID.
            "metadataPath": op.file_outputs[artifact_name],
        }
        runtime_info["outputArtifacts"][artifact_name] = artifact_info

    op.container.add_env_variable(
        k8s_client.V1EnvVar(
            name="KFP_V2_RUNTIME_INFO", value=json.dumps(runtime_info)))

    op.pod_annotations['pipelines.kubeflow.org/v2_component'] = "true"
    op.pod_labels['pipelines.kubeflow.org/v2_component'] = "true"
