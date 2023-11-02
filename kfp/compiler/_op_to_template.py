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

import json
import re
import warnings
import yaml
import copy
from collections import OrderedDict
from typing import Union, List, Any, Callable, TypeVar, Dict

from kfp.compiler._k8s_helper import convert_k8s_obj_to_json
from kfp import dsl
from kfp.dsl._container_op import BaseOp

# generics
T = TypeVar('T')


def _process_obj(obj: Any, map_to_tmpl_var: dict):
    """Recursively sanitize and replace any PipelineParam (instances and
    serialized strings) in the object with the corresponding template variables
    (i.e. '{{inputs.parameters.<PipelineParam.full_name>}}').

    Args:
      obj: any obj that may have PipelineParam
      map_to_tmpl_var: a dict that maps an unsanitized pipeline
                       params signature into a template var
    """
    # serialized str might be unsanitized
    if isinstance(obj, str):
        # get signature
        param_tuples = dsl.match_serialized_pipelineparam(obj)
        if not param_tuples:
            return obj
        # replace all unsanitized signature with template var
        for param_tuple in param_tuples:
            obj = re.sub(param_tuple.pattern,
                         map_to_tmpl_var[param_tuple.pattern], obj)

    # list
    if isinstance(obj, list):
        return [_process_obj(item, map_to_tmpl_var) for item in obj]

    # tuple
    if isinstance(obj, tuple):
        return tuple((_process_obj(item, map_to_tmpl_var) for item in obj))

    # dict
    if isinstance(obj, dict):
        return {
            _process_obj(key, map_to_tmpl_var):
            _process_obj(value, map_to_tmpl_var) for key, value in obj.items()
        }

    # pipelineparam
    if isinstance(obj, dsl.PipelineParam):
        # if not found in unsanitized map, then likely to be sanitized
        return map_to_tmpl_var.get(
            str(obj), '{{inputs.parameters.%s}}' % obj.full_name)

    # k8s objects (generated from swaggercodegen)
    if hasattr(obj, 'attribute_map') and isinstance(obj.attribute_map, dict):
        # process everything inside recursively
        for key in obj.attribute_map.keys():
            setattr(obj, key, _process_obj(getattr(obj, key), map_to_tmpl_var))
        # return json representation of the k8s obj
        return convert_k8s_obj_to_json(obj)

    # do nothing
    return obj


def _process_base_ops(op: BaseOp):
    """Recursively go through the attrs listed in `attrs_with_pipelineparams`
    and sanitize and replace pipeline params with template var string.

    Returns a processed `BaseOp`.

    NOTE this is an in-place update to `BaseOp`'s attributes (i.e. the ones
    specified in `attrs_with_pipelineparams`, all `PipelineParam` are replaced
    with the corresponding template variable strings).

    Args:
        op {BaseOp}: class that inherits from BaseOp

    Returns:
        BaseOp
    """

    # map param's (unsanitized pattern or serialized str pattern) -> input param var str
    map_to_tmpl_var = {(param.pattern or str(param)):
                       '{{inputs.parameters.%s}}' % param.full_name
                       for param in op.inputs}

    # process all attr with pipelineParams except inputs and outputs parameters
    for key in op.attrs_with_pipelineparams:
        setattr(op, key, _process_obj(getattr(op, key), map_to_tmpl_var))

    return op


def _parameters_to_json(params: List[dsl.PipelineParam]):
    """Converts a list of PipelineParam into an argo `parameter` JSON obj."""
    _to_json = (lambda param: dict(name=param.full_name, value=param.value)
                if param.value else dict(name=param.full_name))
    params = [_to_json(param) for param in params]
    # Sort to make the results deterministic.
    params.sort(key=lambda x: x['name'])
    return params


def _inputs_to_json(
    inputs_params: List[dsl.PipelineParam],
    input_artifact_paths: Dict[str, str] = None,
    artifact_arguments: Dict[str, str] = None,
) -> Dict[str, Dict]:
    """Converts a list of PipelineParam into an argo `inputs` JSON obj."""
    parameters = _parameters_to_json(inputs_params)

    # Building the input artifacts section
    artifacts = []
    for name, path in (input_artifact_paths or {}).items():
        artifact = {'name': name, 'path': path}
        if name in artifact_arguments:  # The arguments should be compiled as DAG task arguments, not template's default values, but in the current DSL-compiler implementation it's too hard to make that work when passing artifact references.
            artifact['raw'] = {'data': str(artifact_arguments[name])}
        artifacts.append(artifact)
    artifacts.sort(
        key=lambda x: x['name'])  #Stabilizing the input artifact ordering

    inputs_dict = {}
    if parameters:
        inputs_dict['parameters'] = parameters
    if artifacts:
        inputs_dict['artifacts'] = artifacts
    return inputs_dict


def _outputs_to_json(op: BaseOp, outputs: Dict[str, dsl.PipelineParam],
                     param_outputs: Dict[str,
                                         str], output_artifacts: List[dict]):
    """Creates an argo `outputs` JSON obj."""
    if isinstance(op, dsl.ResourceOp):
        value_from_key = "jsonPath"
    else:
        value_from_key = "path"
    output_parameters = []
    for param in set(outputs.values()):  # set() dedupes output references
        output_parameters.append({
            'name': param.full_name,
            'valueFrom': {
                value_from_key: param_outputs[param.name]
            }
        })
    output_parameters.sort(key=lambda x: x['name'])
    ret = {}
    if output_parameters:
        ret['parameters'] = output_parameters
    if output_artifacts:
        ret['artifacts'] = output_artifacts

    return ret


# TODO: generate argo python classes from swagger and use convert_k8s_obj_to_json??
def _op_to_template(op: BaseOp):
    """Generate template given an operator inherited from BaseOp."""

    # Display name
    if op.display_name:
        op.add_pod_annotation('pipelines.kubeflow.org/task_display_name',
                              op.display_name)

    # Caching option
    op.add_pod_label('pipelines.kubeflow.org/enable_caching',
                     str(op.enable_caching).lower())

    # NOTE in-place update to BaseOp
    # replace all PipelineParams with template var strings
    processed_op = _process_base_ops(op)

    if isinstance(op, dsl.ContainerOp):
        output_artifact_paths = OrderedDict(op.output_artifact_paths)
        # This should have been as easy as output_artifact_paths.update(op.file_outputs), but the _outputs_to_json function changes the output names and we must do the same here, so that the names are the same
        output_artifact_paths.update(
            sorted(((param.full_name, processed_op.file_outputs[param.name])
                    for param in processed_op.outputs.values()),
                   key=lambda x: x[0]))

        output_artifacts = [{
            'name': name,
            'path': path
        } for name, path in output_artifact_paths.items()]

        # workflow template
        template = {
            'name': processed_op.name,
            'container': convert_k8s_obj_to_json(processed_op.container)
        }
    elif isinstance(op, dsl.ResourceOp):
        # no output artifacts
        output_artifacts = []

        # workflow template
        processed_op.resource["manifest"] = yaml.dump(
            convert_k8s_obj_to_json(processed_op.k8s_resource),
            default_flow_style=False)
        template = {
            'name': processed_op.name,
            'resource': convert_k8s_obj_to_json(processed_op.resource)
        }

    # inputs
    input_artifact_paths = processed_op.input_artifact_paths if isinstance(
        processed_op, dsl.ContainerOp) else None
    artifact_arguments = processed_op.artifact_arguments if isinstance(
        processed_op, dsl.ContainerOp) else None
    inputs = _inputs_to_json(processed_op.inputs, input_artifact_paths,
                             artifact_arguments)
    if inputs:
        template['inputs'] = inputs

    # outputs
    if isinstance(op, dsl.ContainerOp):
        param_outputs = processed_op.file_outputs
    elif isinstance(op, dsl.ResourceOp):
        param_outputs = processed_op.attribute_outputs
    outputs_dict = _outputs_to_json(op, processed_op.outputs, param_outputs,
                                    output_artifacts)
    if outputs_dict:
        template['outputs'] = outputs_dict

    # pod spec used for runtime container settings
    podSpecPatch = {}

    # node selector
    if processed_op.node_selector:
        copy_node_selector = copy.deepcopy(processed_op.node_selector)
        for key, value in processed_op.node_selector.items():
            if re.match('^{{inputs.parameters.*}}$', key) or re.match(
                    '^{{inputs.parameters.*}}$', value):
                if not 'nodeSelector' in podSpecPatch:
                    podSpecPatch['nodeSelector'] = {}
                podSpecPatch["nodeSelector"][key] = value
                del copy_node_selector[
                    key]  # avoid to change the dict when iterating it
        if processed_op.node_selector:
            template['nodeSelector'] = copy_node_selector

    # tolerations
    if processed_op.tolerations:
        template['tolerations'] = processed_op.tolerations

    # affinity
    if processed_op.affinity:
        template['affinity'] = convert_k8s_obj_to_json(processed_op.affinity)

    # metadata
    if processed_op.pod_annotations or processed_op.pod_labels:
        template['metadata'] = {}
        if processed_op.pod_annotations:
            template['metadata']['annotations'] = processed_op.pod_annotations
        if processed_op.pod_labels:
            template['metadata']['labels'] = processed_op.pod_labels
    # retries
    if processed_op.num_retries or processed_op.retry_policy:
        template['retryStrategy'] = {}
        if processed_op.num_retries:
            template['retryStrategy']['limit'] = processed_op.num_retries
        if processed_op.retry_policy:
            template['retryStrategy']['retryPolicy'] = processed_op.retry_policy
            if not processed_op.num_retries:
                warnings.warn('retry_policy is set, but num_retries is not')
        backoff_dict = {}
        if processed_op.backoff_duration:
            backoff_dict['duration'] = processed_op.backoff_duration
        if processed_op.backoff_factor:
            backoff_dict['factor'] = processed_op.backoff_factor
        if processed_op.backoff_max_duration:
            backoff_dict['maxDuration'] = processed_op.backoff_max_duration
        if backoff_dict:
            template['retryStrategy']['backoff'] = backoff_dict

    # timeout
    if processed_op.timeout:
        template['activeDeadlineSeconds'] = processed_op.timeout

    # initContainers
    if processed_op.init_containers:
        template['initContainers'] = processed_op.init_containers

    # sidecars
    if processed_op.sidecars:
        template['sidecars'] = processed_op.sidecars

    # volumes
    if processed_op.volumes:
        template['volumes'] = [
            convert_k8s_obj_to_json(volume) for volume in processed_op.volumes
        ]
        template['volumes'].sort(key=lambda x: x['name'])

    # Runtime resource requests
    if isinstance(op, dsl.ContainerOp) and ('resources' in op.container.keys()):
        for setting, val in op.container['resources'].items():
            for resource, param in val.items():
                if (resource in ['cpu', 'memory', 'amd.com/gpu', 'nvidia.com/gpu'] or re.match('^{{inputs.parameters.*}}$', resource))\
                    and re.match('^{{inputs.parameters.*}}$', str(param)):
                    if not 'containers' in podSpecPatch:
                        podSpecPatch['containers'] = [{
                                'name': 'main',
                                'resources': {}
                            }]
                    if setting not in podSpecPatch['containers'][0][
                            'resources']:
                        podSpecPatch['containers'][0]['resources'][setting] = {
                            resource: param
                        }
                    else:
                        podSpecPatch['containers'][0]['resources'][setting][
                            resource] = param
                    del template['container']['resources'][setting][resource]
                    if not template['container']['resources'][setting]:
                        del template['container']['resources'][setting]

    if isinstance(op, dsl.ContainerOp) and op._metadata and not op.is_v2:
        template.setdefault('metadata', {}).setdefault(
            'annotations',
            {})['pipelines.kubeflow.org/component_spec'] = json.dumps(
                op._metadata.to_dict(), sort_keys=True)

    if hasattr(op, '_component_ref'):
        template.setdefault('metadata', {}).setdefault(
            'annotations',
            {})['pipelines.kubeflow.org/component_ref'] = json.dumps(
                op._component_ref.to_dict(), sort_keys=True)

    if hasattr(op, '_parameter_arguments') and op._parameter_arguments:
        template.setdefault('metadata', {}).setdefault(
            'annotations',
            {})['pipelines.kubeflow.org/arguments.parameters'] = json.dumps(
                op._parameter_arguments, sort_keys=True)

    if isinstance(op, dsl.ContainerOp) and op.execution_options:
        if op.execution_options.caching_strategy.max_cache_staleness:
            template.setdefault('metadata', {}).setdefault(
                'annotations',
                {})['pipelines.kubeflow.org/max_cache_staleness'] = str(
                    op.execution_options.caching_strategy.max_cache_staleness)

    if podSpecPatch:
        template['podSpecPatch'] = json.dumps(podSpecPatch)
    return template
