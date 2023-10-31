#/bin/env python3

import copy
import json
import os
import re
import warnings
from typing import List, Optional, Set


def rewrite_data_passing_to_use_volumes(
    workflow: dict,
    volume: dict,
    path_prefix: str = 'artifact_data/',
) -> dict:
    """Converts Argo workflow that passes data using artifacts to workflow that
    passes data using a single multi-write volume.

    Implementation: Changes file passing system from passing Argo artifacts to passing parameters holding volumeMount subPaths.

    Limitations:
    * All artifact file names must be the same (e.g. "data"). E.g. "/tmp/outputs/my_out_1/data".
      Otherwise consumer component that can receive data from different producers won't be able to specify input file path since files from different upstreams will have different names.
    * Passing constant arguments to artifact inputs is not supported for now. Only references can be passed.

    Args:
        workflow: Workflow (dict) that needs to be converted.
        volume: Kubernetes spec (dict) of a volume that should be used for data passing. The volume should support multi-write (READ_WRITE_MANY) if the workflow has any parallel steps. Example: {'persistentVolumeClaim': {'claimName': 'my-pvc'}}

    Returns:
        Converted workflow (dict).
    """

    # Limitation: All artifact file names must be the same (e.g. "data"). E.g. "/tmp/outputs/my_out_1/data".
    # Otherwise consumer component that can receive data from different producers won't be able to specify input file path since files from different upstreams will have different names.
    # Maybe this limitation can be lifted, but it is not trivial. Maybe with DSL compile it's easier since the graph it creates is usually a tree. But recursion creates a real graph.
    # Limitation: Passing constant values to artifact inputs is not supported for now.

    # Idea: Passing volumeMount subPaths instead of artifcats

    workflow = copy.deepcopy(workflow)
    templates = workflow['spec']['templates']

    container_templates = [
        template for template in templates if 'container' in template
    ]
    dag_templates = [template for template in templates if 'dag' in template]
    steps_templates = [
        template for template in templates if 'steps' in template
    ]

    execution_data_dir = path_prefix + '{{workflow.uid}}_{{pod.name}}/'

    data_volume_name = 'data-storage'
    volume['name'] = data_volume_name

    subpath_parameter_name_suffix = '-subpath'

    def convert_artifact_reference_to_parameter_reference(
            reference: str) -> str:
        parameter_reference = re.sub(
            r'{{([^}]+)\.artifacts\.([^}]+)}}',
            r'{{\1.parameters.\2' + subpath_parameter_name_suffix +
            '}}',  # re.escape(subpath_parameter_name_suffix) escapes too much.
            reference,
        )
        return parameter_reference

    # Adding the data storage volume to the workflow
    workflow['spec'].setdefault('volumes', []).append(volume)

    all_artifact_file_names = set(
    )  # All artifacts should have same file name (usually, "data"). This variable holds all different artifact names for verification.

    # Rewriting container templates
    for template in templates:
        if 'container' not in template and 'script' not in template:
            continue
        container_spec = template['container'] or template['steps']
        # Inputs
        input_artifacts = template.get('inputs', {}).get('artifacts', [])
        if input_artifacts:
            input_parameters = template.setdefault('inputs', {}).setdefault(
                'parameters', [])
            volume_mounts = container_spec.setdefault('volumeMounts', [])
            for input_artifact in input_artifacts:
                subpath_parameter_name = input_artifact[
                    'name'] + subpath_parameter_name_suffix  # TODO: Maybe handle clashing names.
                artifact_file_name = os.path.basename(input_artifact['path'])
                all_artifact_file_names.add(artifact_file_name)
                artifact_dir = os.path.dirname(input_artifact['path'])
                volume_mounts.append({
                    'mountPath':
                        artifact_dir,
                    'name':
                        data_volume_name,
                    'subPath':
                        '{{inputs.parameters.' + subpath_parameter_name + '}}',
                    'readOnly':
                        True,
                })
                input_parameters.append({
                    'name': subpath_parameter_name,
                })
        template.get('inputs', {}).pop('artifacts', None)

        # Outputs
        output_artifacts = template.get('outputs', {}).get('artifacts', [])
        if output_artifacts:
            output_parameters = template.setdefault('outputs', {}).setdefault(
                'parameters', [])
            del template.get('outputs', {})['artifacts']
            volume_mounts = container_spec.setdefault('volumeMounts', [])
            for output_artifact in output_artifacts:
                output_name = output_artifact['name']
                subpath_parameter_name = output_name + subpath_parameter_name_suffix  # TODO: Maybe handle clashing names.
                artifact_file_name = os.path.basename(output_artifact['path'])
                all_artifact_file_names.add(artifact_file_name)
                artifact_dir = os.path.dirname(output_artifact['path'])
                output_subpath = execution_data_dir + output_name
                volume_mounts.append({
                    'mountPath': artifact_dir,
                    'name': data_volume_name,
                    'subPath':
                        output_subpath,  # TODO: Switch to subPathExpr when it's out of beta: https://kubernetes.io/docs/concepts/storage/volumes/#using-subpath-with-expanded-environment-variables
                })
                output_parameters.append({
                    'name': subpath_parameter_name,
                    'value': output_subpath,  # Requires Argo 2.3.0+
                })
            whitelist = ['mlpipeline-ui-metadata', 'mlpipeline-metrics']
            output_artifacts = [artifact for artifact in output_artifacts if artifact['name'] in whitelist]
            if not output_artifacts:
              template.get('outputs', {}).pop('artifacts', None)
            else:
                template.get('outputs', {}).update({'artifacts': output_artifacts})

    # Rewrite DAG templates
    for template in templates:
        if 'dag' not in template and 'steps' not in template:
            continue

        # Inputs
        input_artifacts = template.get('inputs', {}).get('artifacts', [])
        if input_artifacts:
            input_parameters = template.setdefault('inputs', {}).setdefault(
                'parameters', [])
            volume_mounts = container_spec.setdefault('volumeMounts', [])
            for input_artifact in input_artifacts:
                subpath_parameter_name = input_artifact[
                    'name'] + subpath_parameter_name_suffix  # TODO: Maybe handle clashing names.
                input_parameters.append({
                    'name': subpath_parameter_name,
                })
        template.get('inputs', {}).pop('artifacts', None)

        # Outputs
        output_artifacts = template.get('outputs', {}).get('artifacts', [])
        if output_artifacts:
            output_parameters = template.setdefault('outputs', {}).setdefault(
                'parameters', [])
            volume_mounts = container_spec.setdefault('volumeMounts', [])
            for output_artifact in output_artifacts:
                output_name = output_artifact['name']
                subpath_parameter_name = output_name + subpath_parameter_name_suffix  # TODO: Maybe handle clashing names.
                output_parameters.append({
                    'name': subpath_parameter_name,
                    'valueFrom': {
                        'parameter':
                            convert_artifact_reference_to_parameter_reference(
                                output_artifact['from'])
                    },
                })
        template.get('outputs', {}).pop('artifacts', None)

        # Arguments
        for task in template.get('dag', {}).get('tasks', []) + [
                steps for group in template.get('steps', []) for steps in group
        ]:
            argument_artifacts = task.get('arguments', {}).get('artifacts', [])
            if argument_artifacts:
                argument_parameters = task.setdefault('arguments',
                                                      {}).setdefault(
                                                          'parameters', [])
                for argument_artifact in argument_artifacts:
                    if 'from' not in argument_artifact:
                        raise NotImplementedError(
                            'Volume-based data passing rewriter does not support constant artifact arguments at this moment. Only references can be passed.'
                        )
                    subpath_parameter_name = argument_artifact[
                        'name'] + subpath_parameter_name_suffix  # TODO: Maybe handle clashing names.
                    argument_parameters.append({
                        'name':
                            subpath_parameter_name,
                        'value':
                            convert_artifact_reference_to_parameter_reference(
                                argument_artifact['from']),
                    })
            task.get('arguments', {}).pop('artifacts', None)

        # There should not be any artifact references in any other part of DAG template (only parameter references)

    # Check that all artifacts have the same file names
    if len(all_artifact_file_names) > 1:
        warnings.warn(
            'Detected different artifact file names: [{}]. The workflow can fail at runtime. Please use the same file name (e.g. "data") for all artifacts.'
            .format(', '.join(all_artifact_file_names)))

    # Fail if workflow has argument artifacts
    workflow_argument_artifacts = workflow['spec'].get('arguments',
                                                       {}).get('artifacts', [])
    if workflow_argument_artifacts:
        raise NotImplementedError(
            'Volume-based data passing rewriter does not support constant artifact arguments at this moment. Only references can be passed.'
        )

    return workflow


if __name__ == '__main__':
    """Converts Argo workflow that passes data using artifacts to workflow that
    passes data using a single multi-write volume."""

    import argparse
    import io
    import yaml

    parser = argparse.ArgumentParser(
        prog='data_passing_using_volume',
        epilog='''Example: data_passing_using_volume.py --input argo/examples/artifact-passing.yaml --output argo/examples/artifact-passing-volumes.yaml --volume "persistentVolumeClaim: {claimName: my-pvc}"'''
    )
    parser.add_argument(
        "--input",
        type=argparse.FileType('r'),
        required=True,
        help='Path to workflow that needs to be converted.')
    parser.add_argument(
        "--output",
        type=argparse.FileType('w'),
        required=True,
        help='Path where to write converted workflow.')
    parser.add_argument(
        "--volume",
        type=str,
        required=True,
        help='''Kubernetes spec (YAML) of a volume that should be used for data passing. The volume should support multi-write (READ_WRITE_MANY) if the workflow has any parallel steps. Example: "persistentVolumeClaim: {claimName: my-pvc}".'''
    )
    args = parser.parse_args()

    input_workflow = yaml.safe_load(args.input)
    volume = yaml.safe_load(io.StringIO(args.volume))
    output_workflow = rewrite_data_passing_to_use_volumes(
        input_workflow, volume)
    yaml.dump(output_workflow, args.output)
