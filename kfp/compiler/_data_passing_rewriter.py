import copy
import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from kfp.dsl import _component_bridge
from kfp import dsl


def fix_big_data_passing(workflow: dict) -> dict:
    '''fix_big_data_passing converts a workflow where some artifact data is passed as parameters and converts it to a workflow where this data is passed as artifacts.
    Args:
        workflow: The workflow to fix
    Returns:
        The fixed workflow

    Motivation:
    DSL compiler only supports passing Argo parameters.
    Due to the convoluted nature of the DSL compiler, the artifact consumption and passing has been implemented on top of that using parameter passing. The artifact data is passed as parameters and the consumer template creates an artifact/file out of that data.
    Due to the limitations of Kubernetes and Argo this scheme cannot pass data larger than few kilobytes preventing any serious use of artifacts.

    This function rewrites the compiled workflow so that the data consumed as artifact is passed as artifact.
    It also prunes the unused parameter outputs. This is important since if a big piece of data is ever returned through a file that is also output as parameter, the execution will fail.
    This makes is possible to pass large amounts of data.

    Implementation:
    1. Index the DAGs to understand how data is being passed and which inputs/outputs are connected to each other.
    2. Search for direct data consumers in container/resource templates and some DAG task attributes (e.g. conditions and loops) to find out which inputs are directly consumed as parameters/artifacts.
    3. Propagate the consumption information upstream to all inputs/outputs all the way up to the data producers.
    4. Convert the inputs, outputs and arguments based on how they're consumed downstream.
    '''
    workflow = copy.deepcopy(workflow)

    container_templates = [
        template for template in workflow['spec']['templates']
        if 'container' in template
    ]
    dag_templates = [
        template for template in workflow['spec']['templates']
        if 'dag' in template
    ]
    resource_templates = [
        template for template in workflow['spec']['templates']
        if 'resource' in template
    ]  # TODO: Handle these
    resource_template_names = set(
        template['name'] for template in resource_templates)

    # 1. Index the DAGs to understand how data is being passed and which inputs/outputs are connected to each other.
    template_input_to_parent_dag_inputs = {
    }  # (task_template_name, task_input_name) -> Set[(dag_template_name, dag_input_name)]
    template_input_to_parent_task_outputs = {
    }  # (task_template_name, task_input_name) -> Set[(upstream_template_name, upstream_output_name)]
    template_input_to_parent_constant_arguments = {
    }  #(task_template_name, task_input_name) -> Set[argument_value] # Unused
    dag_output_to_parent_template_outputs = {
    }  # (dag_template_name, output_name) -> Set[(upstream_template_name, upstream_output_name)]

    for template in dag_templates:
        dag_template_name = template['name']
        # Indexing task arguments
        dag_tasks = template['dag']['tasks']
        task_name_to_template_name = {
            task['name']: task['template'] for task in dag_tasks
        }
        for task in dag_tasks:
            task_template_name = task['template']
            parameter_arguments = task.get('arguments',
                                           {}).get('parameters', {})
            for parameter_argument in parameter_arguments:
                task_input_name = parameter_argument['name']
                argument_value = parameter_argument['value']

                argument_placeholder_parts = deconstruct_single_placeholder(
                    argument_value)
                if not argument_placeholder_parts:  # Argument is considered to be constant string
                    template_input_to_parent_constant_arguments.setdefault(
                        (task_template_name, task_input_name),
                        set()).add(argument_value)

                placeholder_type = argument_placeholder_parts[0]
                if placeholder_type not in ('inputs', 'outputs', 'tasks',
                                            'steps', 'workflow', 'pod', 'item'):
                    # Do not fail on Jinja or other double-curly-brace templates
                    continue
                if placeholder_type == 'inputs':
                    assert argument_placeholder_parts[1] == 'parameters'
                    dag_input_name = argument_placeholder_parts[2]
                    template_input_to_parent_dag_inputs.setdefault(
                        (task_template_name, task_input_name), set()).add(
                            (dag_template_name, dag_input_name))
                elif placeholder_type == 'tasks':
                    upstream_task_name = argument_placeholder_parts[1]
                    assert argument_placeholder_parts[2] == 'outputs'
                    assert argument_placeholder_parts[3] == 'parameters'
                    upstream_output_name = argument_placeholder_parts[4]
                    upstream_template_name = task_name_to_template_name[
                        upstream_task_name]
                    template_input_to_parent_task_outputs.setdefault(
                        (task_template_name, task_input_name), set()).add(
                            (upstream_template_name, upstream_output_name))
                elif placeholder_type == 'item' or placeholder_type == 'workflow' or placeholder_type == 'pod':
                    # Treat loop variables as constant values
                    # workflow.parameters.* placeholders are not supported, but the DSL compiler does not produce those.
                    template_input_to_parent_constant_arguments.setdefault(
                        (task_template_name, task_input_name),
                        set()).add(argument_value)
                else:
                    raise AssertionError

                dag_input_name = extract_input_parameter_name(argument_value)
                if dag_input_name:
                    template_input_to_parent_dag_inputs.setdefault(
                        (task_template_name, task_input_name), set()).add(
                            (dag_template_name, dag_input_name))
                else:
                    template_input_to_parent_constant_arguments.setdefault(
                        (task_template_name, task_input_name),
                        set()).add(argument_value)

            # Indexing DAG outputs (Does DSL compiler produce them?)
            for dag_output in task.get('outputs', {}).get('parameters', {}):
                dag_output_name = dag_output['name']
                output_value = dag_output['value']
                argument_placeholder_parts = deconstruct_single_placeholder(
                    output_value)
                placeholder_type = argument_placeholder_parts[0]
                if not argument_placeholder_parts:  # Argument is considered to be constant string
                    raise RuntimeError(
                        'Constant DAG output values are not supported for now.')
                if placeholder_type == 'inputs':
                    raise RuntimeError(
                        'Pass-through DAG inputs/outputs are not supported')
                elif placeholder_type == 'tasks':
                    upstream_task_name = argument_placeholder_parts[1]
                    assert argument_placeholder_parts[2] == 'outputs'
                    assert argument_placeholder_parts[3] == 'parameters'
                    upstream_output_name = argument_placeholder_parts[4]
                    upstream_template_name = task_name_to_template_name[
                        upstream_task_name]
                    dag_output_to_parent_template_outputs.setdefault(
                        (dag_template_name, dag_output_name), set()).add(
                            (upstream_template_name, upstream_output_name))
                elif placeholder_type == 'item' or placeholder_type == 'workflow' or placeholder_type == 'pod':
                    raise RuntimeError(
                        'DAG output value "{}" is not supported.'.format(
                            output_value))
                else:
                    raise AssertionError(
                        'Unexpected placeholder type "{}".'.format(
                            placeholder_type))
    # Finshed indexing the DAGs

    # 2. Search for direct data consumers in container/resource templates and some DAG task attributes (e.g. conditions and loops) to find out which inputs are directly consumed as parameters/artifacts.
    inputs_directly_consumed_as_parameters = set()
    inputs_directly_consumed_as_artifacts = set()
    outputs_directly_consumed_as_parameters = set()

    # Searching for artifact input consumers in container template inputs
    for template in container_templates:
        template_name = template['name']
        for input_artifact in template.get('inputs', {}).get('artifacts', {}):
            raw_data = input_artifact['raw']['data']  # The structure must exist
            # The raw data must be a single input parameter reference. Otherwise (e.g. it's a string or a string with multiple inputs) we should not do the conversion to artifact passing.
            input_name = extract_input_parameter_name(raw_data)
            if input_name:
                inputs_directly_consumed_as_artifacts.add(
                    (template_name, input_name))
                del input_artifact[
                    'raw']  # Deleting the "default value based" data passing hack so that it's replaced by the "argument based" way of data passing.
                input_artifact[
                    'name'] = input_name  # The input artifact name should be the same as the original input parameter name

    # Searching for parameter input consumers in DAG templates (.when, .withParam, etc)
    for template in dag_templates:
        template_name = template['name']
        dag_tasks = template['dag']['tasks']
        task_name_to_template_name = {
            task['name']: task['template'] for task in dag_tasks
        }
        for task in template['dag']['tasks']:
            # We do not care about the inputs mentioned in task arguments since we will be free to switch them from parameters to artifacts
            # TODO: Handle cases where argument value is a string containing placeholders (not just consisting of a single placeholder) or the input name contains placeholder
            task_without_arguments = task.copy()  # Shallow copy
            task_without_arguments.pop('arguments', None)
            placeholders = extract_all_placeholders(task_without_arguments)
            for placeholder in placeholders:
                parts = placeholder.split('.')
                placeholder_type = parts[0]
                if placeholder_type not in ('inputs', 'outputs', 'tasks',
                                            'steps', 'workflow', 'pod', 'item'):
                    # Do not fail on Jinja or other double-curly-brace templates
                    continue
                if placeholder_type == 'inputs':
                    if parts[1] == 'parameters':
                        input_name = parts[2]
                        inputs_directly_consumed_as_parameters.add(
                            (template_name, input_name))
                    else:
                        raise AssertionError
                elif placeholder_type == 'tasks':
                    upstream_task_name = parts[1]
                    assert parts[2] == 'outputs'
                    assert parts[3] == 'parameters'
                    upstream_output_name = parts[4]
                    upstream_template_name = task_name_to_template_name[
                        upstream_task_name]
                    outputs_directly_consumed_as_parameters.add(
                        (upstream_template_name, upstream_output_name))
                elif placeholder_type == 'workflow' or placeholder_type == 'pod':
                    pass
                elif placeholder_type == 'item':
                    raise AssertionError(
                        'The "{{item}}" placeholder is not expected outside task arguments.'
                    )
                else:
                    raise AssertionError(
                        'Unexpected placeholder type "{}".'.format(
                            placeholder_type))

    # Searching for parameter input consumers in container and resource templates
    for template in container_templates + resource_templates:
        template_name = template['name']
        placeholders = extract_all_placeholders(template)
        for placeholder in placeholders:
            parts = placeholder.split('.')
            placeholder_type = parts[0]
            if placeholder_type not in ('inputs', 'outputs', 'tasks', 'steps',
                                        'workflow', 'pod', 'item'):
                # Do not fail on Jinja or other double-curly-brace templates
                continue

            if placeholder_type == 'workflow' or placeholder_type == 'pod':
                pass
            elif placeholder_type == 'inputs':
                if parts[1] == 'parameters':
                    input_name = parts[2]
                    inputs_directly_consumed_as_parameters.add(
                        (template_name, input_name))
                elif parts[1] == 'artifacts':
                    raise AssertionError(
                        'Found unexpected Argo input artifact placeholder in container template: {}'
                        .format(placeholder))
                else:
                    raise AssertionError(
                        'Found unexpected Argo input placeholder in container template: {}'
                        .format(placeholder))
            else:
                raise AssertionError(
                    'Found unexpected Argo placeholder in container template: {}'
                    .format(placeholder))

    # Finished indexing data consumers

    # 3. Propagate the consumption information upstream to all inputs/outputs all the way up to the data producers.
    inputs_consumed_as_parameters = set()
    inputs_consumed_as_artifacts = set()

    outputs_consumed_as_parameters = set()
    outputs_consumed_as_artifacts = set()

    def mark_upstream_ios_of_input(template_input, marked_inputs,
                                   marked_outputs):
        # Stopping if the input has already been visited to save time and handle recursive calls
        if template_input in marked_inputs:
            return
        marked_inputs.add(template_input)

        upstream_inputs = template_input_to_parent_dag_inputs.get(
            template_input, [])
        for upstream_input in upstream_inputs:
            mark_upstream_ios_of_input(upstream_input, marked_inputs,
                                       marked_outputs)

        upstream_outputs = template_input_to_parent_task_outputs.get(
            template_input, [])
        for upstream_output in upstream_outputs:
            mark_upstream_ios_of_output(upstream_output, marked_inputs,
                                        marked_outputs)

    def mark_upstream_ios_of_output(template_output, marked_inputs,
                                    marked_outputs):
        # Stopping if the output has already been visited to save time and handle recursive calls
        if template_output in marked_outputs:
            return
        marked_outputs.add(template_output)

        upstream_outputs = dag_output_to_parent_template_outputs.get(
            template_output, [])
        for upstream_output in upstream_outputs:
            mark_upstream_ios_of_output(upstream_output, marked_inputs,
                                        marked_outputs)

    for input in inputs_directly_consumed_as_parameters:
        mark_upstream_ios_of_input(input, inputs_consumed_as_parameters,
                                   outputs_consumed_as_parameters)
    for input in inputs_directly_consumed_as_artifacts:
        mark_upstream_ios_of_input(input, inputs_consumed_as_artifacts,
                                   outputs_consumed_as_artifacts)
    for output in outputs_directly_consumed_as_parameters:
        mark_upstream_ios_of_output(output, inputs_consumed_as_parameters,
                                    outputs_consumed_as_parameters)

    # 4. Convert the inputs, outputs and arguments based on how they're consumed downstream.

    # Container templates already output all data as artifacts, so we do not need to convert their outputs to artifacts. (But they also output data as parameters and we need to fix that.)

    # Convert DAG argument passing from parameter to artifacts as needed
    for template in dag_templates:
        # Converting DAG inputs
        inputs = template.get('inputs', {})
        input_parameters = inputs.get('parameters', [])
        input_artifacts = inputs.setdefault('artifacts', [])  # Should be empty
        for input_parameter in input_parameters:
            input_name = input_parameter['name']
            if (template['name'], input_name) in inputs_consumed_as_artifacts:
                input_artifacts.append({
                    'name': input_name,
                })

        # Converting DAG outputs
        outputs = template.get('outputs', {})
        output_parameters = outputs.get('parameters', [])
        output_artifacts = outputs.setdefault('artifacts',
                                              [])  # Should be empty
        for output_parameter in output_parameters:
            output_name = output_parameter['name']
            if (template['name'], output_name) in outputs_consumed_as_artifacts:
                parameter_reference_placeholder = output_parameter['valueFrom'][
                    'parameter']
                output_artifacts.append({
                    'name':
                        output_name,
                    'from':
                        parameter_reference_placeholder.replace(
                            '.parameters.', '.artifacts.'),
                })

        # Converting DAG task arguments
        tasks = template.get('dag', {}).get('tasks', [])
        for task in tasks:
            task_arguments = task.get('arguments', {})
            parameter_arguments = task_arguments.get('parameters', [])
            artifact_arguments = task_arguments.setdefault('artifacts', [])
            for parameter_argument in parameter_arguments:
                input_name = parameter_argument['name']
                if (task['template'],
                        input_name) in inputs_consumed_as_artifacts:
                    argument_value = parameter_argument[
                        'value']  # argument parameters always use "value"; output parameters always use "valueFrom" (container/DAG/etc)
                    argument_placeholder_parts = deconstruct_single_placeholder(
                        argument_value)
                    # If the argument is consumed as artifact downstream:
                    # Pass DAG inputs and DAG/container task outputs as usual;
                    # Everything else (constant strings, loop variables, resource task outputs) is passed as raw artifact data. Argo properly replaces placeholders in it.
                    if argument_placeholder_parts and argument_placeholder_parts[
                            0] in [
                                'inputs', 'tasks'
                            ] and not (argument_placeholder_parts[0] == 'tasks'
                                       and argument_placeholder_parts[1]
                                       in resource_template_names):
                        artifact_arguments.append({
                            'name':
                                input_name,
                            'from':
                                argument_value.replace('.parameters.',
                                                       '.artifacts.'),
                        })
                    else:
                        artifact_arguments.append({
                            'name': input_name,
                            'raw': {
                                'data': argument_value,
                            },
                        })

    # Remove input parameters unless they're used downstream. This also removes unused container template inputs if any.
    for template in container_templates + dag_templates:
        inputs = template.get('inputs', {})
        inputs['parameters'] = [
            input_parameter for input_parameter in inputs.get('parameters', [])
            if (template['name'],
                input_parameter['name']) in inputs_consumed_as_parameters
        ]

    # Remove output parameters unless they're used downstream
    for template in container_templates + dag_templates:
        outputs = template.get('outputs', {})
        outputs['parameters'] = [
            output_parameter
            for output_parameter in outputs.get('parameters', [])
            if (template['name'],
                output_parameter['name']) in outputs_consumed_as_parameters
        ]

    # Remove DAG parameter arguments unless they're used downstream
    for template in dag_templates:
        tasks = template.get('dag', {}).get('tasks', [])
        for task in tasks:
            task_arguments = task.get('arguments', {})
            task_arguments['parameters'] = [
                parameter_argument
                for parameter_argument in task_arguments.get('parameters', [])
                if (task['template'],
                    parameter_argument['name']) in inputs_consumed_as_parameters
            ]

    # Fix Workflow parameter arguments that are consumed as artifacts downstream
    #
    workflow_spec = workflow['spec']
    entrypoint_template_name = workflow_spec['entrypoint']
    workflow_arguments = workflow_spec['arguments']
    parameter_arguments = workflow_arguments.get('parameters', [])
    artifact_arguments = workflow_arguments.get('artifacts',
                                                [])  # Should be empty
    for parameter_argument in parameter_arguments:
        input_name = parameter_argument['name']
        if (entrypoint_template_name,
                input_name) in inputs_consumed_as_artifacts:
            artifact_arguments.append({
                'name': input_name,
                'raw': {
                    'data': '{{workflow.parameters.' + input_name + '}}',
                },
            })
    if artifact_arguments:
        workflow_arguments['artifacts'] = artifact_arguments

    clean_up_empty_workflow_structures(workflow)
    return workflow


def clean_up_empty_workflow_structures(workflow: dict):
    templates = workflow['spec']['templates']
    for template in templates:
        inputs = template.setdefault('inputs', {})
        if not inputs.setdefault('parameters', []):
            del inputs['parameters']
        if not inputs.setdefault('artifacts', []):
            del inputs['artifacts']
        if not inputs:
            del template['inputs']
        outputs = template.setdefault('outputs', {})
        if not outputs.setdefault('parameters', []):
            del outputs['parameters']
        if not outputs.setdefault('artifacts', []):
            del outputs['artifacts']
        if not outputs:
            del template['outputs']
        if 'dag' in template:
            for task in template['dag'].get('tasks', []):
                arguments = task.setdefault('arguments', {})
                if not arguments.setdefault('parameters', []):
                    del arguments['parameters']
                if not arguments.setdefault('artifacts', []):
                    del arguments['artifacts']
                if not arguments:
                    del task['arguments']


def extract_all_placeholders(template: dict) -> Set[str]:
    template_str = json.dumps(template)
    placeholders = set(re.findall('{{([-._a-zA-Z0-9]+)}}', template_str))
    return placeholders


def extract_input_parameter_name(s: str) -> Optional[str]:
    match = re.fullmatch('{{inputs.parameters.([-_a-zA-Z0-9]+)}}', s)
    if not match:
        return None
    (input_name,) = match.groups()
    return input_name


def deconstruct_single_placeholder(s: str) -> List[str]:
    if not re.fullmatch('{{[-._a-zA-Z0-9]+}}', s):
        return None
    return s.lstrip('{').rstrip('}').split('.')
