import kfp
from kfp import dsl
from kubernetes.client.models import V1EnvVar

from .constants import DEFAULT_FLOW_CODE_URL, DEFAULT_KFP_YAML_OUTPUT_PATH, DEFAULT_DOWNLOADED_FLOW_FILENAME
from .constants import S3_AWS_ARN as S3_AWS_ARN_VALUE
from .constants import S3_BUCKET as S3_BUCKET_VALUE
from typing import NamedTuple
from collections import deque

StepOutput = NamedTuple('StepOutput', [('ds_root', str), ('run_id', str)])

def step_op_func(python_cmd_template, step_name: str,
                 code_url: str,
                 ds_root: str,
                 run_id: str,
               ):
    """
    Function used to create a KFP container op (see: `step_container_op`) that corresponds to a single step in the flow.

    """
    import subprocess
    import os

    MODIFIED_METAFLOW_URL = 'git+https://github.com/zillow/metaflow.git@state-integ-s3'
    DEFAULT_DOWNLOADED_FLOW_FILENAME = 'downloaded_flow.py'

    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    subprocess.call(
        ["curl -o {downloaded_file_name} {code_url}".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            code_url=code_url)], shell=True)

    print("\n----------RUNNING: KFP Installation---------------")
    subprocess.call(["pip3 install kfp"], shell=True)  # TODO: Remove this once KFP is added to dependencies

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    subprocess.call(["pip3 install --user --upgrade {modified_metaflow_git_url}".format(
        modified_metaflow_git_url=MODIFIED_METAFLOW_URL)],
                    shell=True)

    print("\n----------RUNNING: MAIN STEP COMMAND--------------")
    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_AWS_ARN = os.getenv("S3_AWS_ARN")

    define_s3_env_vars = 'export METAFLOW_DATASTORE_SYSROOT_S3="{}" && export METAFLOW_AWS_ARN="{}" '.format(S3_BUCKET,
                                                                                                             S3_AWS_ARN)
    define_username = 'export USERNAME="kfp-user"'
    python_cmd = python_cmd_template.format(ds_root=ds_root, run_id=run_id)

    final_run_cmd = f'{define_username} && {define_s3_env_vars} && {python_cmd}'

    print("RUNNING COMMAND: ", final_run_cmd)
    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    proc_output = proc.stdout
    proc_error = proc.stderr

    # END is the final step and no outputs need to be returned
    if step_name.lower() == 'end':
        print("_______________ FLOW RUN COMPLETE ________________")

    # TODO: Check why MF echo statements are getting redirected to stderr
    if len(proc_error) > 1:
        print("_______________STDERR:_____________________________")
        print(proc_error)

    if len(proc_output) > 1:
        print("_______________STDOUT:_____________________________")
        print(proc_output)

    else:
        raise RuntimeWarning("This step did not generate the correct args for next step to run. This might disrupt "
                             "the workflow")

    # TODO: Metadata needed for client API to run needs to be persisted outside before this
    print("--------------- RUNNING: CLEANUP OF TEMP DIR----------")
    subprocess.call(["rm -r /opt/zillow/.metaflow"], shell=True)

    print("_______________ Done _________________________________")

def pre_start_op_func(code_url: str)  -> StepOutput:
    """
    Function used to create a KFP container op (see `pre_start_container_op`)that corresponds to the `pre-start` step of metaflow

    """
    import subprocess
    import os
    from collections import namedtuple

    MODIFIED_METAFLOW_URL = 'git+https://github.com/zillow/metaflow.git@state-integ-s3'
    DEFAULT_DOWNLOADED_FLOW_FILENAME = 'downloaded_flow.py'

    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    subprocess.call(
        ["curl -o {downloaded_file_name} {code_url}".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            code_url=code_url)], shell=True)

    print("\n----------RUNNING: KFP Installation---------------")
    subprocess.call(["pip3 install kfp"], shell=True)  # TODO: Remove this once KFP is added to dependencies

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    subprocess.call(["pip3 install --user --upgrade {modified_metaflow_git_url}".format(
        modified_metaflow_git_url=MODIFIED_METAFLOW_URL)],
                    shell=True)

    print("\n----------RUNNING: MAIN STEP COMMAND--------------")
    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_AWS_ARN = os.getenv("S3_AWS_ARN")

    define_s3_env_vars = 'export METAFLOW_DATASTORE_SYSROOT_S3="{}" && export METAFLOW_AWS_ARN="{}" '.format(S3_BUCKET,
                                                                                                          S3_AWS_ARN)
    define_username = 'export USERNAME="kfp-user"'
    python_cmd = 'python {0} --datastore="s3" --datastore-root="{1}" pre-start'.format(DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                                                       S3_BUCKET)
    final_run_cmd = f'{define_username} && {define_s3_env_vars} && {python_cmd}'

    print("RUNNING COMMAND: ", final_run_cmd)
    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    proc_output = proc.stdout
    proc_error = proc.stderr

    StepOutput = namedtuple('StepOutput',
                             ['ds_root', 'run_id'])

    # TODO: Check why MF echo statements are getting redirected to stderr
    if len(proc_error) > 1:
        print("_____________ STDERR:_____________________________")
        print(proc_error)

    if len(proc_output) > 1:
        print("______________ STDOUT:____________________________")
        print(proc_output)

        # Read in the outputs (in the penultimate line) containing args needed for the next steps.
        # Note: Output format is: ['ds_root', 'run_id']
        outputs = (proc_output.split("\n")[-2]).split() # this contains the args needed for next steps to run

    else:
        raise RuntimeWarning("This step did not generate the correct args for next step to run. This might disrupt the workflow")

    # TODO: Metadata needed for client API to run needs to be persisted outside before this
    print("--------------- RUNNING: CLEANUP OF TEMP DIR----------")
    subprocess.call(["rm -r /opt/zillow/.metaflow"], shell=True)

    print("_______________ Done __________________________")
    return StepOutput(outputs[0], outputs[1])

def step_container_op():
    """
    Container op that corresponds to a step defined in the Metaflow flowgraph.

    Note: The public docker image is a copy of the internal docker image we were using (borrowed from aip-kfp-example).
    """

    step_op = kfp.components.func_to_container_op(step_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return step_op

def pre_start_container_op():
    """
    Container op that corresponds to the 'pre-start' step of Metaflow.

    Note: The public docker image is a copy of the internal docker image we were using (borrowed from aip-kfp-example).
    """

    pre_start_op = kfp.components.func_to_container_op(pre_start_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return pre_start_op

def create_command_templates_from_graph(graph):
    """
    Create a map of steps to their corresponding command templates. These command templates help define the command
    to be used to run that particular step with placeholders for the `run_id` and `datastore_root` (location of the datastore).

    # Note:
    # Level-order traversal is adopted to keep the task-ids in line with what happens during a local metaflow execution.
    # It is not entirely necessary to keep this order of task-ids if we are able to point to the correct input-paths for
    # each step. But, using this ordering does keep the organization of data more understandable and natural (i.e., `start`
    # gets a task id of 1, next step gets a task id of 2 and so on with 'end' step having the highest task id.
    """
    def build_cmd_template(step_name, task_id, input_paths):
        python_cmd = "python {downloaded_file_name} --datastore s3 --datastore-root {{ds_root}} " \
                     "step {step_name} --run-id {{run_id}} --task-id {task_id} " \
                     "--input-paths {input_paths}".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            step_name=step_name, task_id=task_id, input_paths=input_paths)
        return python_cmd

    steps_deque = deque(['start']) # deque to process the DAG in level order
    cur_task_id = 0

    # set of seen steps, i.e., added to the queue for processing
    seen_steps = set(['start'])
    # Mapping of steps to task ids
    task_id_map = {}
    # Mapping of steps to their command templates
    command_template_map = {}

    while len(steps_deque) > 0:
        cur_step = steps_deque.popleft()
        cur_task_id += 1
        task_id_map[cur_step] = cur_task_id

        # Generate the correct input_path for each step. Note: input path depends on a step's parents (i.e., in_funcs)
        # Format of the input-paths for reference:
        # non-join nodes: run-id/parent-step/parent-task-id,
        # branch-join node: run-id/:p1/p1-task-id,p2/p2-task-id,...
        # foreach node: TODO: foreach is not considered here
        if cur_task_id == 1: # start step
            cur_input_path = '{run_id}/_parameters/0' # this is the standard input path for the `start` step
        else:
            if graph.nodes[cur_step].type != 'join':
                parent_step = graph.nodes[cur_step].in_funcs[0]
                cur_input_path = '{run_id}/'+ parent_step + '/' + str(task_id_map[parent_step])
            else:
                cur_input_path = '{run_id}/:'
                for parent in graph.nodes[cur_step].in_funcs:
                    cur_input_path += parent + '/' + str(task_id_map[parent]) + ','
                cur_input_path = cur_input_path.strip(',')

        command_template_map[cur_step] = build_cmd_template(cur_step, cur_task_id, cur_input_path)

        out_funcs = graph.nodes[cur_step].out_funcs
        for step in out_funcs:
            if step not in seen_steps:
                steps_deque.append(step)
                seen_steps.add(step)

    return command_template_map


def create_flow_from_graph(flowgraph, flow_code_url=DEFAULT_FLOW_CODE_URL):

    graph = flowgraph
    code_url = flow_code_url

    command_template_map = create_command_templates_from_graph(graph)

    @dsl.pipeline(
        name='MF on KFP Pipeline',
        description='Pipeline defining KFP equivalent of the Metaflow flow. Currently supports linear flows and flows '
                    'with branch and join nodes'
    )
    def kfp_pipeline_from_flow():
        S3_BUCKET = V1EnvVar(name="S3_BUCKET", value=S3_BUCKET_VALUE)
        S3_AWS_ARN = V1EnvVar(name="S3_AWS_ARN", value=S3_AWS_ARN_VALUE)
        pre_start_op = (pre_start_container_op())(code_url).add_env_variable(S3_BUCKET).add_env_variable(S3_AWS_ARN)
        outputs = pre_start_op.outputs

        container_op_map = {}
        ds_root = outputs['ds_root']
        run_id = outputs['run_id']

        # Initial setup
        pre_start_op.set_display_name('InitialSetup')
        container_op_map['start'] = (step_container_op())(
                                        command_template_map['start'],
                                        'start',
                                        code_url,
                                        ds_root,
                                        run_id
                                        ).add_env_variable(S3_BUCKET).add_env_variable(S3_AWS_ARN)
        container_op_map['start'].set_display_name('start')
        container_op_map['start'].after(pre_start_op)

        for step, cmd in command_template_map.items():
            if step not in container_op_map.keys():
                container_op_map[step] = (step_container_op())(
                                            command_template_map[step],
                                            step,
                                            code_url,
                                            ds_root,
                                            run_id).add_env_variable(S3_BUCKET).add_env_variable(S3_AWS_ARN)
                container_op_map[step].set_display_name(step)
                for parent in graph.nodes[step].in_funcs:
                    container_op_map[step].after(container_op_map[parent])

    return kfp_pipeline_from_flow


def create_run_on_kfp(flowgraph, code_url, experiment_name, run_name):
    """
    Creates a new run on KFP using the `kfp.Client()`. Note: Intermediate pipeline YAML is not generated as this creates
    the run directly using the pipeline function returned by `create_flow_pipeline`

    """

    pipeline_func = create_flow_from_graph(flowgraph, code_url)
    run_pipeline_result = kfp.Client().create_run_from_pipeline_func(pipeline_func,
                                                                     arguments={},
                                                                     experiment_name=experiment_name,
                                                                     run_name=run_name)
    return run_pipeline_result


def create_kfp_pipeline_yaml(flowgraph, code_url, pipeline_file_path=DEFAULT_KFP_YAML_OUTPUT_PATH):
    """
    Creates a new KFP pipeline YAML using `kfp.compiler.Compiler()`. Note: Intermediate pipeline YAML is saved
    at `pipeline_file_path`

    """
    pipeline_func = create_flow_from_graph(flowgraph, code_url)

    kfp.compiler.Compiler().compile(pipeline_func, pipeline_file_path)
    return pipeline_file_path
