import kfp
from kfp import dsl

from .constants import DEFAULT_RUN_NAME, DEFAULT_EXPERIMENT_NAME, DEFAULT_FLOW_CODE_URL, DEFAULT_KFP_YAML_OUTPUT_PATH
from typing import NamedTuple

def get_ordered_steps(graph):
    """
    Returns the ordered step names in the graph (FlowGraph) from start step to end step as a list of strings containing the
    step names.

    # TODO: Support other Metaflow graphs, as branching and joins are not currently supported
    Note: All MF graphs start at the "start" step and end with the "end" step (both of which are mandatory).
    """

    ordered_steps = ['start']
    current_node_name = 'start'

    # This is not an ideal way to iterate over the graph, but it's the (easy+)right thing to do for now.
    # This may need to change as work on improvements.
    while current_node_name != 'end':
        for node in graph:
            if node.name == current_node_name:
                if current_node_name != 'end':
                    current_node_name = node.out_funcs[0]
                    ordered_steps.append(current_node_name)
                    break

    return ordered_steps


def step_op_func(step_name: str,
                 code_url: str,
                 ds_root: str,
                 run_id: str,
                 task_id: str,
                 prev_step_name: str,
                 prev_task_id: str) -> NamedTuple('StepOutput', [('ds_root', str),
                                                         ('run_id', str),
                                                         ('next_step', str),
                                                         ('next_task_id', str),
                                                         ('current_step', str),
                                                         ('current_task_id', str)]
                                                      ):
    """
    Function used to create a KFP container op (see: `step_container_op`) that corresponds to a single step in the flow.

    """
    import subprocess
    from collections import namedtuple

    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    subprocess.call(["curl -o helloworld.py {}".format(code_url)], shell=True)

    print("\n----------RUNNING: KFP Installation---------------")
    subprocess.call(["pip3 install kfp"], shell=True) # TODO: Remove this once KFP is added to dependencies

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    subprocess.call(["pip3 install --user --upgrade git+https://github.com/zillow/metaflow.git@state-integ-s3"],
                    shell=True)

    print("\n----------RUNNING: MAIN STEP COMMAND--------------")
    S3_BUCKET = "s3://workspace-zillow-analytics-stage/aip/metaflow"
    S3_AWS_ARN = "arn:aws:iam::170606514770:role/dev-zestimate-role"

    define_s3_env_vars = 'export METAFLOW_DATASTORE_SYSROOT_S3="{}" && export METAFLOW_AWS_ARN="{}" '.format(S3_BUCKET,
                                                                                                             S3_AWS_ARN)
    define_username = 'export USERNAME="kfp-user"'
    python_cmd = "python helloworld.py --datastore s3 --datastore-root {0} step {1} --run-id {2} " \
                 "--task-id {3} --input-paths {2}/{4}/{5}".format(ds_root, step_name, run_id,
                                                                  task_id, prev_step_name, prev_task_id)
    final_run_cmd = f'{define_username} && {define_s3_env_vars} && {python_cmd}'

    print("RUNNING COMMAND: ", final_run_cmd)
    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    proc_output = proc.stdout
    proc_error = proc.stderr

    step_output = namedtuple('StepOutput',
                             ['ds_root', 'run_id', 'next_step', 'next_task_id', 'current_step', 'current_task_id'])

    # END is the final step and no outputs need to be returned
    if step_name.lower() == 'end':
        print("_______________ FLOW RUN COMPLETE ________________")
        return step_output('None', 'None', 'None', 'None', 'None', 'None')

    # TODO: Check why MF echo statements are getting redirected to stderr
    if len(proc_error) > 1:
        print("_______________STDERR:_____________________________")
        print(proc_error)

    if len(proc_output) > 1:
        print("_______________STDOUT:_____________________________")
        print(proc_output)
        # Read in the outputs (in the penultimate line) containing args needed for the next step.
        # Note: Output format is: ['ds_root', 'run_id', 'next_step', 'next_task_id', 'current_step', 'current_task_id']
        outputs = (proc_output.split("\n")[-2]).split()
        print(step_output(outputs[0], outputs[1], outputs[2], outputs[3], outputs[4], outputs[5]))
    else:
        raise RuntimeWarning("This step did not generate the correct args for next step to run. This might disrupt "
                             "the workflow")

    # TODO: Metadata needed for client API to run needs to be persisted outside before this
    print("--------------- RUNNING: CLEANUP OF TEMP DIR----------")
    subprocess.call(["rm -r /opt/zillow/.metaflow"], shell=True)

    print("_______________ Done _________________________________")
    return step_output(outputs[0], outputs[1], outputs[2], outputs[3], outputs[4], outputs[5])

def pre_start_op_func(code_url)  -> NamedTuple('StepOutput', [('ds_root', str), ('run_id', str), ('next_step', str), ('next_task_id', str), ('current_step', str), ('current_task_id', str)]):
    """
    Function used to create a KFP container op (see `pre_start_container_op`)that corresponds to the `pre-start` step of metaflow

    """

    import subprocess
    from collections import namedtuple

    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    subprocess.call(["curl -o helloworld.py {}".format(code_url)], shell=True)

    print("\n----------RUNNING: KFP Installation---------------")
    subprocess.call(["pip3 install kfp"], shell=True) # TODO: Remove this once KFP is added to dependencies

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    subprocess.call(["pip3 install --user --upgrade git+https://github.com/zillow/metaflow.git@state-integ-s3"],
                    shell=True)

    print("\n----------RUNNING: MAIN STEP COMMAND--------------")
    S3_BUCKET = "s3://workspace-zillow-analytics-stage/aip/metaflow"
    S3_AWS_ARN = "arn:aws:iam::170606514770:role/dev-zestimate-role"

    define_s3_env_vars = 'export METAFLOW_DATASTORE_SYSROOT_S3="{}" && export METAFLOW_AWS_ARN="{}" '.format(S3_BUCKET,
                                                                                                          S3_AWS_ARN)
    define_username = 'export USERNAME="kfp-user"'
    python_cmd = 'python helloworld.py --datastore="s3" --datastore-root="{}" pre-start'.format(S3_BUCKET)
    final_run_cmd = f'{define_username} && {define_s3_env_vars} && {python_cmd}'

    print("RUNNING COMMAND: ", final_run_cmd)
    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    proc_output = proc.stdout
    proc_error = proc.stderr

    step_output = namedtuple('StepOutput',
                             ['ds_root', 'run_id', 'next_step', 'next_task_id', 'current_step', 'current_task_id'])

    # TODO: Check why MF echo statements are getting redirected to stderr
    if len(proc_error) > 1:
        print("_____________ STDERR:_____________________________")
        print(proc_error)

    if len(proc_output) > 1:
        print("______________ STDOUT:____________________________")
        print(proc_output)
        # Read in the outputs (in the penultimate line) containing args needed for the next step.
        # Note: Output format is: ['ds_root', 'run_id', 'next_step', 'next_task_id', 'current_step', 'current_task_id']
        outputs = (proc_output.split("\n")[-2]).split() # this contains the args needed for next step to run
    else:
        raise RuntimeWarning("This step did not generate the correct args for next step to run. This might disrupt the workflow")

    # TODO: Metadata needed for client API to run needs to be persisted outside before this
    print("--------------- RUNNING: CLEANUP OF TEMP DIR----------")
    subprocess.call(["rm -r /opt/zillow/.metaflow"], shell=True)

    print("_______________ Done __________________________")
    return step_output(outputs[0], outputs[1], outputs[2], outputs[3], outputs[4], outputs[5])

def step_container_op():
    """
    Container op that corresponds to a step defined in the Metaflow flowgraph.

    Note: The public docker image is a copy of the internal docker image we were using (borrowed from aip-kfp-example).
    """

    step_op =  kfp.components.func_to_container_op(step_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return step_op

def pre_start_container_op():
    """
    Container op that corresponds to the 'pre-start' step of Metaflow.

    Note: The public docker image is a copy of the internal docker image we were using (borrowed from aip-kfp-example).
    """

    pre_start_op = kfp.components.func_to_container_op(pre_start_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return pre_start_op


def create_flow_pipeline(ordered_steps, flow_code_url=DEFAULT_FLOW_CODE_URL):
    """
    Function used to create the KFP flow pipeline. Return the function defining the KFP equivalent of the flow
    """

    steps = ordered_steps
    code_url = flow_code_url
    print("\nCreating the pipeline definition needed to run the flow on KFP...\n")
    print("\nCode URL of the flow to be converted to KFP: {0}\n".format(flow_code_url))

    @dsl.pipeline(
        name='MF on KFP Pipeline',
        description='Pipeline defining KFP equivalent of the Metaflow flow'
    )
    def kfp_pipeline_from_flow():
        """
        This function converts the flow steps to kfp container op equivalents
        by invoking `step_container_op` for every step in the flow and handling the order of steps.
        """

        step_container_ops = []
        pre_start_op = (pre_start_container_op())(code_url)
        step_container_ops.append(pre_start_op)

        for step in steps:
            prev_step_outputs = step_container_ops[-1].outputs
            step_container_ops.append(
                (step_container_op())(step, code_url,
                      prev_step_outputs['ds_root'],
                      prev_step_outputs['run_id'],
                      prev_step_outputs['next_task_id'],
                      prev_step_outputs['current_step'],
                      prev_step_outputs['current_task_id']
                      )
                )
            step_container_ops[-1].set_display_name(step)
            step_container_ops[-1].after(step_container_ops[-2])

    return kfp_pipeline_from_flow

def create_run_on_kfp(flowgraph, code_url, experiment_name, run_name):
    """
    Creates a new run on KFP using the `kfp.Client()`. Note: Intermediate pipeline YAML is not generated as this creates
    the run directly using the pipeline function returned by `create_flow_pipeline`

    """

    pipeline_func = create_flow_pipeline(get_ordered_steps(flowgraph), code_url)
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
    pipeline_func = create_flow_pipeline(get_ordered_steps(flowgraph), code_url)

    kfp.compiler.Compiler().compile(pipeline_func, pipeline_file_path)
    return pipeline_file_path
