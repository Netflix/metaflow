import kfp
from kfp import dsl

from .constants import DEFAULT_FLOW_CODE_URL, DEFAULT_KFP_YAML_OUTPUT_PATH
from typing import NamedTuple

def step_op_func(python_cmd_template, step_name: str,
                 code_url: str,
                 ds_root: str,
                 run_id: str,
               ):
    """
    Function used to create a KFP container op (see: `step_container_op`) that corresponds to a single step in the flow.

    """
    import subprocess

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
    python_cmd = python_cmd_template.format(ds_root=ds_root, run_id=run_id)

    final_run_cmd = f'{define_username} && {define_s3_env_vars} && {python_cmd}'

    print("RUNNING COMMAND: ", final_run_cmd)
    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    proc_output = proc.stdout
    proc_error = proc.stderr

    # END is the final step and no outputs need to be returned
    if step_name.lower() == 'end':
        print("_______________ FLOW RUN COMPLETE ________________")
        # return step_output('None', 'None', 'None', 'None', 'None', 'None')

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

def pre_start_op_func(code_url)  -> NamedTuple('StepOutput', [('ds_root', str), ('run_id', str)]):
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
                             ['ds_root', 'run_id'])

    # TODO: Check why MF echo statements are getting redirected to stderr
    if len(proc_error) > 1:
        print("_____________ STDERR:_____________________________")
        print(proc_error)

    if len(proc_output) > 1:
        print("______________ STDOUT:____________________________")
        print(proc_output)
        outputs = (proc_output.split("\n")[-2]).split() # this contains the args needed for next steps to run

    else:
        raise RuntimeWarning("This step did not generate the correct args for next step to run. This might disrupt the workflow")

    # TODO: Metadata needed for client API to run needs to be persisted outside before this
    print("--------------- RUNNING: CLEANUP OF TEMP DIR----------")
    subprocess.call(["rm -r /opt/zillow/.metaflow"], shell=True)

    print("_______________ Done __________________________")
    return step_output(outputs[0], outputs[1])

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

def create_flow_from_graph(flowgraph, flow_code_url=DEFAULT_FLOW_CODE_URL):
    from collections import deque
    graph = flowgraph
    code_url = flow_code_url

    def build_cmd_template(step_name, task_id, input_paths):
        python_cmd = "python helloworld.py --datastore s3 --datastore-root {{ds_root}} step {step_name} --run-id {{run_id}} --task-id {task_id} --input-paths {input_paths}".format(step_name=step_name, task_id=task_id, input_paths=input_paths )
        return python_cmd

    # Note: Below, we process the graph in level order and save the command templates to be used for each step of the graph.
    # Level-order traversal is adopted to keep the task-ids in line with what happens during a local metaflow execution.
    # It may not be necessary to keep this order of task-ids (TODO: verify importance of task-id ordering) as long as we
    # are able to point to the correct input-paths for each step.

    dq = deque(['start']) # deque to process the DAG in level order
    cur_task_id = 0
    cur_input_path = '' # non-join node: run-id/parent-step/parent-task-id, branch-join: run-id/:p1/p1-task-id,p2/p2-task-id,...

    # set of seen steps, i.e., added to the queue for processing
    seen = set(['start'])

    # Mapping of steps to task ids
    task_id_map = {}
    # Mapping of steps to their command templates
    command_template_map = {}

    while len(dq) > 0:
        cur_step = dq.popleft()
        cur_task_id += 1
        task_id_map[cur_step] = cur_task_id

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
            if step not in seen:
                dq.append(step)
                seen.add(step)

    @dsl.pipeline(
        name='MF on KFP Pipeline',
        description='Pipeline defining KFP equivalent of the Metaflow flow'
    )
    def kfp_pipeline_from_flow():
        pre_start_op = (pre_start_container_op())(code_url)
        outputs = pre_start_op.outputs

        container_op_map = {}
        ds_root = outputs['ds_root']
        run_id = outputs['run_id']

        # Initial:
        pre_start_op.set_display_name('InitialSetup')
        container_op_map['start'] = (step_container_op())(command_template_map['start'], 'start', code_url, ds_root, run_id)
        container_op_map['start'].set_display_name('start')
        container_op_map['start'].after(pre_start_op)

        for step, cmd in command_template_map.items():
            if step not in container_op_map.keys():
                container_op_map[step] = (step_container_op())(command_template_map[step], step, code_url, ds_root, run_id)
                container_op_map[step].set_display_name(step)
                for parent in graph.nodes[step].in_funcs:
                    container_op_map[step].after(container_op_map[parent])

    return kfp_pipeline_from_flow

def create_flow_pipeline_alternate(mf_graph, flow_code_url=DEFAULT_FLOW_CODE_URL):
    """
    Function used to create the KFP flow pipeline. Return the function defining the KFP equivalent of the flow
    """

    graph = mf_graph
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

        from collections import deque
        import ast
        import json

        dq = deque([pre_start_op])
        seen_in_funcs = set([])
        input_paths_join = None
        is_branch_join = True # Ignore foreach for now
        pre_join_tasks = []

        while len(dq) > 0:
            prev_task = dq.popleft()
            prev_step_outputs = prev_task.outputs

            out_funcs = json.loads(prev_step_outputs['next_step'])
            next_task_ids = json.loads(prev_step_outputs['next_task_id'])
            out_funcs = ast.literal_eval(str(out_funcs))

            for i, step in enumerate(out_funcs):
                node_type = graph.nodes[step].type
                node_in_funcs = graph.nodes[step].in_funcs
                if len(node_in_funcs) <= 1: # non-join nodes
                    # create the current task
                    # no dependencies as this is start step
                    input_path = prev_step_outputs['ds_root'] + "/" + prev_step_outputs['current_step'] + "/" + prev_step_outputs['current_task_id']  # runid/step/taskid
                    current_task = (step_container_op())(
                        step,
                        code_url,
                        prev_step_outputs['ds_root'],
                        prev_step_outputs['run_id'],
                        next_task_ids[0], # there will only be one as it's the start step
                        input_path
                    )
                    current_task.set_display_name(step)
                    current_task.after(prev_task)
                    dq.append(current_task)

                else: # join
                    if input_paths_join is None and is_branch_join:
                        input_paths_join = prev_step_outputs['run_id'] + "/:"
                    seen_in_funcs.add(prev_step_outputs['current_step'])
                    input_paths_join += prev_step_outputs['current_step'] + "/" + prev_step_outputs['current_task_id']
                    pre_join_tasks.append(prev_task)
                    if seen_in_funcs == node_in_funcs: # then the join has all the data it needs to start execution
                        current_task =  (step_container_op())(
                                            step,
                                            code_url,
                                            prev_step_outputs['ds_root'],
                                            prev_step_outputs['run_id'],
                                            next_task_ids[0],  # new task id - check if this is always correct
                                            input_paths_join
                                        )
                        current_task.set_display_name(step)
                        for pre_join_task in pre_join_tasks:
                            current_task.after(pre_join_task)
                        # Reset to prep for the next join
                        input_paths_join = None
                        seen_in_funcs = set([])
                        pre_join_tasks = []
                    else:
                        # There's more to come. So, add a comma
                        input_paths_join += ','

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
