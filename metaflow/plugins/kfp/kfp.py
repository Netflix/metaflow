import kfp
from kfp import dsl
from kubernetes.client.models import V1EnvVar

from .constants import DEFAULT_FLOW_CODE_URL, DEFAULT_KFP_YAML_OUTPUT_PATH, DEFAULT_DOWNLOADED_FLOW_FILENAME
from metaflow.metaflow_config import METAFLOW_AWS_ARN, METAFLOW_AWS_S3_REGION, DATASTORE_SYSROOT_S3

from collections import deque

def step_op_func(python_cmd_template, step_name: str,
                 code_url: str,
                 kfp_run_id: str,
                 ):
    """
    Function used to create a KFP container op (see: `step_container_op`) that corresponds to a single step in the flow.
    """
    import subprocess
    import os

    MODIFIED_METAFLOW_URL = 'git+https://github.com/zillow/metaflow.git@bug/add_namespace_to_kfp_run'
    DEFAULT_DOWNLOADED_FLOW_FILENAME = 'downloaded_flow.py'

    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    subprocess.call(
        ["curl -o {downloaded_file_name} {code_url}".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            code_url=code_url)], shell=True)

    print("\n----------RUNNING: KFP Installation---------------")
    subprocess.call(["pip3 install kfp"], shell=True)  # TODO: Remove this once KFP is added to dependencies

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    subprocess.call(["pip3 install --user --upgrade {modified_metaflow_git_url}".format(
        modified_metaflow_git_url=MODIFIED_METAFLOW_URL)], shell=True)

    print("\n----------RUNNING: MAIN STEP COMMAND--------------")

    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_AWS_ARN = os.getenv("S3_AWS_ARN")
    S3_AWS_REGION = os.getenv("S3_AWS_REGION")

    define_s3_env_vars = 'export METAFLOW_DATASTORE_SYSROOT_S3="{}" && export METAFLOW_AWS_ARN="{}" ' \
                         '&& export METAFLOW_AWS_S3_REGION="{}"'.format(S3_BUCKET, S3_AWS_ARN, S3_AWS_REGION)
    define_username = 'export USERNAME="kfp-user"' # TODO: Map username to KFP specific user/profile/namespace
    python_cmd = python_cmd_template.format(ds_root=S3_BUCKET, run_id=kfp_run_id)

    final_run_cmd = "{define_username} && {define_s3_env_vars} && {python_cmd}".format(define_username=define_username,
                                                                                       define_s3_env_vars=define_s3_env_vars,
                                                                                       python_cmd=python_cmd)

    print("RUNNING COMMAND: ", final_run_cmd)
    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    proc_output = proc.stdout
    proc_error = proc.stderr

    if len(proc_error) > 1:
        print("_______________STDERR:_____________________________")
        print(proc_error)

    if len(proc_output) > 1:
        print("_______________STDOUT:_____________________________")
        print(proc_output)

    # TODO: Metadata needed for client API to run needs to be persisted outside before return

    print("_______________ Done _________________________________")
    # END is the final step
    if step_name.lower() == 'end':
        print("_______________ FLOW RUN COMPLETE ________________")


def start_op_func(start_command_template: str, code_url: str, kfp_run_id: str):
    """
    Function used to create a KFP container op corresponding to the 'start' step of the flow.
    This function also defines the execution of an init step which is needed before the 'start' step
    executes in order to persist parameters of the flow (i.e., class level variables).
    """
    import subprocess
    import os

    def execute(cmd):
        """
        Helper function to run the given command and print `stderr` and `stdout`.
        """
        proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        proc_output = proc.stdout
        proc_error = proc.stderr

        if len(proc_error) > 1:
            print("_____________ STDERR:_____________________________")
            print(proc_error)

        if len(proc_output) > 1:
            print("______________ STDOUT:____________________________")
            print(proc_output)

    MODIFIED_METAFLOW_URL = 'git+https://github.com/zillow/metaflow.git@@bug/add_namespace_to_kfp_run'
    DEFAULT_DOWNLOADED_FLOW_FILENAME = 'downloaded_flow.py'

    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    subprocess.call(
        ["curl -o {downloaded_file_name} {code_url}".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            code_url=code_url)], shell=True)

    print("\n----------RUNNING: KFP Installation---------------")
    subprocess.call(["pip3 install kfp"], shell=True)  # TODO: Remove this once KFP is added to dependencies

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    subprocess.call(["pip3 install --user --upgrade {modified_metaflow_git_url}".format(
        modified_metaflow_git_url=MODIFIED_METAFLOW_URL)], shell=True)

    print("\n----------RUNNING: INIT COMMAND-------------------")

    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_AWS_ARN = os.getenv("S3_AWS_ARN")
    S3_AWS_REGION = os.getenv("S3_AWS_REGION")

    define_s3_env_vars = 'export METAFLOW_DATASTORE_SYSROOT_S3="{}" && export METAFLOW_AWS_ARN="{}" ' \
                         '&& export METAFLOW_AWS_S3_REGION="{}"'.format(S3_BUCKET, S3_AWS_ARN, S3_AWS_REGION)
    define_username = 'export USERNAME="kfp-user"' # TODO: Map username to KFP specific user/profile/namespace
    init_cmd = 'python {0} --datastore="s3" --datastore-root="{1}" init --run-id={2} --task-id=0'.format(DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                                                                         S3_BUCKET, kfp_run_id)
    final_init_cmd = "{define_username} && {define_s3_env_vars} && {init_cmd}".format(define_username=define_username,
                                                                                      define_s3_env_vars=define_s3_env_vars,
                                                                                      init_cmd=init_cmd)

    print("RUNNING COMMAND: ", final_init_cmd)
    execute(final_init_cmd)

    print("\n----------RUNNING: MAIN STEP COMMAND----------------")
    start_cmd = start_command_template.format(ds_root=S3_BUCKET, run_id=kfp_run_id)
    final_run_cmd = "{define_username} && {define_s3_env_vars} && {start_cmd}".format(define_username=define_username,
                                                                                      define_s3_env_vars=define_s3_env_vars,
                                                                                      start_cmd=start_cmd)
    print("RUNNING COMMAND: ", final_run_cmd)
    execute(final_run_cmd)

    # TODO: Metadata needed for client API to run needs to be persisted outside before return

    print("_______________ Done __________________________")


def step_container_op():
    """
    Container op that corresponds to a step defined in the Metaflow flowgraph.

    Note: The public docker image is a copy of the internal docker image we were using (borrowed from aip-kfp-example).
    """

    step_op = kfp.components.func_to_container_op(step_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return step_op


def start_container_op():
    """
    Container op that corresponds to the 'start' step of Metaflow.

    Note: The public docker image is a copy of the internal docker image we were using (borrowed from aip-kfp-example).
    """

    start_op = kfp.components.func_to_container_op(start_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return start_op


def add_env_variables_transformer(container_op):
    """
    Add environment variables to the container op.
    """

    container_op.add_env_variable(V1EnvVar(name="S3_BUCKET", value=DATASTORE_SYSROOT_S3))
    container_op.add_env_variable(V1EnvVar(name="S3_AWS_ARN", value=METAFLOW_AWS_ARN))
    container_op.add_env_variable(V1EnvVar(name="S3_AWS_REGION", value=METAFLOW_AWS_S3_REGION))
    return container_op


def create_command_templates_from_graph(graph):
    """
    Create a map of steps to their corresponding command templates. These command templates help define the command
    to be used to run that particular step with placeholders for the `run_id` and `datastore_root` (location of the datastore).

    # Note:
    # Level-order traversal is adopted to keep the task-ids in line with what happens during a local metaflow execution.
    # It is not entirely necessary to keep this order of task-ids if we are able to point to the correct input-paths for
    # each step. But, using this ordering does keep the organization of data in the datastore more understandable and
    # natural (i.e., `start` gets a task id of 1, next step gets a task id of 2 and so on with 'end' step having the
    # highest task id. So the paths in the datastore look like: {run-id}/start/1, {run-id}/next-step/2, and so on)
    """

    def build_cmd_template(step_name, task_id, input_paths):
        """
        Returns the python command template to be used for each step.

        This method returns a string with placeholders for `datastore_root` and `run_id`
        which get populated using the provided config and the kfp run ID respectively.
        The rest of the command string is populated using the passed arguments which are known before the run starts.

        An example constructed command template (to run a step named `hello`):
        "python downloaded_flow.py --datastore s3 --datastore-root {ds_root} " \
                         "step hello --run-id {run_id} --task-id 2 " \
                         "--input-paths {run_id}/start/1"
        """

        python_cmd = "python {downloaded_file_name} --datastore s3 --datastore-root {{ds_root}} " \
                     "step {step_name} --run-id {{run_id}} --task-id {task_id} " \
                     "--input-paths {input_paths}".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            step_name=step_name, task_id=task_id, input_paths=input_paths)
        return python_cmd

    steps_deque = deque(['start']) # deque to process the DAG in level order
    current_task_id = 0

    # set of seen steps, i.e., added to the queue for processing
    seen_steps = set(['start'])
    # Mapping of steps to task ids
    step_to_task_id_map = {}
    # Mapping of steps to their command templates
    step_to_command_template_map = {}

    while len(steps_deque) > 0:
        current_step = steps_deque.popleft()
        current_task_id += 1
        step_to_task_id_map[current_step] = current_task_id
        current_node = graph.nodes[current_step]

        # Generate the correct input_path for each step. Note: input path depends on a step's parents (i.e., in_funcs)
        # Format of the input-paths for reference:
        # non-join nodes: "run-id/parent-step/parent-task-id",
        # branch-join node: "run-id/:p1/p1-task-id,p2/p2-task-id,..."
        # foreach node: TODO: foreach is not considered here
        if current_task_id == 1: # start step
            cur_input_path = '{run_id}/_parameters/0' # this is the standard input path for the `start` step
        else:
            if current_node.type == 'join':
                cur_input_path = '{run_id}/:'
                for parent_step in current_node.in_funcs:
                    cur_input_path += "{parent}/{parent_task_id},".format(parent=parent_step,
                                                                          parent_task_id=str(step_to_task_id_map[parent_step]))
                cur_input_path = cur_input_path.strip(',')
            else:
                parent_step = current_node.in_funcs[0]
                cur_input_path = "{{run_id}}/{parent}/{parent_task_id}".format(parent=parent_step,
                                                                               parent_task_id=str(step_to_task_id_map[parent_step]))

        step_to_command_template_map[current_step] = build_cmd_template(current_step, current_task_id, cur_input_path)

        for step in current_node.out_funcs:
            if step not in seen_steps:
                steps_deque.append(step)
                seen_steps.add(step)

    return step_to_command_template_map


def create_kfp_pipeline_from_flow_graph(flow_graph, code_url=DEFAULT_FLOW_CODE_URL):

    step_to_command_template_map = create_command_templates_from_graph(flow_graph)

    @dsl.pipeline(
        name='MF on KFP Pipeline',
        description='Pipeline defining KFP equivalent of the Metaflow flow. Currently supports linear flows and flows '
                    'with branch and join nodes'
    )
    def kfp_pipeline_from_flow():
        kfp_run_id = 'kfp-' + dsl.RUN_ID_PLACEHOLDER
        # Start step (start is a special step as additional initialisation is done internally)
        step_to_container_op_map = {}
        step_to_container_op_map['start'] = (start_container_op())(step_to_command_template_map['start'],
                                                                      code_url,
                                                                      kfp_run_id
                                                                    ).set_display_name('start')

        # Define container ops for remaining steps
        for step, cmd in step_to_command_template_map.items():
            if step != 'start':
                step_to_container_op_map[step] = (step_container_op())(
                                                    step_to_command_template_map[step],
                                                    step,
                                                    code_url,
                                                    kfp_run_id
                                                ).set_display_name(step)

        # Add environment variables to all ops
        dsl.get_pipeline_conf().add_op_transformer(add_env_variables_transformer)

        # Define ordering of container op execution
        for step in flow_graph.nodes.keys():
            if step != 'start':
                for parent in flow_graph.nodes[step].in_funcs:
                    step_to_container_op_map[step].after(step_to_container_op_map[parent])

    return kfp_pipeline_from_flow


def create_run_on_kfp(flow_graph, code_url, experiment_name, run_name, namespace, userid):
    """
    Creates a new run on KFP using the `kfp.Client()`. Note: Intermediate pipeline YAML is not generated as this creates
    the run directly using the pipeline function returned by `create_flow_pipeline`
    """

    pipeline_func = create_kfp_pipeline_from_flow_graph(flow_graph, code_url)
    run_pipeline_result = kfp.Client(userid=userid).create_run_from_pipeline_func(pipeline_func,
                                                                     arguments={},
                                                                     experiment_name=experiment_name,
                                                                     run_name=run_name,
                                                                     namespace=namespace)
    return run_pipeline_result


def create_kfp_pipeline_yaml(flow_graph, code_url, pipeline_file_path=DEFAULT_KFP_YAML_OUTPUT_PATH):
    """
    Creates a new KFP pipeline YAML using `kfp.compiler.Compiler()`. Note: Intermediate pipeline YAML is saved
    at `pipeline_file_path`
    """
    pipeline_func = create_kfp_pipeline_from_flow_graph(flow_graph, code_url)

    kfp.compiler.Compiler().compile(pipeline_func, pipeline_file_path)
    return pipeline_file_path
