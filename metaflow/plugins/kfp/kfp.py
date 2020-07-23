import kfp
from kfp import dsl

from .constants import DEFAULT_RUN_NAME, DEFAULT_EXPERIMENT_NAME, DEFAULT_FLOW_CODE_URL, DEFAULT_KFP_YAML_OUTPUT_PATH

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


def step_container_op(step_name, code_url=DEFAULT_FLOW_CODE_URL):
    """
    Method to create a kfp container op that corresponds to a single step in the flow.

    TODO: This does not maintain state. The custom pre-start command used below would be removed once we have state accessible across KFP steps.
    TODO: The public docker is a copy of the internal docker image we were using (borrowed from aip-kfp-example). Check if any stage here may need to be further modified later.
    """

    python_cmd = """ "python helloworld.py --datastore-root ", $1, " step {} --run-id ", $2, " --task-id ", $4, " --input-paths", $2"/"$5"/"$6 """.format(
        step_name)
    command_inside_awk = """ {{ print {0} }}""".format(python_cmd)
    final_run_cmd = """ python helloworld.py pre-start | awk 'END{}' | sh """.format(command_inside_awk)

    return dsl.ContainerOp(

        name='StepRunner-{}'.format(step_name),
        image='ssreejith3/mf_on_kfp:python-curl-git',
        command= ['sh', '-c'],
        arguments=[
            'curl -o helloworld.py {}' \
            ' && pip install git+https://github.com/zillow/metaflow.git@c722fceffa3011ecab68ce319cff98107cc49532' \
            ' && export USERNAME="kfp-user" ' \
            ' && {}'.format(code_url, final_run_cmd)
            ])


def create_flow_pipeline(ordered_steps, flow_code_url=DEFAULT_FLOW_CODE_URL):
    """
    Function that creates the KFP flow pipeline and returns the path to the YAML file containing the pipeline specification.
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

        # Store the list of steps in reverse order
        step_container_ops = [step_container_op(step, code_url) for step in reversed(steps)]

        # Each step in the list can only be executed after the next step in the list, i.e., list[-1] is executed first, followed
        # by list[-2] and so on.
        for i in range(len(steps) - 1):
            step_container_ops[i].after(step_container_ops[i + 1])

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
