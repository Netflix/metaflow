import inspect
import os
import random
import string
import sys
from collections import deque
from pathlib import Path
from typing import NamedTuple

import kfp
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3

from .constants import DEFAULT_KFP_YAML_OUTPUT_PATH
from ... import R
from ...graph import DAGNode


class KfpComponent(object):
    def __init__(self, name, step_command, total_retries):
        self.name = name
        self.step_command = step_command
        self.total_retries = total_retries


class KubeflowPipelines(object):
    def __init__(
        self,
        name,
        graph,
        flow,
        code_package,
        code_package_url,
        metadata,
        datastore,
        environment,
        event_logger,
        monitor,
        base_image=None,
        s3_code_package=True,
        namespace=None,
        api_namespace=None,
        username=None,
        **kwargs,
    ):
        """
        Analogous to step_functions_cli.py
        """
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package = code_package
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.datastore = datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.namespace = namespace
        self.username = username
        self.base_image = base_image
        self.s3_code_package = s3_code_package

        self._client = kfp.Client(namespace=api_namespace, userid=username, **kwargs)

    def create_run_on_kfp(self, experiment_name, run_name):
        """
        Creates a new run on KFP using the `kfp.Client()`.
        """
        run_pipeline_result = self._client.create_run_from_pipeline_func(
            pipeline_func=self.create_kfp_pipeline_from_flow_graph(),
            arguments={"datastore_root": DATASTORE_SYSROOT_S3},
            experiment_name=experiment_name,
            run_name=run_name,
            namespace=self.namespace,
        )
        return run_pipeline_result

    def create_kfp_pipeline_yaml(self, pipeline_file_path=DEFAULT_KFP_YAML_OUTPUT_PATH):
        """
        Creates a new KFP pipeline YAML using `kfp.compiler.Compiler()`.
        Note: Intermediate pipeline YAML is saved at `pipeline_file_path`
        """
        kfp.compiler.Compiler().compile(
            self.create_kfp_pipeline_from_flow_graph(), pipeline_file_path
        )
        return os.path.abspath(pipeline_file_path)

    def _command(self, code_package_url, environment, step_name, step_cli):
        """
        Analogous to batch.py
        """
        commands = (
            environment.get_package_commands(code_package_url)
            if self.s3_code_package
            else ["cd " + str(Path(inspect.getabsfile(self.flow.__class__)).parent)]
        )
        commands.extend(environment.bootstrap_commands(step_name))
        commands.append("echo 'Task is starting.'")
        commands.extend(step_cli)
        return " && ".join(commands)

    @staticmethod
    def _get_retries(node):
        """
        Analogous to step_functions_cli.py
        """
        max_user_code_retries = 0
        max_error_retries = 0
        # Different decorators may have different retrying strategies, so take
        # the max of them.
        for deco in node.decorators:
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        return max_user_code_retries, max_user_code_retries + max_error_retries

    def create_kfp_components_from_graph(self):
        """
        Create a map of steps to their corresponding KfpComponent. The KfpComponent defines the component
        attributes and step command to be used to run that particular step with placeholders for the `run_id`.

        # Note:
        # Level-order traversal is adopted to keep the task-ids in line with what happens during a local metaflow execution.
        # It is not entirely necessary to keep this order of task-ids if we are able to point to the correct input-paths for
        # each step. But, using this ordering does keep the organization of data in the datastore more understandable and
        # natural (i.e., `start` gets a task id of 1, next step gets a task id of 2 and so on with 'end' step having the
        # highest task id. So the paths in the datastore look like: {run-id}/start/1, {run-id}/next-step/2, and so on)
        """

        def build_kfp_component(node, step_name, task_id, input_paths):
            """
            Returns the KfpComponent for each step.

            This method returns a string with placeholders for `run_id` and
            `task_id` which get populated using the provided config and the kfp
            run ID respectively.  The rest of the command string is populated
            using the passed arguments which are known before the run starts.

            An example constructed command template (to run a step named `hello`):
            "python downloaded_flow.py --datastore s3 --datastore-root {ds_root} " \
                             "step hello --run-id {run_id} --task-id 2 " \
                             "--input-paths {run_id}/start/1"
            """

            # TODO: @schedule, @environment, @resources, @timeout, @catch, etc.
            # Resolve retry strategy.
            user_code_retries, total_retries = KubeflowPipelines._get_retries(node)

            step_cli = self._step_cli(node, input_paths, user_code_retries, task_id)

            return KfpComponent(
                node.name,
                self._command(
                    self.code_package_url, self.environment, step_name, [step_cli]
                ),
                total_retries,
            )

        steps_deque = deque(["start"])  # deque to process the DAG in level order
        current_task_id = 0

        # set of seen steps, i.e., added to the queue for processing
        seen_steps = set(["start"])
        # Mapping of steps to task ids
        step_to_task_id_map = {}
        # Mapping of steps to their KfpComponent
        step_to_kfp_component_map = {}

        while len(steps_deque) > 0:
            current_step = steps_deque.popleft()
            current_task_id += 1
            step_to_task_id_map[current_step] = current_task_id
            current_node = self.graph.nodes[current_step]

            # Generate the correct input_path for each step. Note: input path depends on a step's parents (i.e., in_funcs)
            # Format of the input-paths for reference:
            # non-join nodes: "run-id/parent-step/parent-task-id",
            # branch-join node: "run-id/:p1/p1-task-id,p2/p2-task-id,..."
            # foreach node: TODO: foreach is not considered here
            if current_task_id == 1:  # start step
                # this is the standard input path for the `start` step
                cur_input_path = "{run_id}/_parameters/0"
            else:
                if current_node.type == "join":
                    cur_input_path = "{run_id}/:"
                    for parent_step in current_node.in_funcs:
                        cur_input_path += "{parent}/{parent_task_id},".format(
                            parent=parent_step,
                            parent_task_id=str(step_to_task_id_map[parent_step]),
                        )
                    cur_input_path = cur_input_path.strip(",")
                else:
                    parent_step = current_node.in_funcs[0]
                    cur_input_path = "{{run_id}}/{parent}/{parent_task_id}".format(
                        parent=parent_step,
                        parent_task_id=str(step_to_task_id_map[parent_step]),
                    )

            step_to_kfp_component_map[current_step] = (
                build_kfp_component(
                    current_node, current_step, current_task_id, cur_input_path
                ),
                current_step,
                current_task_id,
            )

            for step in current_node.out_funcs:
                if step not in seen_steps:
                    steps_deque.append(step)
                    seen_steps.add(step)

        return step_to_kfp_component_map

    def _step_cli(self, node, input_paths, user_code_retries, task_id):
        """
        Analogous to step_functions_cli.py
        This returns the command line to run the internal Metaflow step click entrypiont.
        """
        from kfp import dsl

        cmds = []

        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)

        if R.use_r():
            entrypoint = [R.entrypoint()]
        else:
            entrypoint = [executable, script_name]

        paths = input_paths
        kfp_run_id = "kfp-" + dsl.RUN_ID_PLACEHOLDER

        # TODO: use dynamic task_id
        #   But then we must save it in Dynamo (a kv store) for joins
        #   task_id = "{{pod.name}}"  # ARGO pod name
        if node.name == "start":
            # We need a separate unique ID for the special _parameters task
            task_id_params = "%s-params" % task_id
            # Export user-defined parameters into runtime environment
            param_file = "".join(
                random.choice(string.ascii_lowercase) for _ in range(10)
            )

            # TODO: move to KFP plugin
            export_params = (
                "python -m "
                "metaflow.plugins.aws.step_functions.set_batch_environment "
                "parameters %s && . `pwd`/%s" % (param_file, param_file)
            )
            params = entrypoint + [
                "--quiet",
                "--metadata=%s" % self.metadata.TYPE,
                "--environment=%s" % self.environment.TYPE,
                "--datastore=s3",
                "--datastore-root={datastore_root}",
                "--event-logger=%s" % self.event_logger.logger_type,
                "--monitor=%s" % self.monitor.monitor_type,
                "--no-pylint",
                "init",
                "--run-id %s" % kfp_run_id,
                "--task-id %s" % task_id_params,
            ]

            # If the start step gets retried, we must be careful not to
            # regenerate multiple parameters tasks. Hence we check first if
            # _parameters exists already.
            task_id_params_path = "{kfp_run_id}/_parameters/{task_id_params}".format(
                kfp_run_id=kfp_run_id, task_id_params=task_id_params
            )
            exists = entrypoint + ["dump", "--max-value-size=0", task_id_params_path]
            cmd = "if ! %s >/dev/null 2>/dev/null; then %s && %s; fi" % (
                " ".join(exists),
                export_params,
                " ".join(params),
            )
            cmds.append(cmd)
            paths = task_id_params_path

        if node.type == "join" and self.graph[node.split_parents[-1]].type == "foreach":
            # TODO: get from Dynamo or a kv store
            pass
            # export_parent_tasks = \
            #     'python -m ' \
            #     'metaflow.plugins.aws.step_functions.set_batch_environment ' \
            #     'parent_tasks %s && . `pwd`/%s' \
            #         % (parent_tasks_file, parent_tasks_file)
            # cmds.append(export_parent_tasks)

        top_level = [
            "--quiet",
            "--metadata=%s" % self.metadata.TYPE,
            "--environment=%s" % self.environment.TYPE,
            "--datastore=s3",
            "--datastore-root={datastore_root}",
            "--event-logger=%s" % self.event_logger.logger_type,
            "--monitor=%s" % self.monitor.monitor_type,
            "--no-pylint",
            "--with=kfp_internal",
        ]

        step = [
            "step",
            node.name,
            "--run-id %s" % kfp_run_id,
            "--task-id %s" % task_id,
            # Since retries are handled by KFP Argo, we can rely on
            # {{retries}} as the job counter.
            # '--retry-count {{retries}}',  # TODO: test verify, should it be -1?
            "--max-user-code-retries %d" % user_code_retries,
            "--input-paths %s" % paths,
        ]

        if any(self.graph[n].type == "foreach" for n in node.in_funcs):
            # We set the `METAFLOW_SPLIT_INDEX` through JSONPath-foo
            # to pass the state from the parent DynamoDb state for for-each.
            step.append(
                "--split-index $METAFLOW_SPLIT_INDEX"
            )  # TODO: get from KFP param
        if self.namespace:
            step.append("--namespace %s" % self.namespace)
        cmds.append(" ".join(entrypoint + top_level + step))
        return " && ".join(cmds)

    def create_kfp_pipeline_from_flow_graph(self):
        import kfp
        from kfp import dsl

        step_to_kfp_component_map = self.create_kfp_components_from_graph()

        # Container op that corresponds to a step defined in the Metaflow flowgraph.
        step_op = kfp.components.func_to_container_op(
            step_op_func, base_image=self.base_image
        )

        @dsl.pipeline(name=self.name, description=self.graph.doc)
        def kfp_pipeline_from_flow(datastore_root: str = DATASTORE_SYSROOT_S3):
            kfp_run_id = "kfp-" + dsl.RUN_ID_PLACEHOLDER

            visited = {}

            def build_kfp_dag(node: DAGNode, context: str, index=None):
                kfp_component, step_name, task_id = step_to_kfp_component_map[node.name]
                visited[node.name] = step_op(
                    datastore_root,
                    kfp_component.step_command,
                    kfp_run_id,
                    context,
                    self.flow.name,
                    step_name,
                    str(task_id),
                    index=index,
                ).set_display_name(node.name)

                if node.type == "foreach":
                    with kfp.dsl.ParallelFor(
                        visited[node.name].outputs["split_indexes"]
                    ) as index:
                        for step in node.out_funcs:
                            build_kfp_dag(
                                self.graph[step],
                                visited[node.name].outputs["task_out_dict"],
                                index,
                            )
                    # TODO: the join
                    # visited[node.name].outputs["task_out_dict"]
                else:
                    for step in node.out_funcs:
                        if step not in visited:
                            build_kfp_dag(
                                self.graph[step],
                                visited[node.name].outputs["task_out_dict"],
                            )

                        visited[step].after(visited[node.name])

            build_kfp_dag(self.graph["start"], context="")
        
        return kfp_pipeline_from_flow


def step_op_func(
    datastore_root: str, 
    cmd_template: str, 
    kfp_run_id: str, 
    context,
    flow_name: str,
    step_name: str,
    task_id: str, 
    index=None
) -> NamedTuple("context", [("task_out_dict", dict), ("split_indexes", list)]):
    """
    Function used to create a KFP container op that corresponds to a single step in the flow.
    """
    import os
    import json
    import tempfile
    from io import StringIO
    from subprocess import Popen, PIPE, STDOUT
    from typing import NamedTuple

    print("----")
    print("context")
    print(context)
    print("----")
    context_dict = json.loads("{}" if context == "" else context)

    cmd = cmd_template.format(
        run_id=kfp_run_id,
        parent_task_id=context_dict.get("task_id", ""),
        datastore_root=datastore_root,
    )

    print("RUNNING COMMAND: ", cmd)
    print("----")
    print(cmd.replace(" && ", "\n"))
    print("----")

    # TODO: Map username to KFP specific user/profile/namespace
    # Note: we don't put this in a try catch block as below because
    # Popen will not produce a error that will cause the Kubeflow step
    # to stop
    with Popen(
        ["/bin/sh", "-c", cmd],
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        universal_newlines=True,
        env=dict(os.environ, USERNAME="kfp-user", METAFLOW_RUN_ID=kfp_run_id),
    ) as process, StringIO() as stdout_buffer, StringIO() as stderr_buffer:
        for line in process.stdout:
            print(line, end="")
            stdout_buffer.write(line)
        stdout_output = stdout_buffer.getvalue()
        for line in process.stderr:
            print(line, end="")
            stderr_buffer.write(line)
        stderr_output = stderr_buffer.getvalue()

    # We put a try-catch block here because if the process above fails,
    # this line will cause an error which will stop the Kubeflow step.
    # We want to catch this error so the error logs can be captured
    # and persisted to s3.
    # TODO: this behavior is implicit. Any suggestion to make it explicit?
    try:
        with open(
            os.path.join(tempfile.gettempdir(), "kfp_metaflow_out_dict.json"), "r"
        ) as file:
            task_out_dict = json.load(file)
        print("___DONE___")
    except:
        print("Error. Persisting error logs to S3.")

    # Create two logs files, write the logs (strings) into them, and close
    stdout_file, stderr_file = open("0.stdout.log", "w"), open("0.stderr.log", "w")
    _, _ = stdout_file.write(stdout_output), stderr_file.write(stderr_output)
    stdout_file.close()
    stderr_file.close()

    # TODO: obtain the string `s3://kfp-example-aip-dev/metaflow/` dynamically
    save_logs_cmd_template = (
        f"python -m awscli s3 cp {{log_file}} {datastore_root}/"
        f"{flow_name}/{kfp_run_id}/{step_name}/"
        f"{task_id}/{{log_file}}"
    )

    log_stdout_cmd = save_logs_cmd_template.format(log_file="0.stdout.log")
    log_stderr_cmd = save_logs_cmd_template.format(log_file="0.stderr.log")
    save_logs_cmd = f"{log_stderr_cmd} >/dev/null && {log_stdout_cmd} >/dev/null"

    with Popen(
        ["/bin/sh", "-c", save_logs_cmd],
        stdin=PIPE,
        stdout=PIPE,
        stderr=STDOUT,
        universal_newlines=True,
    ) as _:
        print(
            "Finished saving logs to S3."
        )  # we persist logs even if everything went fine

    if process.returncode != 0:
        raise Exception("Returned: %s" % process.returncode)

    StepMetaflowContext = NamedTuple(
        "context", [("task_out_dict", dict), ("split_indexes", list)]
    )
    return StepMetaflowContext(task_out_dict, task_out_dict.get("split_indexes", None))
