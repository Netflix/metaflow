import base64
from collections import defaultdict
from datetime import datetime, timedelta
from metaflow.util import get_username

from metaflow import R
import rich
import sys
from metaflow.util import compress_list, dict_to_cli_options, to_pascalcase
import os
from metaflow.mflog import capture_output_to_mflog
import random
import string
import json
from metaflow.decorators import flow_decorators
from metaflow.graph import FlowGraph, DAGNode
from metaflow.plugins.cards.card_modules import chevron
from metaflow.plugins.aws.aws_utils import compute_resource_attributes
from .exceptions import AirflowNotPresent, AirflowException
from .airflow_utils import Workflow, AirflowTask, AirflowDAGArgs
from . import airflow_utils as af_utils

# TODO : remove rich at the end.
# Question : Does scheduling interval be a top level argument
# Question : Does The schedule decorator have to be enforced.
AIRFLOW_DEPLOY_TEMPLATE_FILE = os.path.join(os.path.dirname(__file__), "af_deploy.py")


AIRFLOW_PREFIX = "arf"


class Airflow(object):
    def __init__(
        self,
        name,
        graph,
        flow,
        code_package_sha,
        code_package_url,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags=None,
        namespace=None,
        username=None,
        max_workers=None,
        is_project=False,
        email=None,
        start_date=datetime.now(),
        description=None,
        catchup=False,
        file_path=None,
    ):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package_sha = code_package_sha
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.namespace = namespace
        self.username = username
        self.max_workers = max_workers
        self.email = email
        self.description = description
        self.start_date = start_date
        self.catchup = catchup
        # todo : fill information here.
        self.workflow_timeout = 10
        self.schedule_interval = "*/2 * * * *"
        self._file_path = file_path

    def _get_retries(self, node):
        max_user_code_retries = 0
        max_error_retries = 0
        # Different decorators may have different retrying strategies, so take
        # the max of them.
        for deco in node.decorators:
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        return max_user_code_retries, max_user_code_retries + max_error_retries

    def _to_job(self, node):
        attrs = {
            "metaflow.owner": self.username,
            "metaflow.flow_name": self.flow.name,
            "metaflow.step_name": node.name,
            "metaflow.run_id": "",  # todod
            "metaflow.version": self.environment.get_environment_info()[
                "metaflow_version"
            ],
            "step_name": node.name,
        }

        # Add env vars from the optional @environment decorator.
        env_deco = [deco for deco in node.decorators if deco.name == "environment"]
        env = {}
        if env_deco:
            env = env_deco[0].attributes["vars"]

        if node.name == "start":
            # Initialize parameters for the flow in the `start` step.
            # todo : Handle parameters
            # `start` step has no upstream input dependencies aside from
            # parameters.
            input_paths = None
        else:
            # We need to rely on the `InputPath` of the AWS Step Functions
            # specification to grab task ids and the step names of the parent
            # to properly construct input_paths at runtime. Thanks to the
            # JsonPath-foo embedded in the parent states, we have this
            # information easily available.

            if node.parallel_foreach:
                raise AirflowException(
                    "Parallel steps are not supported yet with AWS step functions."
                )

            # Handle foreach join.
            if (
                node.type == "join"
                and self.graph[node.split_parents[-1]].type == "foreach"
            ):
                # todo : Handle split values + input_paths
                pass
            else:
                # Set appropriate environment variables for runtime replacement.
                if len(node.in_funcs) == 1:
                    # todo : set input paths for a foreach join step
                    pass
                else:
                    # todo : set input paths for a split join step
                    pass
            env["METAFLOW_INPUT_PATHS"] = input_paths

            if node.is_inside_foreach:
                if any(self.graph[n].type == "foreach" for n in node.in_funcs):
                    # if node is inside foreach and it itself is a foreach
                    # todo : set split_parent_task_id_ for all parents
                    pass
                elif node.type == "join":
                    # if node is inside foreach and it itself is a join
                    if self.graph[node.split_parents[-1]].type == "foreach":
                        # todo : Handle passing task-ids for when immediate parent is a foreach
                        pass
                    else:
                        # todo : Handle passing task-ids for when any of the parent is a foreach
                        pass
                else:
                    # If the node is inside foreach and it's not a foreach or join type
                    # todo : Handle passing task-ids for when immediate parent is a foreach
                    pass

                # Set `METAFLOW_SPLIT_PARENT_TASK_ID_FOR_FOREACH_JOIN` if the
                # next transition is to a foreach join
                if any(
                    self.graph[n].type == "join"
                    and self.graph[self.graph[n].split_parents[-1]].type == "foreach"
                    for n in node.out_funcs
                ):
                    # todo : set METAFLOW_SPLIT_PARENT_TASK_ID_FOR_FOREACH_JOIN
                    env["METAFLOW_SPLIT_PARENT_TASK_ID_FOR_FOREACH_JOIN"] = None

                # todo : Check if timeout needs to be set explicitly

            # todo : Handle split index for for-each.
            if any(self.graph[n].type == "foreach" for n in node.in_funcs):
                pass

        env["METAFLOW_CODE_URL"] = self.code_package_url
        env["METAFLOW_FLOW_NAME"] = attrs["metaflow.flow_name"]
        env["METAFLOW_STEP_NAME"] = attrs["metaflow.step_name"]
        # todo : Find way to set runid
        env["METAFLOW_RUN_ID"] = None
        env["METAFLOW_OWNER"] = attrs["metaflow.owner"]
        metadata_env = self.metadata.get_runtime_environment("airflow")
        env.update(metadata_env)

        metaflow_version = self.environment.get_environment_info()
        metaflow_version["flow_name"] = self.graph.name
        env["METAFLOW_VERSION"] = json.dumps(metaflow_version)

        if (
            node.type == "foreach"
            or (
                node.is_inside_foreach
                and any(
                    self.graph[n].type == "join"
                    and self.graph[self.graph[n].split_parents[-1]].type == "foreach"
                    for n in node.out_funcs
                )
            )
            or (
                node.type == "join"
                and self.graph[node.split_parents[-1]].type == "foreach"
            )
        ):

            # Todo : Find ways to pass state using for the below usecases:
            #   1. To set the cardinality of foreaches (which are subsequently)
            #      read prior to the instantiation of the Map state by AWS Step
            #      Functions.
            #   2. To set the input paths from the parent steps of a foreach join.
            #   3. To read the input paths in a foreach join.
            pass

        # Todo : Find and set resource requirements for the decorator.
        user_code_retries, total_retries = self._get_retries(node)
        # Todo : Convert to batch / Kubernetes JOB
        # Todo : - Set it's cli
        # Todo : - env vars etc.
        pass

    def _step_cli(self, node, paths, code_package_url, user_code_retries):
        cmds = []

        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)

        if R.use_r():
            entrypoint = [R.entrypoint()]
        else:
            entrypoint = [executable, script_name]

        task_id = ""
        top_opts_dict = {
            "with": [
                decorator.make_decorator_spec()
                for decorator in node.decorators
                if not decorator.statically_defined
            ]
        }
        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook. See similar logic in runtime.py.
        for deco in flow_decorators():
            top_opts_dict.update(deco.get_top_level_options())

        top_opts = list(dict_to_cli_options(top_opts_dict))

        top_level = top_opts + [
            "--quiet",
            "--metadata=%s" % self.metadata.TYPE,
            "--environment=%s" % self.environment.TYPE,
            "--datastore=%s" % self.flow_datastore.TYPE,
            "--datastore-root=%s" % self.flow_datastore.datastore_root,
            "--event-logger=%s" % self.event_logger.logger_type,
            "--monitor=%s" % self.monitor.monitor_type,
            "--no-pylint",
            "--with=step_functions_internal",
        ]

        if node.name == "start":
            # We need a separate unique ID for the special _parameters task
            task_id_params = "%s-params" % task_id
            # TODO : Currently I am putting this boiler plate because we need to check if parameters are set or not.
            # Export user-defined parameters into runtime environment
            param_file = "".join(
                random.choice(string.ascii_lowercase) for _ in range(10)
            )
            # Setup Parameters as environment variables which are stored in a dictionary.
            export_params = " && ".join(
                [
                    capture_output_to_mflog(
                        "python -m metaflow.plugins.airflow.set_parameters  %s"
                        % param_file
                    ),
                    ". `pwd`/%s" % param_file,
                ]
            )
            params = (
                entrypoint
                + top_level
                + [
                    "init",
                    # todo : "--run-id sfn-$METAFLOW_RUN_ID",
                    # todo : "--task-id %s" % task_id_params,
                ]
            )
            # Assign tags to run objects.
            if self.tags:
                params.extend("--tag %s" % tag for tag in self.tags)

            # If the start step gets retried, we must be careful not to
            # regenerate multiple parameters tasks. Hence we check first if
            # _parameters exists already.
            exists = entrypoint + [
                # Dump the parameters task
                "dump",
                "--max-value-size=0",
                # todo : set task_id for parameters
                # "sfn-${METAFLOW_RUN_ID}/_parameters/%s" % (task_id_params),
            ]
            cmd = "if ! %s >/dev/null 2>/dev/null; then %s && %s; fi" % (
                " ".join(exists),
                export_params,
                capture_output_to_mflog(" ".join(params)),
            )
            cmds.append(cmd)
            # todo : set input paths for parameters = "sfn-${METAFLOW_RUN_ID}/_parameters/%s" % (task_id_params)

        if node.type == "join" and self.graph[node.split_parents[-1]].type == "foreach":
            # todo : handle join case
            pass
            # parent_tasks_file = "".join(
            #     random.choice(string.ascii_lowercase) for _ in range(10)
            # )
            # export_parent_tasks = capture_output_to_mflog(
            #     "python -m "
            #     "metaflow.plugins.aws.step_functions.set_batch_environment "
            #     "parent_tasks %s && . `pwd`/%s" % (parent_tasks_file, parent_tasks_file)
            # )
            # cmds.append(export_parent_tasks)

        step = [
            "step",
            node.name,
            # todo  "--run-id sfn-$METAFLOW_RUN_ID",
            # todo "--task-id %s" % task_id,
            # todo "--retry-count $((AWS_BATCH_JOB_ATTEMPT-1))",
            # todo "--max-user-code-retries %d" % user_code_retries,
            # todo "--input-paths %s" % paths,
        ]
        if any(self.graph[n].type == "foreach" for n in node.in_funcs):
            # # todo step.append("--split-index $METAFLOW_SPLIT_INDEX")
            pass
        if self.tags:
            step.extend("--tag %s" % tag for tag in self.tags)
        if self.namespace is not None:
            step.append("--namespace=%s" % self.namespace)
        cmds.append(capture_output_to_mflog(" ".join(entrypoint + top_level + step)))
        return " && ".join(cmds)

    def compile(self):

        # Visit every node of the flow and recursively build the state machine.
        def _visit(node: DAGNode, workflow: Workflow, exit_node=None):
            if node.parallel_foreach:
                raise AirflowException(
                    "Deploying flows with @parallel decorator(s) "
                    "to Airflow is not supported currently."
                )

            state = AirflowTask(node.name)

            if node.type == "end" or exit_node in node.out_funcs:
                workflow.add_state(state)

            # Continue linear assignment within the (sub)workflow if the node
            # doesn't branch or fork.
            elif node.type in ("start", "linear", "join"):
                workflow.add_state(state.next(node.out_funcs[0]))
                _visit(self.graph[node.out_funcs[0]], workflow, exit_node)

            elif node.type == "split":
                # Todo : handle Taskgroup in this step cardinality in some way
                pass

            elif node.type == "foreach":
                # Todo : handle foreach cardinality in some way
                # Continue the traversal from the matching_join.
                _visit(self.graph[node.matching_join], workflow, exit_node)
            # We shouldn't ideally ever get here.
            else:
                raise AirflowException(
                    "Node type *%s* for  step *%s* "
                    "is not currently supported by "
                    "Airflow." % (node.type, node.name)
                )
            return workflow

        workflow = Workflow(
            dag_id=self.name,
            default_args=self._create_defaults(),
            description=self.description,
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
            catchup=self.catchup,
            tags=self.tags,
            file_path=self._file_path,
        )
        json_dag = _visit(self.graph["start"], workflow).to_json()
        return self._create_airflow_file(json_dag)

    def _create_airflow_file(self, json_dag):
        util_file = None
        with open(af_utils.__file__) as f:
            util_file = f.read()
        with open(AIRFLOW_DEPLOY_TEMPLATE_FILE) as f:
            return chevron.render(
                f.read(),
                dict(
                    # Converting the configuration to base64 so that there can be no indentation related issues that can be caused because of
                    # malformed strings / json.
                    metaflow_workflow_compile_params=base64.b64encode(
                        json_dag.encode("utf-8")
                    ).decode("utf-8"),
                    AIRFLOW_UTILS=util_file,
                ),
            )

    def _create_defaults(self):
        return {
            "owner": get_username(),
            "depends_on_past": False,
            "email": [] if self.email is None else [self.email],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            # 'queue': 'bash_queue',
            # 'pool': 'backfill',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),
            # 'wait_for_downstream': False,
            # 'dag': dag,
            # 'sla': timedelta(hours=2),
            # 'execution_timeout': timedelta(seconds=300),
            # 'on_failure_callback': some_function,
            # 'on_success_callback': some_other_function,
            # 'on_retry_callback': another_function,
            # 'sla_miss_callback': yet_another_function,
            # 'trigger_rule': 'all_success'
        }
