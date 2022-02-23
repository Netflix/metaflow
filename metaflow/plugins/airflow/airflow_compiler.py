from collections import defaultdict
from datetime import datetime, timedelta
from metaflow.util import get_username


import rich
import os
from metaflow.graph import FlowGraph, DAGNode
from metaflow.plugins.cards.card_modules import chevron
from .exceptions import AirflowNotPresent, AirflowException
from .airflow_utils import Workflow, AirflowTask, AirflowDAGArgs

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
        try:
            import airflow
        except ImportError:
            raise AirflowNotPresent
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
                #
                pass

            elif node.type == "foreach":
                pass
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
        with open(AIRFLOW_DEPLOY_TEMPLATE_FILE) as f:
            return chevron.render(
                f.read(), dict(metaflow_workflow_compile_params=json_dag)
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
