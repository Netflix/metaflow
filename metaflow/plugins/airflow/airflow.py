import json
import os
import random
import string
import sys
from datetime import datetime, timedelta
from io import BytesIO

import metaflow.util as util
from metaflow import current
from metaflow.decorators import flow_decorators
from metaflow.exception import MetaflowException
from metaflow.includefile import FilePathClass
from metaflow.metaflow_config import (
    AIRFLOW_KUBERNETES_CONN_ID,
    AIRFLOW_KUBERNETES_KUBECONFIG_CONTEXT,
    AIRFLOW_KUBERNETES_KUBECONFIG_FILE,
    AIRFLOW_KUBERNETES_STARTUP_TIMEOUT_SECONDS,
    AWS_SECRETS_MANAGER_DEFAULT_REGION,
    GCP_SECRET_MANAGER_PREFIX,
    AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
    CARD_AZUREROOT,
    CARD_GSROOT,
    CARD_S3ROOT,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    DEFAULT_SECRETS_BACKEND_TYPE,
    KUBERNETES_SECRETS,
    KUBERNETES_SERVICE_ACCOUNT,
    S3_ENDPOINT_URL,
    SERVICE_HEADERS,
    SERVICE_INTERNAL_URL,
    AZURE_KEY_VAULT_PREFIX,
)

from metaflow.metaflow_config_funcs import config_values

from metaflow.parameters import (
    DelayedEvaluationParameter,
    JSONTypeClass,
    deploy_time_eval,
)

# TODO: Move chevron to _vendor
from metaflow.plugins.cards.card_modules import chevron
from metaflow.plugins.kubernetes.kubernetes import Kubernetes
from metaflow.plugins.kubernetes.kube_utils import qos_requests_and_limits
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.util import compress_list, dict_to_cli_options, get_username

from . import airflow_utils
from .airflow_utils import AIRFLOW_MACROS, TASK_ID_XCOM_KEY, AirflowTask, Workflow
from .exception import AirflowException
from .sensors import SUPPORTED_SENSORS

AIRFLOW_DEPLOY_TEMPLATE_FILE = os.path.join(os.path.dirname(__file__), "dag.py")


class Airflow(object):
    TOKEN_STORAGE_ROOT = "mf.airflow"

    def __init__(
        self,
        name,
        graph,
        flow,
        code_package_metadata,
        code_package_sha,
        code_package_url,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        production_token,
        tags=None,
        namespace=None,
        username=None,
        max_workers=None,
        worker_pool=None,
        description=None,
        file_path=None,
        workflow_timeout=None,
        is_paused_upon_creation=True,
    ):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package_metadata = code_package_metadata
        self.code_package_sha = code_package_sha
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.namespace = namespace  # this is the username space
        self.username = username
        self.max_workers = max_workers
        self.description = description
        self._depends_on_upstream_sensors = False
        self._file_path = file_path
        _, self.graph_structure = self.graph.output_steps()
        self.worker_pool = worker_pool
        self.is_paused_upon_creation = is_paused_upon_creation
        self.workflow_timeout = workflow_timeout
        self.schedule = self._get_schedule()
        self.parameters = self._process_parameters()
        self.production_token = production_token
        self.contains_foreach = self._contains_foreach()

    @classmethod
    def get_existing_deployment(cls, name, flow_datastore):
        _backend = flow_datastore._storage_impl
        token_exists = _backend.is_file([cls.get_token_path(name)])
        if not token_exists[0]:
            return None
        with _backend.load_bytes([cls.get_token_path(name)]) as get_results:
            for _, path, _ in get_results:
                if path is not None:
                    with open(path, "r") as f:
                        data = json.loads(f.read())
                    return (data["owner"], data["production_token"])

    @classmethod
    def get_token_path(cls, name):
        return os.path.join(cls.TOKEN_STORAGE_ROOT, name)

    @classmethod
    def save_deployment_token(cls, owner, name, token, flow_datastore):
        _backend = flow_datastore._storage_impl
        _backend.save_bytes(
            [
                (
                    cls.get_token_path(name),
                    BytesIO(
                        bytes(
                            json.dumps({"production_token": token, "owner": owner}),
                            "utf-8",
                        )
                    ),
                )
            ],
            overwrite=False,
        )

    def _get_schedule(self):
        # Using the cron presets provided here :
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html?highlight=schedule%20interval#cron-presets
        schedule = self.flow._flow_decorators.get("schedule")
        if not schedule:
            return None
        schedule = schedule[0]
        if schedule.attributes["cron"]:
            return schedule.attributes["cron"]
        elif schedule.attributes["weekly"]:
            return "@weekly"
        elif schedule.attributes["hourly"]:
            return "@hourly"
        elif schedule.attributes["daily"]:
            return "@daily"
        return None

    def _get_retries(self, node):
        max_user_code_retries = 0
        max_error_retries = 0
        foreach_default_retry = 1
        # Different decorators may have different retrying strategies, so take
        # the max of them.
        for deco in node.decorators:
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)
        parent_is_foreach = any(  # The immediate parent is a foreach node.
            self.graph[n].type == "foreach" for n in node.in_funcs
        )

        if parent_is_foreach:
            max_user_code_retries + foreach_default_retry
        return max_user_code_retries, max_user_code_retries + max_error_retries

    def _get_retry_delay(self, node):
        retry_decos = [deco for deco in node.decorators if deco.name == "retry"]
        if len(retry_decos) > 0:
            retry_mins = retry_decos[0].attributes["minutes_between_retries"]
            return timedelta(minutes=int(retry_mins))
        return None

    def _process_parameters(self):
        airflow_params = []
        type_transform_dict = {
            int.__name__: "integer",
            str.__name__: "string",
            bool.__name__: "string",
            float.__name__: "number",
        }

        for var, param in self.flow._get_parameters():
            # Airflow requires defaults set for parameters.
            value = deploy_time_eval(param.kwargs.get("default"))
            # Setting airflow related param args.
            airflow_param = dict(
                name=param.name,
            )
            if value is not None:
                airflow_param["default"] = value
            if param.kwargs.get("help"):
                airflow_param["description"] = param.kwargs.get("help")

            # Since we will always have a default value and `deploy_time_eval` resolved that to an actual value
            # we can just use the `default` to infer the object's type.
            # This avoids parsing/identifying types like `JSONType` or `FilePathClass`
            # which are returned by calling `param.kwargs.get("type")`
            param_type = type(airflow_param["default"])

            # extract the name of the type and resolve the type-name
            # compatible with Airflow.
            param_type_name = getattr(param_type, "__name__", None)
            if param_type_name in type_transform_dict:
                airflow_param["type"] = type_transform_dict[param_type_name]

            if param_type_name == bool.__name__:
                airflow_param["default"] = str(airflow_param["default"])

            airflow_params.append(airflow_param)

        return airflow_params

    def _compress_input_path(
        self,
        steps,
    ):
        """
        This function is meant to compress the input paths, and it specifically doesn't use
        `metaflow.util.compress_list` under the hood. The reason is that the `AIRFLOW_MACROS.RUN_ID` is a complicated
        macro string that doesn't behave nicely with `metaflow.util.decompress_list`, since the `decompress_util`
        function expects a string which doesn't contain any delimiter characters and the run-id string does. Hence, we
        have a custom compression string created via `_compress_input_path` function instead of `compress_list`.
        """
        return "%s:" % (AIRFLOW_MACROS.RUN_ID) + ",".join(
            self._make_input_path(step, only_task_id=True) for step in steps
        )

    def _make_foreach_input_path(self, step_name):
        return (
            "%s/%s/:{{ task_instance.xcom_pull(task_ids='%s',key='%s') | join_list }}"
            % (
                AIRFLOW_MACROS.RUN_ID,
                step_name,
                step_name,
                TASK_ID_XCOM_KEY,
            )
        )

    def _make_input_path(self, step_name, only_task_id=False):
        """
        This is set using the `airflow_internal` decorator to help pass state.
        This will pull the `TASK_ID_XCOM_KEY` xcom which holds task-ids.
        The key is set via the `MetaflowKubernetesOperator`.
        """
        task_id_string = "/%s/{{ task_instance.xcom_pull(task_ids='%s',key='%s') }}" % (
            step_name,
            step_name,
            TASK_ID_XCOM_KEY,
        )

        if only_task_id:
            return task_id_string

        return "%s%s" % (AIRFLOW_MACROS.RUN_ID, task_id_string)

    def _to_job(self, node):
        """
        This function will transform the node's specification into Airflow compatible operator arguments.
        Since this function is long, below is the summary of the two major duties it performs:
            1. Based on the type of the graph node (start/linear/foreach/join etc.)
                it will decide how to set the input paths
            2. Based on node's decorator specification convert the information into
                a job spec for the KubernetesPodOperator.
        """
        # Add env vars from the optional @environment decorator.
        env_deco = [deco for deco in node.decorators if deco.name == "environment"]
        env = {}
        if env_deco:
            env = env_deco[0].attributes["vars"].copy()

        # The below if/else block handles "input paths".
        # Input Paths help manage dataflow across the graph.
        if node.name == "start":
            # POSSIBLE_FUTURE_IMPROVEMENT:
            # We can extract metadata about the possible upstream sensor triggers.
            # There is a previous commit (7bdf6) in the `airflow` branch that has `SensorMetaExtractor` class and
            # associated MACRO we have built to handle this case if a metadata regarding the sensor is needed.
            # Initialize parameters for the flow in the `start` step.
            # `start` step has no upstream input dependencies aside from
            # parameters.

            if len(self.parameters):
                env["METAFLOW_PARAMETERS"] = AIRFLOW_MACROS.PARAMETERS
            input_paths = None
        else:
            # If it is not the start node then we check if there are many paths
            # converging into it or a single path. Based on that we set the INPUT_PATHS
            if node.parallel_foreach:
                raise AirflowException(
                    "Parallel steps are not supported yet with Airflow."
                )
            is_foreach_join = (
                node.type == "join"
                and self.graph[node.split_parents[-1]].type == "foreach"
            )
            if is_foreach_join:
                input_paths = self._make_foreach_input_path(node.in_funcs[0])

            elif len(node.in_funcs) == 1:
                # set input paths where this is only one parent node
                # The parent-task-id is passed via the xcom; There is no other way to get that.
                # One key thing about xcoms is that they are immutable and only accepted if the task
                # doesn't fail.
                # From airflow docs :
                # "Note: If the first task run is not succeeded then on every retry task
                # XComs will be cleared to make the task run idempotent."
                input_paths = self._make_input_path(node.in_funcs[0])
            else:
                # this is a split scenario where there can be more than one input paths.
                input_paths = self._compress_input_path(node.in_funcs)

            # env["METAFLOW_INPUT_PATHS"] = input_paths

        env["METAFLOW_CODE_URL"] = self.code_package_url
        env["METAFLOW_FLOW_NAME"] = self.flow.name
        env["METAFLOW_STEP_NAME"] = node.name
        env["METAFLOW_OWNER"] = self.username

        metadata_env = self.metadata.get_runtime_environment("airflow")
        env.update(metadata_env)

        metaflow_version = self.environment.get_environment_info()
        metaflow_version["flow_name"] = self.graph.name
        metaflow_version["production_token"] = self.production_token
        env["METAFLOW_VERSION"] = json.dumps(metaflow_version)

        # Temporary passing of *some* environment variables. Do not rely on this
        # mechanism as it will be removed in the near future
        env.update(
            {
                k: v
                for k, v in config_values()
                if k.startswith("METAFLOW_CONDA_") or k.startswith("METAFLOW_DEBUG_")
            }
        )

        # Extract the k8s decorators for constructing the arguments of the K8s Pod Operator on Airflow.
        k8s_deco = [deco for deco in node.decorators if deco.name == "kubernetes"][0]
        user_code_retries, _ = self._get_retries(node)
        retry_delay = self._get_retry_delay(node)
        # This sets timeouts for @timeout decorators.
        # The timeout is set as "execution_timeout" for an airflow task.
        runtime_limit = get_run_time_limit_for_task(node.decorators)

        k8s = Kubernetes(self.flow_datastore, self.metadata, self.environment)
        user = util.get_username()

        labels = {
            "app": "metaflow",
            "app.kubernetes.io/name": "metaflow-task",
            "app.kubernetes.io/part-of": "metaflow",
            "app.kubernetes.io/created-by": user,
            # Question to (savin) : Should we have username set over here for created by since it is the
            # airflow installation that is creating the jobs.
            # Technically the "user" is the stakeholder but should these labels be present.
        }
        additional_mf_variables = {
            "METAFLOW_CODE_METADATA": self.code_package_metadata,
            "METAFLOW_CODE_SHA": self.code_package_sha,
            "METAFLOW_CODE_URL": self.code_package_url,
            "METAFLOW_CODE_DS": self.flow_datastore.TYPE,
            "METAFLOW_USER": user,
            "METAFLOW_SERVICE_URL": SERVICE_INTERNAL_URL,
            "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
            "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
            "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
            "METAFLOW_DEFAULT_DATASTORE": self.flow_datastore.TYPE,
            "METAFLOW_DEFAULT_METADATA": "service",
            "METAFLOW_KUBERNETES_WORKLOAD": str(
                1
            ),  # This is used by kubernetes decorator.
            "METAFLOW_RUNTIME_ENVIRONMENT": "kubernetes",
            "METAFLOW_CARD_S3ROOT": CARD_S3ROOT,
            "METAFLOW_RUN_ID": AIRFLOW_MACROS.RUN_ID,
            "METAFLOW_AIRFLOW_TASK_ID": AIRFLOW_MACROS.create_task_id(
                self.contains_foreach
            ),
            "METAFLOW_AIRFLOW_DAG_RUN_ID": AIRFLOW_MACROS.AIRFLOW_RUN_ID,
            "METAFLOW_AIRFLOW_JOB_ID": AIRFLOW_MACROS.AIRFLOW_JOB_ID,
            "METAFLOW_PRODUCTION_TOKEN": self.production_token,
            "METAFLOW_ATTEMPT_NUMBER": AIRFLOW_MACROS.ATTEMPT,
            # GCP stuff
            "METAFLOW_DATASTORE_SYSROOT_GS": DATASTORE_SYSROOT_GS,
            "METAFLOW_CARD_GSROOT": CARD_GSROOT,
            "METAFLOW_S3_ENDPOINT_URL": S3_ENDPOINT_URL,
        }
        env["METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT"] = (
            AZURE_STORAGE_BLOB_SERVICE_ENDPOINT
        )
        env["METAFLOW_DATASTORE_SYSROOT_AZURE"] = DATASTORE_SYSROOT_AZURE
        env["METAFLOW_CARD_AZUREROOT"] = CARD_AZUREROOT
        if DEFAULT_SECRETS_BACKEND_TYPE:
            env["METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE"] = DEFAULT_SECRETS_BACKEND_TYPE
        if AWS_SECRETS_MANAGER_DEFAULT_REGION:
            env["METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION"] = (
                AWS_SECRETS_MANAGER_DEFAULT_REGION
            )
        if GCP_SECRET_MANAGER_PREFIX:
            env["METAFLOW_GCP_SECRET_MANAGER_PREFIX"] = GCP_SECRET_MANAGER_PREFIX

        if AZURE_KEY_VAULT_PREFIX:
            env["METAFLOW_AZURE_KEY_VAULT_PREFIX"] = AZURE_KEY_VAULT_PREFIX

        env.update(additional_mf_variables)

        service_account = (
            KUBERNETES_SERVICE_ACCOUNT
            if k8s_deco.attributes["service_account"] is None
            else k8s_deco.attributes["service_account"]
        )
        k8s_namespace = (
            k8s_deco.attributes["namespace"]
            if k8s_deco.attributes["namespace"] is not None
            else "default"
        )
        qos_requests, qos_limits = qos_requests_and_limits(
            k8s_deco.attributes["qos"],
            k8s_deco.attributes["cpu"],
            k8s_deco.attributes["memory"],
            k8s_deco.attributes["disk"],
        )
        resources = dict(
            requests=qos_requests,
            limits={
                **qos_limits,
                **{
                    "%s.com/gpu".lower()
                    % k8s_deco.attributes["gpu_vendor"]: str(k8s_deco.attributes["gpu"])
                    for k in [0]
                    # Don't set GPU limits if gpu isn't specified.
                    if k8s_deco.attributes["gpu"] is not None
                },
            },
        )

        annotations = {
            "metaflow/production_token": self.production_token,
            "metaflow/owner": self.username,
            "metaflow/user": self.username,
            "metaflow/flow_name": self.flow.name,
        }
        if current.get("project_name"):
            annotations.update(
                {
                    "metaflow/project_name": current.project_name,
                    "metaflow/branch_name": current.branch_name,
                    "metaflow/project_flow_name": current.project_flow_name,
                }
            )

        k8s_operator_args = dict(
            # like argo workflows we use step_name as name of container
            name=node.name,
            namespace=k8s_namespace,
            service_account_name=service_account,
            node_selector=k8s_deco.attributes["node_selector"],
            cmds=k8s._command(
                self.flow.name,
                AIRFLOW_MACROS.RUN_ID,
                node.name,
                AIRFLOW_MACROS.create_task_id(self.contains_foreach),
                AIRFLOW_MACROS.ATTEMPT,
                code_package_metadata=self.code_package_metadata,
                code_package_url=self.code_package_url,
                step_cmds=self._step_cli(
                    node, input_paths, self.code_package_url, user_code_retries
                ),
            ),
            annotations=annotations,
            image=k8s_deco.attributes["image"],
            resources=resources,
            execution_timeout=dict(seconds=runtime_limit),
            retries=user_code_retries,
            env_vars=[dict(name=k, value=v) for k, v in env.items() if v is not None],
            labels=labels,
            task_id=node.name,
            startup_timeout_seconds=AIRFLOW_KUBERNETES_STARTUP_TIMEOUT_SECONDS,
            get_logs=True,
            do_xcom_push=True,
            log_events_on_failure=True,
            is_delete_operator_pod=True,
            retry_exponential_backoff=False,  # todo : should this be a arg we allow on CLI. not right now - there is an open ticket for this - maybe at some point we will.
            reattach_on_restart=False,
            secrets=[],
        )
        k8s_operator_args["in_cluster"] = True
        if AIRFLOW_KUBERNETES_CONN_ID is not None:
            k8s_operator_args["kubernetes_conn_id"] = AIRFLOW_KUBERNETES_CONN_ID
            k8s_operator_args["in_cluster"] = False
        if AIRFLOW_KUBERNETES_KUBECONFIG_CONTEXT is not None:
            k8s_operator_args["cluster_context"] = AIRFLOW_KUBERNETES_KUBECONFIG_CONTEXT
            k8s_operator_args["in_cluster"] = False
        if AIRFLOW_KUBERNETES_KUBECONFIG_FILE is not None:
            k8s_operator_args["config_file"] = AIRFLOW_KUBERNETES_KUBECONFIG_FILE
            k8s_operator_args["in_cluster"] = False

        if k8s_deco.attributes["secrets"]:
            if isinstance(k8s_deco.attributes["secrets"], str):
                k8s_operator_args["secrets"] = k8s_deco.attributes["secrets"].split(",")
            elif isinstance(k8s_deco.attributes["secrets"], list):
                k8s_operator_args["secrets"] = k8s_deco.attributes["secrets"]
        if len(KUBERNETES_SECRETS) > 0:
            k8s_operator_args["secrets"] += KUBERNETES_SECRETS.split(",")

        if retry_delay:
            k8s_operator_args["retry_delay"] = dict(seconds=retry_delay.total_seconds())

        return k8s_operator_args

    def _step_cli(self, node, paths, code_package_url, user_code_retries):
        cmds = []

        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)

        entrypoint = [executable, script_name]

        top_opts_dict = {
            "with": [
                decorator.make_decorator_spec()
                for decorator in node.decorators
                if not decorator.statically_defined and decorator.inserted_by is None
            ]
        }
        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook. See similar logic in runtime.py.
        for deco in flow_decorators(self.flow):
            top_opts_dict.update(deco.get_top_level_options())

        top_opts = list(dict_to_cli_options(top_opts_dict))

        top_level = top_opts + [
            "--quiet",
            "--metadata=%s" % self.metadata.TYPE,
            "--environment=%s" % self.environment.TYPE,
            "--datastore=%s" % self.flow_datastore.TYPE,
            "--datastore-root=%s" % self.flow_datastore.datastore_root,
            "--event-logger=%s" % self.event_logger.TYPE,
            "--monitor=%s" % self.monitor.TYPE,
            "--no-pylint",
            "--with=airflow_internal",
        ]

        if node.name == "start":
            # We need a separate unique ID for the special _parameters task
            task_id_params = "%s-params" % AIRFLOW_MACROS.create_task_id(
                self.contains_foreach
            )
            # Export user-defined parameters into runtime environment
            param_file = "".join(
                random.choice(string.ascii_lowercase) for _ in range(10)
            )
            # Setup Parameters as environment variables which are stored in a dictionary.
            export_params = (
                "python -m "
                "metaflow.plugins.airflow.plumbing.set_parameters %s "
                "&& . `pwd`/%s" % (param_file, param_file)
            )
            # Setting parameters over here.
            params = (
                entrypoint
                + top_level
                + [
                    "init",
                    "--run-id %s" % AIRFLOW_MACROS.RUN_ID,
                    "--task-id %s" % task_id_params,
                ]
            )

            # Assign tags to run objects.
            if self.tags:
                params.extend("--tag %s" % tag for tag in self.tags)

            # If the start step gets retried, we must be careful not to
            # regenerate multiple parameters tasks. Hence, we check first if
            # _parameters exists already.
            exists = entrypoint + [
                # Dump the parameters task
                "dump",
                "--max-value-size=0",
                "%s/_parameters/%s" % (AIRFLOW_MACROS.RUN_ID, task_id_params),
            ]
            cmd = "if ! %s >/dev/null 2>/dev/null; then %s && %s; fi" % (
                " ".join(exists),
                export_params,
                " ".join(params),
            )
            cmds.append(cmd)
            # set input paths for parameters
            paths = "%s/_parameters/%s" % (AIRFLOW_MACROS.RUN_ID, task_id_params)

        step = [
            "step",
            node.name,
            "--run-id %s" % AIRFLOW_MACROS.RUN_ID,
            "--task-id %s" % AIRFLOW_MACROS.create_task_id(self.contains_foreach),
            "--retry-count %s" % AIRFLOW_MACROS.ATTEMPT,
            "--max-user-code-retries %d" % user_code_retries,
            "--input-paths %s" % paths,
        ]
        if self.tags:
            step.extend("--tag %s" % tag for tag in self.tags)
        if self.namespace is not None:
            step.append("--namespace=%s" % self.namespace)

        parent_is_foreach = any(  # The immediate parent is a foreach node.
            self.graph[n].type == "foreach" for n in node.in_funcs
        )
        if parent_is_foreach:
            step.append("--split-index %s" % AIRFLOW_MACROS.FOREACH_SPLIT_INDEX)

        cmds.append(" ".join(entrypoint + top_level + step))
        return cmds

    def _collect_flow_sensors(self):
        decos_lists = [
            self.flow._flow_decorators.get(s.name)
            for s in SUPPORTED_SENSORS
            if self.flow._flow_decorators.get(s.name) is not None
        ]
        af_tasks = [deco.create_task() for decos in decos_lists for deco in decos]
        if len(af_tasks) > 0:
            self._depends_on_upstream_sensors = True
        return af_tasks

    def _contains_foreach(self):
        for node in self.graph:
            if node.type == "foreach":
                return True
        return False

    def compile(self):
        if self.flow._flow_decorators.get("trigger") or self.flow._flow_decorators.get(
            "trigger_on_finish"
        ):
            raise AirflowException(
                "Deploying flows with @trigger or @trigger_on_finish decorator(s) "
                "to Airflow is not supported currently."
            )

        if self.flow._flow_decorators.get("exit_hook"):
            raise AirflowException(
                "Deploying flows with the @exit_hook decorator "
                "to Airflow is not currently supported."
            )

        # Visit every node of the flow and recursively build the state machine.
        def _visit(node, workflow, exit_node=None):
            kube_deco = dict(
                [deco for deco in node.decorators if deco.name == "kubernetes"][
                    0
                ].attributes
            )
            if kube_deco:
                # Only guard against use_tmpfs and tmpfs_size as these determine if tmpfs is enabled.
                for attr in [
                    "use_tmpfs",
                    "tmpfs_size",
                    "persistent_volume_claims",
                    "image_pull_policy",
                ]:
                    if kube_deco[attr]:
                        raise AirflowException(
                            "The decorator attribute *%s* is currently not supported on Airflow "
                            "for the @kubernetes decorator on step *%s*"
                            % (attr, node.name)
                        )

            parent_is_foreach = any(  # Any immediate parent is a foreach node.
                self.graph[n].type == "foreach" for n in node.in_funcs
            )
            state = AirflowTask(
                node.name, is_mapper_node=parent_is_foreach
            ).set_operator_args(**self._to_job(node))
            if node.type == "end":
                workflow.add_state(state)

            # Continue linear assignment within the (sub)workflow if the node
            # doesn't branch or fork.
            elif node.type in ("start", "linear", "join", "foreach"):
                workflow.add_state(state)
                _visit(
                    self.graph[node.out_funcs[0]],
                    workflow,
                )

            elif node.type == "split":
                workflow.add_state(state)
                for func in node.out_funcs:
                    _visit(
                        self.graph[func],
                        workflow,
                    )
            else:
                raise AirflowException(
                    "Node type *%s* for  step *%s* "
                    "is not currently supported by "
                    "Airflow." % (node.type, node.name)
                )

            return workflow

        # set max active tasks here , For more info check here :
        # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG
        airflow_dag_args = (
            {} if self.max_workers is None else dict(max_active_tasks=self.max_workers)
        )
        airflow_dag_args["is_paused_upon_creation"] = self.is_paused_upon_creation

        # workflow timeout should only be enforced if a dag is scheduled.
        if self.workflow_timeout is not None and self.schedule is not None:
            airflow_dag_args["dagrun_timeout"] = dict(seconds=self.workflow_timeout)

        appending_sensors = self._collect_flow_sensors()
        workflow = Workflow(
            dag_id=self.name,
            default_args=self._create_defaults(),
            description=self.description,
            schedule_interval=self.schedule,
            # `start_date` is a mandatory argument even though the documentation lists it as optional value
            # Based on the code, Airflow will throw a `AirflowException` when `start_date` is not provided
            # to a DAG : https://github.com/apache/airflow/blob/0527a0b6ce506434a23bc2a6f5ddb11f492fc614/airflow/models/dag.py#L2170
            start_date=datetime.now(),
            tags=self.tags,
            file_path=self._file_path,
            graph_structure=self.graph_structure,
            metadata=dict(
                contains_foreach=self.contains_foreach, flow_name=self.flow.name
            ),
            **airflow_dag_args
        )
        workflow = _visit(self.graph["start"], workflow)

        workflow.set_parameters(self.parameters)
        if len(appending_sensors) > 0:
            for s in appending_sensors:
                workflow.add_state(s)
            workflow.graph_structure.insert(0, [[s.name] for s in appending_sensors])
        return self._to_airflow_dag_file(workflow.to_dict())

    def _to_airflow_dag_file(self, json_dag):
        util_file = None
        with open(airflow_utils.__file__) as f:
            util_file = f.read()
        with open(AIRFLOW_DEPLOY_TEMPLATE_FILE) as f:
            return chevron.render(
                f.read(),
                dict(
                    # Converting the configuration to base64 so that there can be no indentation related issues that can be caused because of
                    # malformed strings / json.
                    config=json_dag,
                    utils=util_file,
                    deployed_on=str(datetime.now()),
                ),
            )

    def _create_defaults(self):
        defu_ = {
            "owner": get_username(),
            # If set on a task and the previous run of the task has failed,
            # it will not run the task in the current DAG run.
            "depends_on_past": False,
            # TODO: Enable emails
            "execution_timeout": timedelta(days=5),
            "retry_delay": timedelta(seconds=200),
            # check https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html?highlight=retry_delay#airflow.models.baseoperator.BaseOperatorMeta
        }
        if self.worker_pool is not None:
            defu_["pool"] = self.worker_pool

        return defu_
