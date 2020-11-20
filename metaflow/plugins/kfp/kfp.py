import inspect
import os
import sys
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Union

import yaml

import kfp
from kfp import dsl
from kfp.dsl import ContainerOp, PipelineConf
from metaflow.metaflow_config import (
    ARGO_DEFAULT_TTL,
    DATASTORE_SYSROOT_S3,
    METADATA_SERVICE_URL,
)
from metaflow.plugins.kfp.kfp_step_function import kfp_step_function

from ... import R
from ...environment import MetaflowEnvironment
from ...graph import DAGNode
from ...plugins.resources_decorator import ResourcesDecorator
from .kfp_constants import (
    INPUT_PATHS_ENV_NAME,
    SPLIT_INDEX_ENV_NAME,
    STEP_ENVIRONMENT_VARIABLES,
    TASK_ID_ENV_NAME,
)
from .kfp_foreach_splits import graph_to_task_ids


class KfpComponent(object):
    def __init__(
        self,
        name: str,
        cmd_template: str,
        total_retries: int,
        resource_requirements: Dict[str, str],
    ):
        self.name = name
        self.cmd_template = cmd_template
        self.total_retries = total_retries
        self.resource_requirements = resource_requirements


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
        max_parallelism=None,
        workflow_timeout=None,
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
        self.max_parallelism = max_parallelism
        self.workflow_timeout = (
            workflow_timeout if workflow_timeout else 0  # 0 is unlimited
        )

        self._client = kfp.Client(namespace=api_namespace, userid=username, **kwargs)

    def create_run_on_kfp(self, experiment_name: str, run_name: str):
        """
        Creates a new run on KFP using the `kfp.Client()`.
        """
        # TODO: first create KFP Pipeline, then an experiment if provided else default experiment.
        run_pipeline_result = self._client.create_run_from_pipeline_func(
            pipeline_func=self.create_kfp_pipeline_from_flow_graph(),
            arguments={"datastore_root": DATASTORE_SYSROOT_S3},
            experiment_name=experiment_name,
            run_name=run_name,
            namespace=self.namespace,
        )
        return run_pipeline_result

    def create_kfp_pipeline_yaml(self, pipeline_file_path) -> str:
        """
        Creates a new KFP pipeline YAML using `kfp.compiler.Compiler()`.
        Note: Intermediate pipeline YAML is saved at `pipeline_file_path`
        """
        pipeline_conf = PipelineConf()
        pipeline_conf.set_timeout(self.workflow_timeout)
        if ARGO_DEFAULT_TTL is not None:  # if None, we use the Argo defaults
            pipeline_conf.set_ttl_seconds_after_finished(ARGO_DEFAULT_TTL)

        kfp.compiler.Compiler().compile(
            self.create_kfp_pipeline_from_flow_graph(),
            pipeline_file_path,
            pipeline_conf=pipeline_conf,
        )
        return os.path.abspath(pipeline_file_path)

    def _command(
        self,
        code_package_url: str,
        environment: MetaflowEnvironment,
        step_name: str,
        step_cli: List[str],
    ) -> str:
        """
        Analogous to batch.py
        """
        commands = (
            environment.get_package_commands(code_package_url, pip_install=False)
            if self.s3_code_package
            else ["cd " + str(Path(inspect.getabsfile(self.flow.__class__)).parent)]
        )
        commands.extend(environment.bootstrap_commands(step_name))
        commands.append("echo 'Task is starting.'")
        commands.extend(step_cli)
        subshell_commands = " && ".join(
            commands
        )  # run inside subshell to capture all stdout/stderr
        # redirect stdout/stderr to separate files, using tee to display to UI
        redirection_commands = "> >(tee -a 0.stdout.log) 2> >(tee -a 0.stderr.log >&2)"

        # Creating a template to save logs to S3. This is within a function because
        # datastore_root is not available within the scope of this function, and needs
        # to be provided in the `step_op` function. f strings (AFAK) don't support
        # insertion of only a partial number of placeholder strings.
        def copy_log_cmd(log_file):
            cp_command = environment.get_boto3_copy_command(
                s3_path=(
                    f"{{datastore_root}}/{self.flow.name}/{{run_id}}/{step_name}"
                    f"/${TASK_ID_ENV_NAME}/{log_file}"
                ),
                local_path=log_file,
                command="upload_file",
            )
            return (
                f". {STEP_ENVIRONMENT_VARIABLES} "  # for $TASK_ID_ENV_NAME
                f"&& {cp_command}"
            )

        # TODO: see datastore get_log_location()
        #  where the ordinal is attempt/retry count
        cp_stderr = copy_log_cmd(log_file="0.stderr.log")
        cp_stdout = copy_log_cmd(log_file="0.stdout.log")
        cp_logs_cmd = f"{cp_stderr} && {cp_stdout}"

        # We capture the exit code at two places:
        # Once after the subshell/redirection commands, and once after the saving logs
        # command. If either of these exit codes are not 0, we exit with the nonzero
        # exit code manually because combining bash commands with ';' always results
        # in an exit code of 0, whether or not certain commands failed.
        return (
            f"({subshell_commands}) {redirection_commands}; export exit_code_1=$?; "
            f"{cp_logs_cmd}; export exit_code_2=$?; "
            f'if [ "$exit_code_1" -ne 0 ]; then exit $exit_code_1; else exit $exit_code_2; fi'
        )

    @staticmethod
    def _get_retries(node: DAGNode) -> Tuple[int, int]:
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

    @staticmethod
    def _get_resource_requirements(node: DAGNode) -> Dict[str, str]:
        """
        Get resource request or limit for a Metaflow step (node) set by @resources decorator.

        Supported parameters: 'cpu', 'cpu_limit', 'gpu', 'gpu_vendor', 'memory', 'memory_limit'
        Keys with no suffix set resource request (minimum);
        keys with 'limit' suffix sets resource limit (maximum).

        Eventually resource request and limits link back to kubernetes, see
        https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/

        Default unit for memory is megabyte, aligning with existing resource decorator usage.

        Example using resource decorator:
            @resource(cpu=0.5, cpu_limit=2, gpu=1, memory=300)
            @step
            def my_kfp_step(): ...
        """

        def to_k8s_resource_format(resource: str, value: Union[int, float, str]) -> str:
            value = str(value)

            # Defaults memory unit to megabyte
            if resource in ["memory", "memory_limit"] and value.isnumeric():
                value = f"{value}M"
            return value

        resource_requirements = {}
        for deco in node.decorators:
            if isinstance(deco, ResourcesDecorator):
                for attr_key, attr_value in deco.attributes.items():
                    if attr_value is not None:
                        resource_requirements[attr_key] = to_k8s_resource_format(
                            attr_key, attr_value
                        )

        return resource_requirements

    def create_kfp_components_from_graph(self) -> Dict[str, KfpComponent]:
        """
        Returns a map of steps to their corresponding KfpComponent.
        The KfpComponent defines the component attributes
        and step command to be used to run that particular step.
        """

        def build_kfp_component(node: DAGNode, task_id: int) -> KfpComponent:
            """
            Returns the KfpComponent for each step.
            """

            # TODO: @schedule, @environment, @timeout, @catch, etc.
            # TODO: @retry
            user_code_retries, total_retries = KubeflowPipelines._get_retries(node)

            step_cli = self._step_cli(node, task_id, user_code_retries)

            return KfpComponent(
                node.name,
                self._command(
                    self.code_package_url,
                    self.environment,
                    node.name,
                    [step_cli],
                ),
                total_retries,
                self._get_resource_requirements(node),
            )

        # Mapping of steps to their KfpComponent
        task_ids: Dict[str, int] = graph_to_task_ids(self.graph)
        step_to_kfp_component_map: Dict[str, KfpComponent] = {}
        for step_name, task_id in task_ids.items():
            node = self.graph[step_name]
            step_to_kfp_component_map[step_name] = build_kfp_component(node, task_id)

        return step_to_kfp_component_map

    def _step_cli(self, node: DAGNode, task_id: int, user_code_retries: int) -> str:
        """
        Analogous to step_functions_cli.py
        This returns the command line to run the internal Metaflow step click entrypiont.
        """
        cmds = []

        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)

        if R.use_r():
            entrypoint = [R.entrypoint()]
        else:
            entrypoint = [executable, script_name]

        kfp_run_id = "kfp-" + dsl.RUN_ID_PLACEHOLDER
        start_task_id_params_path = None

        if node.name == "start":
            # We need a separate unique ID for the special _parameters task
            task_id_params = "1-params"

            # Export user-defined parameters into runtime environment
            param_file = "parameters.sh"
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
            start_task_id_params_path = (
                "{kfp_run_id}/_parameters/{task_id_params}".format(
                    kfp_run_id=kfp_run_id, task_id_params=task_id_params
                )
            )
            exists = entrypoint + [
                "dump",
                "--max-value-size=0",
                start_task_id_params_path,
            ]
            cmd = "if ! %s >/dev/null 2>/dev/null; then %s && %s; fi" % (
                " ".join(exists),
                export_params,
                " ".join(params),
            )
            cmds.append(cmd)

        top_level = [
            "--quiet",
            "--metadata=%s" % self.metadata.TYPE,
            "--environment=%s" % self.environment.TYPE,
            "--datastore=s3",
            "--datastore-root={datastore_root}",
            "--event-logger=%s" % self.event_logger.logger_type,
            "--monitor=%s" % self.monitor.monitor_type,
            "--no-pylint",
        ]

        cmds.append(
            " ".join(
                entrypoint
                + top_level
                + [
                    "kfp step-init",
                    "--run-id %s" % kfp_run_id,
                    "--step_name %s" % node.name,
                    "--passed_in_split_indexes {passed_in_split_indexes}",
                    "--task_id %s" % task_id,  # the assigned task_id from Flow graph
                ]
            )
        )

        # load environment variables set in STEP_ENVIRONMENT_VARIABLES
        cmds.append(f". {STEP_ENVIRONMENT_VARIABLES}")

        step = [
            "--with=kfp_internal",
            "step",
            node.name,
            "--run-id %s" % kfp_run_id,
            f"--task-id ${TASK_ID_ENV_NAME}",
            # Since retries are handled by KFP Argo, we can rely on
            # {{retries}} as the job counter.
            # '--retry-count {{retries}}',  # TODO: test verify, should it be -1?
            "--max-user-code-retries %d" % user_code_retries,
            (
                "--input-paths %s" % start_task_id_params_path
                if node.name == "start"
                else f"--input-paths ${INPUT_PATHS_ENV_NAME}"
            ),
        ]

        if any(self.graph[n].type == "foreach" for n in node.in_funcs):
            step.append(f"--split-index ${SPLIT_INDEX_ENV_NAME}")

        if self.namespace:
            step.append("--namespace %s" % self.namespace)

        cmds.append(" ".join(entrypoint + top_level + step))
        return " && ".join(cmds)

    @staticmethod
    def _set_container_settings(container_op: ContainerOp, kfp_component: KfpComponent):
        resource_requirements: Dict[str, str] = kfp_component.resource_requirements
        if "memory" in resource_requirements:
            container_op.container.set_memory_request(resource_requirements["memory"])
        if "memory_limit" in resource_requirements:
            container_op.container.set_memory_limit(
                resource_requirements["memory_limit"]
            )
        if "cpu" in resource_requirements:
            container_op.container.set_cpu_request(resource_requirements["cpu"])
        if "cpu_limit" in resource_requirements:
            container_op.container.set_cpu_limit(resource_requirements["cpu_limit"])
        if "gpu" in resource_requirements:
            # TODO(yunw)(AIP-2048): Support mixture of GPU from different vendors.
            container_op.container.set_gpu_limit(
                resource_requirements["gpu"],
                vendor=resource_requirements["gpu_vendor"],
            )

    def step_op(self, step_name: str) -> Callable[..., ContainerOp]:
        """
        Workaround of KFP.components.func_to_container_op() to set KFP Component name
        """
        # KFP Component for a step defined in the Metaflow FlowSpec.
        step_op_component: Dict = yaml.load(
            kfp.components.func_to_component_text(
                kfp_step_function, base_image=self.base_image
            ),
            yaml.SafeLoader,
        )

        step_op_component["name"] = step_name
        return kfp.components.load_component_from_text(yaml.dump(step_op_component))

    def create_kfp_pipeline_from_flow_graph(self) -> Callable:
        step_to_kfp_component_map: Dict[
            str, KfpComponent
        ] = self.create_kfp_components_from_graph()

        def pipeline_transform(op: ContainerOp):
            # Disable caching because Metaflow doesn't have memoization
            op.execution_options.caching_strategy.max_cache_staleness = "P0D"

        @dsl.pipeline(name=self.name, description=self.graph.doc)
        def kfp_pipeline_from_flow(datastore_root: str = DATASTORE_SYSROOT_S3):
            visited: Dict[str, ContainerOp] = {}

            def build_kfp_dag(node: DAGNode, passed_in_split_indexes=None):
                if node.name in visited:
                    return

                op = visited[node.name] = self.step_op(node.name)(
                    datastore_root,
                    step_to_kfp_component_map[node.name].cmd_template,
                    kfp_run_id=f"kfp-{dsl.RUN_ID_PLACEHOLDER}",
                    passed_in_split_indexes=passed_in_split_indexes,
                    metaflow_service_url=METADATA_SERVICE_URL,
                )

                KubeflowPipelines._set_container_settings(
                    op, step_to_kfp_component_map[node.name]
                )

                if node.type == "foreach":
                    # Please see nested_parallelfor.ipynb for how this works
                    with kfp.dsl.ParallelFor(op.output) as split_index:
                        # build_kfp_dag() will halt when a foreach join is
                        # reached.
                        # NOTE: A Metaflow foreach node can only have one child
                        #  or one out_func
                        build_kfp_dag(
                            self.graph[node.out_funcs[0]],
                            passed_in_split_indexes=split_index,
                        )

                    # Handle the ParallelFor join step, and pass in
                    # passed_in_split_indexes of parent context
                    build_kfp_dag(
                        self.graph[node.matching_join], passed_in_split_indexes
                    )
                else:
                    for step in node.out_funcs:
                        step_node = self.graph[step]
                        if (
                            step_node.type == "join"
                            and self.graph[step_node.split_parents[-1]].type
                            == "foreach"
                        ):
                            # halt with a foreach join is reached
                            # see the ParallelFor and adjacent call to build_kfp_dag()
                            # which handles the ParallelFor join.
                            return
                        else:
                            build_kfp_dag(step_node, passed_in_split_indexes)

            build_kfp_dag(self.graph["start"])

            # Instruct KFP of the DAG order by iterating over the Metaflow
            # graph nodes.  Each Metaflow graph node has in_funcs (nodes that
            # point to this node), and we use that to instruct to KFP of the
            # order.
            # NOTE: It is the Metaflow compiler's job to check for cycles and a
            #   correctly constructed DAG (ex: splits and foreaches are joined).
            for step in self.graph.nodes:
                node = self.graph[step]
                for parent_step in node.in_funcs:
                    visited[node.name].after(visited[parent_step])

            dsl.get_pipeline_conf().add_op_transformer(pipeline_transform)
            dsl.get_pipeline_conf().set_parallelism(self.max_parallelism)
            dsl.get_pipeline_conf().set_timeout(self.workflow_timeout)
            if ARGO_DEFAULT_TTL is not None:  # # if None, we use the Argo defaults
                dsl.get_pipeline_conf().set_ttl_seconds_after_finished(ARGO_DEFAULT_TTL)

        kfp_pipeline_from_flow.__name__ = self.name
        return kfp_pipeline_from_flow
