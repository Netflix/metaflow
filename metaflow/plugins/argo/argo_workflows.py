import json
import os
import shlex
import sys
from collections import defaultdict

from metaflow import current
from metaflow.decorators import flow_decorators
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    SERVICE_HEADERS,
    SERVICE_INTERNAL_URL,
    CARD_S3ROOT,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    DEFAULT_METADATA,
    KUBERNETES_NAMESPACE,
    KUBERNETES_NODE_SELECTOR,
    KUBERNETES_SANDBOX_INIT_SCRIPT,
    KUBERNETES_SECRETS,
    S3_ENDPOINT_URL,
    AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
    CARD_AZUREROOT,
    CARD_GSROOT,
)
from metaflow.mflog import BASH_SAVE_LOGS, bash_capture_logs, export_mflog_env_vars
from metaflow.parameters import deploy_time_eval
from metaflow.util import compress_list, dict_to_cli_options, to_camelcase

from .argo_client import ArgoClient


class ArgoWorkflowsException(MetaflowException):
    headline = "Argo Workflows error"


class ArgoWorkflowsSchedulingException(MetaflowException):
    headline = "Argo Workflows scheduling error"


# List of future enhancements -
#     1. Configure Argo metrics.
#     2. Support Argo Events.
#     3. Support resuming failed workflows within Argo Workflows.
#     4. Support gang-scheduled clusters for distributed PyTorch/TF - One option is to
#        use volcano - https://github.com/volcano-sh/volcano/tree/master/example/integrations/argo
#     5. Support GitOps workflows.
#     6. Add Metaflow tags to labels/annotations.
#     7. Support Multi-cluster scheduling - https://github.com/argoproj/argo-workflows/issues/3523#issuecomment-792307297
#     8. Support for workflow notifications.
#     9. Support R lang.
#     10.Ping @savin at slack.outerbounds.co for any feature request.


class ArgoWorkflows(object):
    def __init__(
        self,
        name,
        graph,
        flow,
        code_package_sha,
        code_package_url,
        production_token,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags=None,
        namespace=None,
        username=None,
        max_workers=None,
        workflow_timeout=None,
        workflow_priority=None,
    ):
        # Some high-level notes -
        #
        # Fail-fast behavior for Argo Workflows - Argo stops
        # scheduling new steps as soon as it detects that one of the DAG nodes
        # has failed. After waiting for all the scheduled DAG nodes to run till
        # completion, Argo with fail the DAG. This implies that after a node
        # has failed, it may be awhile before the entire DAG is marked as
        # failed. There is nothing Metaflow can do here for failing even
        # faster (as of Argo 3.2).
        #
        # argo stop` vs `argo terminate` - since we don't currently
        # rely on any exit handlers, it's safe to either stop or terminate any running
        # argo workflow deployed through Metaflow. This may not hold true, once we
        # integrate with Argo Events.
        #
        # Currently, an Argo Workflow can only execute entirely within a single
        # Kubernetes namespace. Multi-cluster / Multi-namespace execution is on the
        # deck for v3.4 release for Argo Workflows; beyond which point, we will be
        # able to support them natively.
        #
        # Since this implementation generates numerous templates on the fly, please
        # ensure that your Argo Workflows controller doesn't restrict
        # templateReferencing.

        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package_sha = code_package_sha
        self.code_package_url = code_package_url
        self.production_token = production_token
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.namespace = namespace
        self.username = username
        self.max_workers = max_workers
        self.workflow_timeout = workflow_timeout
        self.workflow_priority = workflow_priority

        self.parameters = self._process_parameters()
        self._workflow_template = self._compile()
        self._cron = self._cron()

    def __str__(self):
        return str(self._workflow_template)

    def deploy(self):
        try:
            ArgoClient(namespace=KUBERNETES_NAMESPACE).register_workflow_template(
                self.name, self._workflow_template.to_json()
            )
        except Exception as e:
            raise ArgoWorkflowsException(str(e))

    @staticmethod
    def _sanitize(name):
        # Metaflow allows underscores in node names, which are disallowed in Argo
        # Workflow template names - so we swap them with hyphens which are not
        # allowed by Metaflow - guaranteeing uniqueness.
        return name.replace("_", "-")

    @classmethod
    def trigger(cls, name, parameters={}):
        try:
            workflow_template = ArgoClient(
                namespace=KUBERNETES_NAMESPACE
            ).get_workflow_template(name)
        except Exception as e:
            raise ArgoWorkflowsException(str(e))
        if workflow_template is None:
            raise ArgoWorkflowsException(
                "The workflow *%s* doesn't exist on Argo Workflows in namespace *%s*. "
                "Please deploy your flow first." % (name, KUBERNETES_NAMESPACE)
            )
        else:
            try:
                # Check that the workflow was deployed through Metaflow
                workflow_template["metadata"]["annotations"]["metaflow/owner"]
            except KeyError as e:
                raise ArgoWorkflowsException(
                    "An existing non-metaflow workflow with the same name as "
                    "*%s* already exists in Argo Workflows. \nPlease modify the "
                    "name of this flow or delete your existing workflow on Argo "
                    "Workflows before proceeding." % name
                )
        try:
            return ArgoClient(namespace=KUBERNETES_NAMESPACE).trigger_workflow_template(
                name, parameters
            )
        except Exception as e:
            raise ArgoWorkflowsException(str(e))

    def _cron(self):
        schedule = self.flow._flow_decorators.get("schedule")
        if schedule:
            # Remove the field "Year" if it exists
            return " ".join(schedule.schedule.split()[:5])
        return None

    def schedule(self):
        try:
            ArgoClient(namespace=KUBERNETES_NAMESPACE).schedule_workflow_template(
                self.name, self._cron
            )
        except Exception as e:
            raise ArgoWorkflowsSchedulingException(str(e))

    def trigger_explanation(self):
        if self._cron:
            return (
                "This workflow triggers automatically via the CronWorkflow *%s*."
                % self.name
            )
        else:
            return "No triggers defined. You need to launch this workflow manually."

    @classmethod
    def get_existing_deployment(cls, name):
        workflow_template = ArgoClient(
            namespace=KUBERNETES_NAMESPACE
        ).get_workflow_template(name)
        if workflow_template is not None:
            try:
                return (
                    workflow_template["metadata"]["annotations"]["metaflow/owner"],
                    workflow_template["metadata"]["annotations"][
                        "metaflow/production_token"
                    ],
                )
            except KeyError as e:
                raise ArgoWorkflowsException(
                    "An existing non-metaflow workflow with the same name as "
                    "*%s* already exists in Argo Workflows. \nPlease modify the "
                    "name of this flow or delete your existing workflow on Argo "
                    "Workflows before proceeding." % name
                )
        return None

    def _process_parameters(self):
        parameters = []
        has_schedule = self._cron() is not None
        seen = set()
        for var, param in self.flow._get_parameters():
            # Throw an exception if the parameter is specified twice.
            norm = param.name.lower()
            if norm in seen:
                raise MetaflowException(
                    "Parameter *%s* is specified twice. "
                    "Note that parameter names are "
                    "case-insensitive." % param.name
                )
            seen.add(norm)

            is_required = param.kwargs.get("required", False)
            # Throw an exception if a schedule is set for a flow with required
            # parameters with no defaults. We currently don't have any notion
            # of data triggers in Argo Workflows.

            # TODO: Support Argo Events for data triggering in the near future.
            if "default" not in param.kwargs and is_required and has_schedule:
                raise MetaflowException(
                    "The parameter *%s* does not have a default and is required. "
                    "Scheduling such parameters via Argo CronWorkflows is not "
                    "currently supported." % param.name
                )
            value = deploy_time_eval(param.kwargs.get("default"))
            # If the value is not required and the value is None, we set the value to
            # the JSON equivalent of None to please argo-workflows.
            if not is_required or value is not None:
                value = json.dumps(value)
            parameters.append(
                dict(name=param.name, value=value, description=param.kwargs.get("help"))
            )
        return parameters

    def _compile(self):
        # This method compiles a Metaflow FlowSpec into Argo WorkflowTemplate
        #
        # WorkflowTemplate
        #   |
        #    -- WorkflowSpec
        #         |
        #          -- Array<Template>
        #                     |
        #                      -- DAGTemplate, ContainerTemplate
        #                           |                  |
        #                            -- Array<DAGTask> |
        #                                       |      |
        #                                        -- Template
        #
        # Steps in FlowSpec are represented as DAGTasks.
        # A DAGTask can reference to -
        #     a ContainerTemplate (for linear steps..) or
        #     another DAGTemplate (for nested `foreach`s).
        #
        # While we could have very well inlined container templates inside a DAGTask,
        # unfortunately Argo variable substitution ({{pod.name}}) doesn't work as
        # expected within DAGTasks
        # (https://github.com/argoproj/argo-workflows/issues/7432) and we are forced to
        # generate container templates at the top level (in WorkflowSpec) and maintain
        # references to them within the DAGTask.

        labels = {"app.kubernetes.io/part-of": "metaflow"}

        annotations = {
            "metaflow/production_token": self.production_token,
            "metaflow/owner": self.username,
            "metaflow/user": "argo-workflows",
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

        return (
            WorkflowTemplate()
            .metadata(
                # Workflow Template metadata.
                ObjectMeta()
                .name(self.name)
                # Argo currently only supports Workflow-level namespaces. When v3.4.0
                # is released, we should be able to support multi-namespace /
                # multi-cluster scheduling.
                .namespace(KUBERNETES_NAMESPACE)
                .label("app.kubernetes.io/name", "metaflow-flow")
                .label("app.kubernetes.io/part-of", "metaflow")
                .annotations(annotations)
            )
            .spec(
                WorkflowSpec()
                # Set overall workflow timeout.
                .active_deadline_seconds(self.workflow_timeout)
                # TODO: Allow Argo to optionally archive all workflow execution logs
                #       It's disabled for now since it requires all Argo installations
                #       to enable an artifactory repository. If log archival is
                #       enabled in workflow controller, the logs for this workflow will
                #       automatically get archived.
                # .archive_logs()
                # Don't automount service tokens for now - https://github.com/kubernetes/kubernetes/issues/16779#issuecomment-159656641
                # TODO: Service account names are currently set in the templates. We
                #       can specify the default service account name here to reduce
                #       the size of the generated YAML by a tiny bit.
                # .automount_service_account_token()
                # TODO: Support ImagePullSecrets for Argo & Kubernetes
                # .image_pull_secrets(...)
                # Limit workflow parallelism
                .parallelism(self.max_workers)
                # TODO: Support Prometheus metrics for Argo
                # .metrics(...)
                # TODO: Support PodGC and DisruptionBudgets
                .priority(self.workflow_priority)
                # Set workflow metadata
                .workflow_metadata(
                    Metadata()
                    .label("app.kubernetes.io/name", "metaflow-run")
                    .label("app.kubernetes.io/part-of", "metaflow")
                    .annotations(
                        {**annotations, **{"metaflow/run_id": "argo-{{workflow.name}}"}}
                    )
                    # TODO: Set dynamic labels using labels_from. Ideally, we would
                    #       want to expose run_id as a label. It's easy to add labels,
                    #       but very difficult to remove them - let's err on the
                    #       conservative side and only add labels when we come across
                    #       use-cases for them.
                )
                # Handle parameters
                .arguments(
                    Arguments().parameters(
                        [
                            Parameter(parameter["name"])
                            .value(parameter["value"])
                            .description(parameter.get("description"))
                            # TODO: Better handle IncludeFile in Argo Workflows UI.
                            for parameter in self.parameters
                        ]
                    )
                )
                # Set common pod metadata.
                .pod_metadata(
                    Metadata()
                    .label("app.kubernetes.io/name", "metaflow-task")
                    .label("app.kubernetes.io/part-of", "metaflow")
                    .annotations(annotations)
                )
                # Set the entrypoint to flow name
                .entrypoint(self.flow.name)
                # Top-level DAG template(s)
                .templates(self._dag_templates())
                # Container templates
                .templates(self._container_templates())
            )
        )

    # Visit every node and yield the uber DAGTemplate(s).
    def _dag_templates(self):
        def _visit(node, exit_node=None, templates=[], dag_tasks=[]):
            # Every for-each node results in a separate subDAG and an equivalent
            # DAGTemplate rooted at the child of the for-each node. Each DAGTemplate
            # has a unique name - the top-level DAGTemplate is named as the name of
            # the flow and the subDAG DAGTemplates are named after the (only) descendant
            # of the for-each node.

            # Emit if we have reached the end of the sub workflow
            if exit_node is not None and exit_node is node.name:
                return templates, dag_tasks

            if node.name == "start":
                # Start node has no dependencies.
                dag_task = DAGTask(self._sanitize(node.name)).template(
                    self._sanitize(node.name)
                )
            elif (
                node.is_inside_foreach
                and self.graph[node.in_funcs[0]].type == "foreach"
            ):
                # Child of a foreach node needs input-paths as well as split-index
                # This child is the first node of the sub workflow and has no dependency
                parameters = [
                    Parameter("input-paths").value("{{inputs.parameters.input-paths}}"),
                    Parameter("split-index").value("{{inputs.parameters.split-index}}"),
                ]
                dag_task = (
                    DAGTask(self._sanitize(node.name))
                    .template(self._sanitize(node.name))
                    .arguments(Arguments().parameters(parameters))
                )
            else:
                # Every other node needs only input-paths
                parameters = [
                    Parameter("input-paths").value(
                        compress_list(
                            [
                                "argo-{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}"
                                % (n, self._sanitize(n))
                                for n in node.in_funcs
                            ]
                        )
                    )
                ]
                dag_task = (
                    DAGTask(self._sanitize(node.name))
                    .dependencies(
                        [self._sanitize(in_func) for in_func in node.in_funcs]
                    )
                    .template(self._sanitize(node.name))
                    .arguments(Arguments().parameters(parameters))
                )
            dag_tasks.append(dag_task)

            # End the workflow if we have reached the end of the flow
            if node.type == "end":
                return [
                    Template(self.flow.name).dag(
                        DAGTemplate().fail_fast().tasks(dag_tasks)
                    )
                ] + templates, dag_tasks
            # For split nodes traverse all the children
            if node.type == "split":
                for n in node.out_funcs:
                    _visit(self.graph[n], node.matching_join, templates, dag_tasks)
                return _visit(
                    self.graph[node.matching_join], exit_node, templates, dag_tasks
                )
            # For foreach nodes generate a new sub DAGTemplate
            elif node.type == "foreach":
                foreach_template_name = self._sanitize(
                    "%s-foreach-%s"
                    % (
                        node.name,
                        node.foreach_param,
                    )
                )
                foreach_task = (
                    DAGTask(foreach_template_name)
                    .dependencies([self._sanitize(node.name)])
                    .template(foreach_template_name)
                    .arguments(
                        Arguments().parameters(
                            [
                                Parameter("input-paths").value(
                                    "argo-{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}"
                                    % (node.name, self._sanitize(node.name))
                                ),
                                Parameter("split-index").value("{{item}}"),
                            ]
                        )
                    )
                    .with_param(
                        "{{tasks.%s.outputs.parameters.num-splits}}"
                        % self._sanitize(node.name)
                    )
                )
                dag_tasks.append(foreach_task)
                templates, dag_tasks_1 = _visit(
                    self.graph[node.out_funcs[0]], node.matching_join, templates, []
                )
                templates.append(
                    Template(foreach_template_name)
                    .inputs(
                        Inputs().parameters(
                            [Parameter("input-paths"), Parameter("split-index")]
                        )
                    )
                    .outputs(
                        Outputs().parameters(
                            [
                                Parameter("task-id").valueFrom(
                                    {
                                        "parameter": "{{tasks.%s.outputs.parameters.task-id}}"
                                        % self._sanitize(
                                            self.graph[node.matching_join].in_funcs[0]
                                        )
                                    }
                                )
                            ]
                        )
                    )
                    .dag(DAGTemplate().fail_fast().tasks(dag_tasks_1))
                )
                join_foreach_task = (
                    DAGTask(self._sanitize(self.graph[node.matching_join].name))
                    .template(self._sanitize(self.graph[node.matching_join].name))
                    .dependencies([foreach_template_name])
                    .arguments(
                        Arguments().parameters(
                            [
                                Parameter("input-paths").value(
                                    "argo-{{workflow.name}}/%s/{{tasks.%s.outputs.parameters}}"
                                    % (
                                        self.graph[node.matching_join].in_funcs[-1],
                                        foreach_template_name,
                                    )
                                )
                            ]
                        )
                    )
                )
                dag_tasks.append(join_foreach_task)
                return _visit(
                    self.graph[self.graph[node.matching_join].out_funcs[0]],
                    exit_node,
                    templates,
                    dag_tasks,
                )
            # For linear nodes continue traversing to the next node
            if node.type in ("linear", "join", "start"):
                return _visit(
                    self.graph[node.out_funcs[0]], exit_node, templates, dag_tasks
                )
            else:
                raise ArgoWorkflowsException(
                    "Node type *%s* for step *%s* is not currently supported by "
                    "Argo Workflows." % (node.type, node.name)
                )

        templates, _ = _visit(node=self.graph["start"])
        return templates

    # Visit every node and yield ContainerTemplates.
    def _container_templates(self):
        try:
            # Kubernetes is a soft dependency for generating Argo objects.
            # We can very well remove this dependency for Argo with the downside of
            # adding a bunch more json bloat classes (looking at you... V1Container)
            from kubernetes import client as kubernetes_sdk
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import Python package 'kubernetes'. Install kubernetes "
                "sdk (https://pypi.org/project/kubernetes/) first."
            )
        for node in self.graph:
            # Resolve entry point for pod container.
            script_name = os.path.basename(sys.argv[0])
            executable = self.environment.executable(node.name)
            # TODO: Support R someday. Quite a few people will be happy.
            entrypoint = [executable, script_name]

            # The values with curly braces '{{}}' are made available by Argo
            # Workflows. Unfortunately, there are a few bugs in Argo which prevent
            # us from accessing these values as liberally as we would like to - e.g,
            # within inline templates - so we are forced to generate container templates
            run_id = "argo-{{workflow.name}}"

            # Unfortunately, we don't have any easy access to unique ids that remain
            # stable across task attempts through Argo Workflows. So, we are forced to
            # stitch them together ourselves. The task ids are a function of step name,
            # split index and the parent task id (available from input path name).
            # Ideally, we would like these task ids to be the same as node name
            # (modulo retry suffix) on Argo Workflows but that doesn't seem feasible
            # right now.
            task_str = node.name + "-{{workflow.creationTimestamp}}"
            if node.name != "start":
                task_str += "-{{inputs.parameters.input-paths}}"
            if any(self.graph[n].type == "foreach" for n in node.in_funcs):
                task_str += "-{{inputs.parameters.split-index}}"
            # Generated task_ids need to be non-numeric - see register_task_id in
            # service.py. We do so by prefixing `t-`
            task_id_expr = (
                "export METAFLOW_TASK_ID="
                "(t-$(echo %s | md5sum | cut -d ' ' -f 1 | tail -c 9))" % task_str
            )
            task_id = "$METAFLOW_TASK_ID"

            # Resolve retry strategy.
            (
                user_code_retries,
                total_retries,
                retry_count,
                minutes_between_retries,
            ) = self._get_retries(node)

            mflog_expr = export_mflog_env_vars(
                datastore_type=self.flow_datastore.TYPE,
                stdout_path="$PWD/.logs/mflog_stdout",
                stderr_path="$PWD/.logs/mflog_stderr",
                flow_name=self.flow.name,
                run_id=run_id,
                step_name=node.name,
                task_id=task_id,
                retry_count=retry_count,
            )

            init_cmds = " && ".join(
                [
                    # For supporting sandboxes, ensure that a custom script is executed
                    # before anything else is executed. The script is passed in as an
                    # env var.
                    '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"}',
                    "mkdir -p $PWD/.logs",
                    task_id_expr,
                    mflog_expr,
                ]
                + self.environment.get_package_commands(
                    self.code_package_url, self.flow_datastore.TYPE
                )
            )
            step_cmds = self.environment.bootstrap_commands(
                node.name, self.flow_datastore.TYPE
            )

            input_paths = "{{inputs.parameters.input-paths}}"

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

            top_level = list(dict_to_cli_options(top_opts_dict)) + [
                "--quiet",
                "--metadata=%s" % self.metadata.TYPE,
                "--environment=%s" % self.environment.TYPE,
                "--datastore=%s" % self.flow_datastore.TYPE,
                "--datastore-root=%s" % self.flow_datastore.datastore_root,
                "--event-logger=%s" % self.event_logger.TYPE,
                "--monitor=%s" % self.monitor.TYPE,
                "--no-pylint",
                "--with=argo_workflows_internal",
            ]

            if node.name == "start":
                # Execute `init` before any step of the workflow executes
                task_id_params = "%s-params" % task_id
                init = (
                    entrypoint
                    + top_level
                    + [
                        "init",
                        "--run-id %s" % run_id,
                        "--task-id %s" % task_id_params,
                    ]
                    + [
                        # Parameter names can be hyphenated, hence we use
                        # {{foo.bar['param_name']}}
                        "--%s={{workflow.parameters.%s}}"
                        % (parameter["name"], parameter["name"])
                        for parameter in self.parameters
                    ]
                )
                if self.tags:
                    init.extend("--tag %s" % tag for tag in self.tags)
                # if the start step gets retried, we must be careful
                # not to regenerate multiple parameters tasks. Hence,
                # we check first if _parameters exists already.
                exists = entrypoint + [
                    "dump",
                    "--max-value-size=0",
                    "%s/_parameters/%s" % (run_id, task_id_params),
                ]
                step_cmds.extend(
                    [
                        "if ! %s >/dev/null 2>/dev/null; then %s; fi"
                        % (" ".join(exists), " ".join(init))
                    ]
                )
                input_paths = "%s/_parameters/%s" % (run_id, task_id_params)
            elif (
                node.type == "join"
                and self.graph[node.split_parents[-1]].type == "foreach"
            ):
                # Set aggregated input-paths for a foreach-join
                input_paths = (
                    "$(python -m metaflow.plugins.argo.process_input_paths %s)"
                    % input_paths
                )
            step = [
                "step",
                node.name,
                "--run-id %s" % run_id,
                "--task-id %s" % task_id,
                "--retry-count %s" % retry_count,
                "--max-user-code-retries %d" % user_code_retries,
                "--input-paths %s" % input_paths,
            ]
            if any(self.graph[n].type == "foreach" for n in node.in_funcs):
                # Pass split-index to a foreach task
                step.append("--split-index {{inputs.parameters.split-index}}")
            if self.tags:
                step.extend("--tag %s" % tag for tag in self.tags)
            if self.namespace is not None:
                step.append("--namespace=%s" % self.namespace)

            step_cmds.extend([" ".join(entrypoint + top_level + step)])

            cmd_str = "%s; c=$?; %s; exit $c" % (
                " && ".join([init_cmds, bash_capture_logs(" && ".join(step_cmds))]),
                BASH_SAVE_LOGS,
            )
            cmds = shlex.split('bash -c "%s"' % cmd_str)

            # Resolve resource requirements.
            resources = dict(
                [deco for deco in node.decorators if deco.name == "kubernetes"][
                    0
                ].attributes
            )

            if (
                resources["namespace"]
                and resources["namespace"] != KUBERNETES_NAMESPACE
            ):
                raise ArgoWorkflowsException(
                    "Multi-namespace Kubernetes execution of flows in Argo Workflows "
                    "is not currently supported. \nStep *%s* is trying to override the "
                    "default Kubernetes namespace *%s*."
                    % (node.name, KUBERNETES_NAMESPACE)
                )

            run_time_limit = [
                deco for deco in node.decorators if deco.name == "kubernetes"
            ][0].run_time_limit

            # Resolve @environment decorator. We set three classes of environment
            # variables -
            #   (1) User-specified environment variables through @environment
            #   (2) Metaflow runtime specific environment variables
            #   (3) @kubernetes, @argo_workflows_internal bookkeeping environment
            #       variables
            env = dict(
                [deco for deco in node.decorators if deco.name == "environment"][
                    0
                ].attributes["vars"]
            )
            env.update(
                {
                    **{
                        # These values are needed by Metaflow to set it's internal
                        # state appropriately
                        "METAFLOW_CODE_URL": self.code_package_url,
                        "METAFLOW_CODE_SHA": self.code_package_sha,
                        "METAFLOW_CODE_DS": self.flow_datastore.TYPE,
                        "METAFLOW_SERVICE_URL": SERVICE_INTERNAL_URL,
                        "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
                        "METAFLOW_USER": "argo-workflows",
                        "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
                        "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
                        "METAFLOW_DEFAULT_DATASTORE": self.flow_datastore.TYPE,
                        "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
                        "METAFLOW_CARD_S3ROOT": CARD_S3ROOT,
                        "METAFLOW_KUBERNETES_WORKLOAD": 1,
                        "METAFLOW_RUNTIME_ENVIRONMENT": "kubernetes",
                        "METAFLOW_OWNER": self.username,
                    },
                    **{
                        # Some optional values for bookkeeping
                        "METAFLOW_FLOW_NAME": self.flow.name,
                        "METAFLOW_STEP_NAME": node.name,
                        "METAFLOW_RUN_ID": run_id,
                        # "METAFLOW_TASK_ID": task_id,
                        "METAFLOW_RETRY_COUNT": retry_count,
                        "METAFLOW_PRODUCTION_TOKEN": self.production_token,
                        "ARGO_WORKFLOW_TEMPLATE": self.name,
                        "ARGO_WORKFLOW_NAME": "{{workflow.name}}",
                        "ARGO_WORKFLOW_NAMESPACE": KUBERNETES_NAMESPACE,
                    },
                    **self.metadata.get_runtime_environment("argo-workflows"),
                }
            )
            # add METAFLOW_S3_ENDPOINT_URL
            env["METAFLOW_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL

            # support Metaflow sandboxes
            env["METAFLOW_INIT_SCRIPT"] = KUBERNETES_SANDBOX_INIT_SCRIPT

            # Azure stuff
            env[
                "METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT"
            ] = AZURE_STORAGE_BLOB_SERVICE_ENDPOINT
            env["METAFLOW_DATASTORE_SYSROOT_AZURE"] = DATASTORE_SYSROOT_AZURE
            env["METAFLOW_CARD_AZUREROOT"] = CARD_AZUREROOT

            # GCP stuff
            env["METAFLOW_DATASTORE_SYSROOT_GS"] = DATASTORE_SYSROOT_GS
            env["METAFLOW_CARD_GSROOT"] = CARD_GSROOT

            metaflow_version = self.environment.get_environment_info()
            metaflow_version["flow_name"] = self.graph.name
            metaflow_version["production_token"] = self.production_token
            env["METAFLOW_VERSION"] = json.dumps(metaflow_version)

            # Set the template inputs and outputs for passing state. Very simply,
            # the container template takes in input-paths as input and outputs
            # the task-id (which feeds in as input-paths to the subsequent task).
            # In addition to that, if the parent of the node under consideration
            # is a for-each node, then we take the split-index as an additional
            # input. Analogously, if the node under consideration is a foreach
            # node, then we emit split cardinality as an extra output. I would like
            # to thank the designers of Argo Workflows for making this so
            # straightforward!
            inputs = []
            if node.name != "start":
                inputs.append(Parameter("input-paths"))
            if any(self.graph[n].type == "foreach" for n in node.in_funcs):
                # Fetch split-index from parent
                inputs.append(Parameter("split-index"))

            outputs = []
            if node.name != "end":
                outputs = [Parameter("task-id").valueFrom({"path": "/mnt/out/task_id"})]
            if node.type == "foreach":
                # Emit split cardinality from foreach task
                outputs.append(
                    Parameter("num-splits").valueFrom({"path": "/mnt/out/splits"})
                )

            # It makes no sense to set env vars to None (shows up as "None" string)
            env_without_none_values = {k: v for k, v in env.items() if v is not None}
            del env

            # Create a ContainerTemplate for this node. Ideally, we would have
            # liked to inline this ContainerTemplate and avoid scanning the workflow
            # twice, but due to issues with variable substitution, we will have to
            # live with this routine.
            yield (
                Template(self._sanitize(node.name))
                # Set @timeout values
                .active_deadline_seconds(run_time_limit)
                # Set service account
                .service_account_name(resources["service_account"])
                # Configure template input
                .inputs(Inputs().parameters(inputs))
                # Configure template output
                .outputs(Outputs().parameters(outputs))
                # Fail fast!
                .fail_fast()
                # Set @retry/@catch values
                .retry_strategy(
                    times=total_retries,
                    minutes_between_retries=minutes_between_retries,
                ).metadata(
                    ObjectMeta().annotation("metaflow/step_name", node.name)
                    # Unfortunately, we can't set the task_id since it is generated
                    # inside the pod. However, it can be inferred from the annotation
                    # set by argo-workflows - `workflows.argoproj.io/outputs` - refer
                    # the field 'task-id' in 'parameters'
                    # .annotation("metaflow/task_id", ...)
                    .annotation("metaflow/attempt", retry_count)
                )
                # Set emptyDir volume for state management
                .empty_dir_volume("out")
                # Set node selectors
                .node_selectors(
                    {
                        str(k.split("=", 1)[0]): str(k.split("=", 1)[1])
                        for k in (
                            resources.get("node_selector")
                            or (
                                KUBERNETES_NODE_SELECTOR.split(",")
                                if KUBERNETES_NODE_SELECTOR
                                else []
                            )
                        )
                    }
                )
                # Set container
                .container(
                    # TODO: Unify the logic with kubernetes.py
                    # Important note - Unfortunately, V1Container uses snakecase while
                    # Argo Workflows uses camel. For most of the attributes, both cases
                    # are indistinguishable, but unfortunately, not for all - (
                    # env_from, value_from, etc.) - so we need to handle the conversion
                    # ourselves using to_camelcase. We need to be vigilant about
                    # resources attributes in particular where the keys maybe user
                    # defined.
                    to_camelcase(
                        kubernetes_sdk.V1Container(
                            name=self._sanitize(node.name),
                            command=cmds,
                            env=[
                                kubernetes_sdk.V1EnvVar(name=k, value=str(v))
                                for k, v in env_without_none_values.items()
                            ]
                            # Add environment variables for book-keeping.
                            # https://argoproj.github.io/argo-workflows/fields/#fields_155
                            + [
                                kubernetes_sdk.V1EnvVar(
                                    name=k,
                                    value_from=kubernetes_sdk.V1EnvVarSource(
                                        field_ref=kubernetes_sdk.V1ObjectFieldSelector(
                                            field_path=str(v)
                                        )
                                    ),
                                )
                                for k, v in {
                                    "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                                    "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                                    "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
                                    "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME": "spec.serviceAccountName",
                                }.items()
                            ],
                            image=resources["image"],
                            resources=kubernetes_sdk.V1ResourceRequirements(
                                requests={
                                    "cpu": str(resources["cpu"]),
                                    "memory": "%sM" % str(resources["memory"]),
                                    "ephemeral-storage": "%sM" % str(resources["disk"]),
                                },
                                limits={
                                    "%s.com/gpu".lower()
                                    % resources["gpu_vendor"]: str(resources["gpu"])
                                    for k in [0]
                                    if resources["gpu"] is not None
                                },
                            ),
                            # Configure secrets
                            env_from=[
                                kubernetes_sdk.V1EnvFromSource(
                                    secret_ref=kubernetes_sdk.V1SecretEnvSource(
                                        name=str(k),
                                        # optional=True
                                    )
                                )
                                for k in list(
                                    []
                                    if not resources.get("secrets")
                                    else [resources.get("secrets")]
                                    if isinstance(resources.get("secrets"), str)
                                    else resources.get("secrets")
                                )
                                + KUBERNETES_SECRETS.split(",")
                                if k
                            ],
                            # Assign a volume point to pass state to the next task.
                            volume_mounts=[
                                kubernetes_sdk.V1VolumeMount(
                                    name="out", mount_path="/mnt/out"
                                )
                            ],
                        ).to_dict()
                    )
                )
            )

    def _get_retries(self, node):
        max_user_code_retries = 0
        max_error_retries = 0
        minutes_between_retries = "2"
        for deco in node.decorators:
            if deco.name == "retry":
                minutes_between_retries = deco.attributes.get(
                    "minutes_between_retries", minutes_between_retries
                )
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        return (
            max_user_code_retries,
            max_user_code_retries + max_error_retries,
            # {{retries}} is only available if retryStrategy is specified
            "{{retries}}" if max_user_code_retries + max_error_retries else 0,
            int(minutes_between_retries),
        )


# Helper classes to assist with JSON-foo. This can very well replaced with an explicit
# dependency on argo-workflows Python SDK if this method turns out to be painful.
# TODO: Autogenerate them, maybe?


class WorkflowTemplate(object):
    # https://argoproj.github.io/argo-workflows/fields/#workflowtemplate

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["apiVersion"] = "argoproj.io/v1alpha1"
        self.payload["kind"] = "WorkflowTemplate"

    def metadata(self, object_meta):
        self.payload["metadata"] = object_meta.to_json()
        return self

    def spec(self, workflow_spec):
        self.payload["spec"] = workflow_spec.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class ObjectMeta(object):
    # https://argoproj.github.io/argo-workflows/fields/#objectmeta

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def annotation(self, key, value):
        self.payload["annotations"][key] = str(value)
        return self

    def annotations(self, annotations):
        if "annotations" not in self.payload:
            self.payload["annotations"] = {}
        self.payload["annotations"].update(annotations)
        return self

    def generate_name(self, generate_name):
        self.payload["generateName"] = generate_name
        return self

    def label(self, key, value):
        self.payload["labels"][key] = str(value)
        return self

    def labels(self, labels):
        if "labels" not in self.payload:
            self.payload["labels"] = {}
        self.payload["labels"].update(labels)
        return self

    def name(self, name):
        self.payload["name"] = name
        return self

    def namespace(self, namespace):
        self.payload["namespace"] = namespace
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class WorkflowSpec(object):
    # https://argoproj.github.io/argo-workflows/fields/#workflowspec
    # This object sets all Workflow level properties.

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def active_deadline_seconds(self, active_deadline_seconds):
        # Overall duration of a workflow in seconds
        if active_deadline_seconds is not None:
            self.payload["activeDeadlineSeconds"] = int(active_deadline_seconds)
        return self

    def automount_service_account_token(self, mount=True):
        self.payload["automountServiceAccountToken"] = mount
        return self

    def arguments(self, arguments):
        self.payload["arguments"] = arguments.to_json()
        return self

    def archive_logs(self, archive_logs=True):
        self.payload["archiveLogs"] = archive_logs
        return self

    def entrypoint(self, entrypoint):
        self.payload["entrypoint"] = entrypoint
        return self

    def parallelism(self, parallelism):
        # Set parallelism at Workflow level
        self.payload["parallelism"] = int(parallelism)
        return self

    def pod_metadata(self, metadata):
        self.payload["podMetadata"] = metadata.to_json()
        return self

    def priority(self, priority):
        if priority is not None:
            self.payload["priority"] = int(priority)
        return self

    def workflow_metadata(self, workflow_metadata):
        self.payload["workflowMetadata"] = workflow_metadata.to_json()
        return self

    def service_account_name(self, service_account_name):
        # https://argoproj.github.io/argo-workflows/workflow-rbac/
        self.payload["serviceAccountName"] = service_account_name
        return self

    def templates(self, templates):
        if "templates" not in self.payload:
            self.payload["templates"] = []
        for template in templates:
            self.payload["templates"].append(template.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class Metadata(object):
    # https://argoproj.github.io/argo-workflows/fields/#metadata

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def annotation(self, key, value):
        self.payload["annotations"][key] = str(value)
        return self

    def annotations(self, annotations):
        if "annotations" not in self.payload:
            self.payload["annotations"] = {}
        self.payload["annotations"].update(annotations)
        return self

    def label(self, key, value):
        self.payload["labels"][key] = str(value)
        return self

    def labels(self, labels):
        if "labels" not in self.payload:
            self.payload["labels"] = {}
        self.payload["labels"].update(labels)
        return self

    def labels_from(self, labels_from):
        # Only available in workflow_metadata
        # https://github.com/argoproj/argo-workflows/blob/master/examples/label-value-from-workflow.yaml
        if "labelsFrom" not in self.payload:
            self.payload["labelsFrom"] = {}
        for k, v in labels_from.items():
            self.payload["labelsFrom"].update({k: {"expression": v}})
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class Template(object):
    # https://argoproj.github.io/argo-workflows/fields/#template

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def active_deadline_seconds(self, active_deadline_seconds):
        # Overall duration of a pod in seconds, only obeyed for container templates
        # Used for implementing @timeout.
        self.payload["activeDeadlineSeconds"] = int(active_deadline_seconds)
        return self

    def dag(self, dag_template):
        self.payload["dag"] = dag_template.to_json()
        return self

    def container(self, container):
        # Luckily this can simply be V1Container and we are spared from writing more
        # boilerplate - https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Container.md.
        self.payload["container"] = container
        return self

    def inputs(self, inputs):
        self.payload["inputs"] = inputs.to_json()
        return self

    def outputs(self, outputs):
        self.payload["outputs"] = outputs.to_json()
        return self

    def fail_fast(self, fail_fast=True):
        # https://github.com/argoproj/argo-workflows/issues/1442
        self.payload["failFast"] = fail_fast
        return self

    def metadata(self, metadata):
        self.payload["metadata"] = metadata.to_json()
        return self

    def service_account_name(self, service_account_name):
        self.payload["serviceAccountName"] = service_account_name
        return self

    def retry_strategy(self, times, minutes_between_retries):
        if times > 0:
            self.payload["retryStrategy"] = {
                "retryPolicy": "Always",
                "limit": times,
                "backoff": {"duration": "%sm" % minutes_between_retries},
            }
        return self

    def empty_dir_volume(self, name):
        # Attach an emptyDir volume
        # https://argoproj.github.io/argo-workflows/empty-dir/
        if "volumes" not in self.payload:
            self.payload["volumes"] = []
        self.payload["volumes"].append({"name": name, "emptyDir": {}})
        return self

    def node_selectors(self, node_selectors):
        if "nodeSelector" not in self.payload:
            self.payload["labels"] = {}
        self.payload["nodeSelector"].update(node_selectors)
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Inputs(object):
    # https://argoproj.github.io/argo-workflows/fields/#inputs

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Outputs(object):
    # https://argoproj.github.io/argo-workflows/fields/#outputs

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Parameter(object):
    # https://argoproj.github.io/argo-workflows/fields/#parameter

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def value(self, value):
        self.payload["value"] = value
        return self

    def default(self, value):
        self.payload["default"] = value
        return self

    def valueFrom(self, value_from):
        self.payload["valueFrom"] = value_from
        return self

    def description(self, description):
        self.payload["description"] = description
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class DAGTemplate(object):
    # https://argoproj.github.io/argo-workflows/fields/#dagtemplate

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def fail_fast(self, fail_fast=True):
        # https://github.com/argoproj/argo-workflows/issues/1442
        self.payload["failFast"] = fail_fast
        return self

    def tasks(self, tasks):
        if "tasks" not in self.payload:
            self.payload["tasks"] = []
        for task in tasks:
            self.payload["tasks"].append(task.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class DAGTask(object):
    # https://argoproj.github.io/argo-workflows/fields/#dagtask

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def arguments(self, arguments):
        self.payload["arguments"] = arguments.to_json()
        return self

    def dependencies(self, dependencies):
        self.payload["dependencies"] = dependencies
        return self

    def template(self, template):
        # Template reference
        self.payload["template"] = template
        return self

    def inline(self, template):
        # We could have inlined the template here but
        # https://github.com/argoproj/argo-workflows/issues/7432 prevents us for now.
        self.payload["inline"] = template.to_json()
        return self

    def with_param(self, with_param):
        self.payload["withParam"] = with_param
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Arguments(object):
    # https://argoproj.github.io/argo-workflows/fields/#arguments

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)
