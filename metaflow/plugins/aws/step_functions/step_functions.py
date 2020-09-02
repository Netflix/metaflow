import os
from collections import defaultdict
import sys
import json
import time
import string
import random
import uuid

from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.plugins import ResourcesDecorator, BatchDecorator, RetryDecorator
from metaflow.parameters import deploy_time_eval
from metaflow.util import compress_list, dict_to_cli_options, to_pascalcase
from metaflow.metaflow_config import SFN_IAM_ROLE, \
    EVENTS_SFN_ACCESS_IAM_ROLE, SFN_DYNAMO_DB_TABLE
from metaflow import R

from .step_functions_client import StepFunctionsClient
from .event_bridge_client import EventBridgeClient
from ..batch.batch import Batch


class StepFunctionsException(MetaflowException):
    headline = 'AWS Step Functions error'

class StepFunctionsSchedulingException(MetaflowException):
    headline = 'AWS Step Functions scheduling error'

class StepFunctions(object):

    def __init__(self,
                 name,
                 graph,
                 flow,
                 code_package,
                 code_package_url,
                 production_token,
                 metadata,
                 datastore,
                 environment,
                 event_logger,
                 monitor,
                 tags=None,
                 namespace=None,
                 username=None,
                 max_workers=None,
                 workflow_timeout=None):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package = code_package
        self.code_package_url = code_package_url
        self.production_token = production_token
        self.metadata = metadata
        self.datastore = datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.namespace = namespace
        self.username = username
        self.max_workers = max_workers
        self.workflow_timeout = workflow_timeout
        
        self._client = StepFunctionsClient()
        self._workflow = self._compile()
        self._cron = self._cron()
        self._state_machine_arn = None

    def to_json(self):
        return self._workflow.to_json(pretty=True)

    def trigger_explanation(self):
        if self._cron:
            # Sometime in the future, we should vendor (or write) a utility
            # that can translate cron specifications into a human readable 
            # format and push to the user for a better UX, someday.
            return 'This workflow triggers automatically '\
                'via a cron schedule *%s* defined in AWS EventBridge.' \
                % self.name
        else:
            return 'No triggers defined. '\
                'You need to launch this workflow manually.'

    def deploy(self):
        if SFN_IAM_ROLE is None:
            raise StepFunctionsException("No IAM role found for AWS Step "
                                         "Functions. You can create one "
                                         "following the instructions listed at "
                                         "*https://admin-docs.metaflow.org/meta"
                                         "flow-on-aws/deployment-guide/manual-d"
                                         "eployment#scheduling* and "
                                         "re-configure Metaflow using "
                                         "*metaflow configure aws* on your "
                                         "terminal.")
        try:
            self._state_machine_arn = self._client.push(
                    name = self.name, 
                    definition = self.to_json(), 
                    roleArn = SFN_IAM_ROLE
                )
        except Exception as e:
            raise StepFunctionsException(repr(e))

    def schedule(self):
        # Scheduling is currently enabled via AWS Event Bridge.
        if EVENTS_SFN_ACCESS_IAM_ROLE is None:
            raise StepFunctionsSchedulingException("No IAM role found for AWS "
                                                   "Events Bridge. You can "
                                                   "create one following the "
                                                   "instructions listed at "
                                                   "*https://admin-docs.metaflo"
                                                   "w.org/metaflow-on-aws/deplo"
                                                   "yment-guide/manual-deployme"
                                                   "nt#scheduling* and "
                                                   "re-configure Metaflow "
                                                   "using *metaflow configure "
                                                   "aws* on your terminal.")
        try:
            EventBridgeClient(self.name) \
                .cron(self._cron) \
                .role_arn(EVENTS_SFN_ACCESS_IAM_ROLE) \
                .state_machine_arn(self._state_machine_arn) \
                .schedule()
        except Exception as e:
            raise StepFunctionsSchedulingException(repr(e))

    @classmethod
    def trigger(cls, name, parameters):
        try:
            state_machine = StepFunctionsClient().get(name)
        except Exception as e:
            raise StepFunctionsException(repr(e))
        if state_machine is None:
            raise StepFunctionsException("The workflow *%s* doesn't exist "
                                         "on AWS Step Functions. Please "
                                         "deploy your flow first." % name)
        # Dump parameters into `Parameters` input field.
        input = json.dumps({"Parameters" : json.dumps(parameters)})
        # AWS Step Functions limits input to be 32KiB, but AWS Batch
        # has it's own limitation of 30KiB for job specification length.
        # Reserving 10KiB for rest of the job sprecification leaves 20KiB
        # for us, which should be enough for most use cases for now.
        if len(input) > 20480:
            raise StepFunctionsException("Length of parameter names and "
                                         "values shouldn't exceed 20480 as "
                                         "imposed by AWS Step Functions.")
        try:
            state_machine_arn = state_machine.get('stateMachineArn')
            return StepFunctionsClient().trigger(state_machine_arn, input)
        except Exception as e:
            raise StepFunctionsException(repr(e))

    @classmethod
    def list(cls, name, states):
        try:
            state_machine = StepFunctionsClient().get(name)
        except Exception as e:
            raise StepFunctionsException(repr(e))
        if state_machine is None:
            raise StepFunctionsException("The workflow *%s* doesn't exist "
                                         "on AWS Step Functions." % name)
        try:
            state_machine_arn = state_machine.get('stateMachineArn')
            return StepFunctionsClient() \
                .list_executions(state_machine_arn, states)
        except Exception as e:
            raise StepFunctionsException(repr(e))

    @classmethod
    def get_existing_deployment(cls, name):
        workflow = StepFunctionsClient().get(name)
        if workflow is not None:
            try:
                start = json.loads(workflow['definition'])['States']['start']
                parameters = start['Parameters']['Parameters']
                return parameters.get('metaflow.owner'), \
                    parameters.get('metaflow.production_token')
            except KeyError as e:
                raise StepFunctionsException("An exisiting non-metaflow "
                                             "workflow with the same name as "
                                             "*%s* already exists in AWS Step "
                                             "Functions. Please modify the "
                                             "name of this flow or delete your "
                                             "existing workflow on AWS Step "
                                             "Functions." % name)
        return None

    def _compile(self):

        # Visit every node of the flow and recursively build the state machine.
        def _visit(node, workflow, exit_node=None):
            # Assign an AWS Batch job to the AWS Step Functions state
            # and pass the intermediate state by exposing `JobId` and 
            # `Parameters` to the child job(s) as outputs. `Index` and 
            # `SplitParentTaskId` are populated optionally, when available.

            # We can't modify the names of keys in AWS Step Functions aside
            # from a blessed few which are set as `Parameters` for the Map
            # state. That's why even though `JobId` refers to the parent task 
            # id, we can't call it as such. Similar situation for `Parameters`.
            state = State(node.name) \
                        .batch(self._batch(node)) \
                        .output_path('$.[\'JobId\', '
                                    '\'Parameters\', '
                                    '\'Index\', '
                                    '\'SplitParentTaskId\']')
            # End the (sub)workflow if we have reached the end of the flow or
            # the parent step of matching_join of the sub workflow.
            if node.type == 'end' or exit_node in node.out_funcs:
                workflow.add_state(state.end())
            # Continue linear assignment within the (sub)workflow if the node 
            # doesn't branch or fork.
            elif node.type in ('linear', 'join'):
                workflow.add_state(state.next(node.out_funcs[0]))
                _visit(self.graph[node.out_funcs[0]], workflow, exit_node)
            # Create a `Parallel` state and assign sub workflows if the node
            # branches out.
            elif node.type == 'split-and':
                branch_name = '&'.join(node.out_funcs)
                workflow.add_state(state.next(branch_name))
                branch = Parallel(branch_name) \
                            .next(node.matching_join)
                # Generate as many sub workflows as branches and recurse.
                for n in node.out_funcs:
                    branch.branch(
                        _visit(
                            self.graph[n], 
                            Workflow(n).start_at(n), 
                            node.matching_join))
                workflow.add_state(branch)
                # Continue the traversal from the matching_join.
                _visit(self.graph[node.matching_join], workflow, exit_node)
            # Create a `Map` state and assign sub workflow if the node forks.
            elif node.type == 'foreach':
                # Fetch runtime cardinality via an AWS DynamoDb Get call before
                # configuring the node
                cardinality_state_name = '#%s' % node.out_funcs[0]
                workflow.add_state(state.next(cardinality_state_name))
                cardinality_state = State(cardinality_state_name) \
                                        .dynamo_db(SFN_DYNAMO_DB_TABLE, 
                                            '$.JobId', 
                                            'for_each_cardinality') \
                                        .result_path('$.Result')
                iterator_name = '*%s' % node.out_funcs[0]
                workflow.add_state(cardinality_state.next(iterator_name))
                workflow.add_state(
                    Map(iterator_name) \
                        .items_path('$.Result.Item.for_each_cardinality.NS') \
                        .parameter('JobId.$', '$.JobId') \
                        .parameter('SplitParentTaskId.$', '$.JobId') \
                        .parameter('Parameters.$', '$.Parameters') \
                        .parameter('Index.$', '$$.Map.Item.Value') \
                        .next(node.matching_join) \
                        .iterator(
                            _visit(
                                self.graph[node.out_funcs[0]], 
                                Workflow(node.out_funcs[0]) \
                                    .start_at(node.out_funcs[0]), 
                                node.matching_join)) \
                        .max_concurrency(self.max_workers) \
                        .output_path('$.[0]'))
                # Continue the traversal from the matching_join.
                _visit(self.graph[node.matching_join], workflow, exit_node)
            # We shouldn't ideally ever get here.
            else:
                raise StepFunctionsException("Node type *%s* for  step *%s* "
                                             "is not currently supported by "
                                             "AWS Step Functions." 
                                             % (node.type, node.name))
            return workflow

        workflow = Workflow(self.name) \
                        .start_at('start')
        if self.workflow_timeout:
            workflow.timeout_seconds(self.workflow_timeout)
        return _visit(self.graph['start'], workflow)

    def _cron(self):
        schedule = self.flow._flow_decorators.get('schedule')
        if schedule:
            return schedule.schedule
        return None

    def _process_parameters(self):
        parameters = []
        has_schedule = self._cron() is not None
        for var, param in self.flow._get_parameters():
            valuetype = param.kwargs.get('type', str)
            value = deploy_time_eval(param.kwargs.get('default'))
            required = param.kwargs.get('required', False)
            # Throw an exception if the flow has optional parameters
            # with no default value.
            if value is None and required is False:
                raise MetaflowException("The value of parameter *%s* is "
                                        "ambiguous. It does not have a "
                                        "default and it is not required."
                                        % param.name)
            # Throw an exception if a schedule is set for a flow with required
            # parameters with no defaults. We currently don't have any notion
            # of data triggers in AWS Event Bridge.
            if value is None and required and has_schedule:
                raise MetaflowException("The parameter *%s* does not have a "
                                        "default and is required. Scheduling "
                                        "such parameters via AWS Event Bridge "
                                        "is not currently supported."
                                        % param.name)
            parameters.append(dict(name=param.name,
                                   value=value))
        return parameters

    def _batch(self, node):
        attrs = {
            # metaflow.user is only used for setting the AWS Job Name.
            # Since job executions are no longer tied to a specific user
            # identity, we will just set their user to `SFN`. We still do need
            # access to the owner of the workflow for production tokens, which 
            # we can stash in metaflow.owner.
            'metaflow.user': 'SFN',
            'metaflow.owner': self.username,
            'metaflow.flow_name': self.flow.name,
            'metaflow.step_name': node.name,
            'metaflow.run_id.$': '$$.Execution.Name',
            # Unfortunately we can't set the task id here since AWS Step 
            # Functions lacks any notion of run-scoped task identifiers. We 
            # instead co-opt the AWS Batch job id as the task id. This also
            # means that the AWS Batch job name will have missing fields since
            # the job id is determined at job execution, but since the job id is
            # part of the job description payload, we don't lose much except for
            # a few ugly looking black fields in the AWS Batch UI.
            
            # Also, unfortunately we can't set the retry count since 
            # `$$.State.RetryCount` resolves to an int dynamically and 
            # AWS Batch job specification only accepts strings. We handle 
            # retries/catch within AWS Batch to get around this limitation.
            'metaflow.version': self.environment.get_environment_info()[
                'metaflow_version'
            ],
            # We rely on step names and task ids of parent steps to construct
            # input paths for a task. Since the only information we can pass 
            # between states (via `InputPath` and `ResultPath`) in AWS Step 
            # Functions is the job description, we run the risk of exceeding 
            # 32K state size limit rather quickly if we don't filter the job 
            # description to a minimal set of fields. Unfortunately, the partial
            # `JsonPath` implementation within AWS Step Functions makes this 
            # work a little non-trivial; it doesn't like dots in keys, so we
            # have to add the field again. 
            # This pattern is repeated in a lot of other places, where we use 
            # AWS Batch parameters to store AWS Step Functions state 
            # information, since this field is the only field in the AWS Batch 
            # specification that allows us to set key-values.
            'step_name': node.name
        }

        # Store production token within the `start` step, so that subsequent
        # `step-functions create` calls can perform a rudimentary authorization
        # check.
        if node.name == 'start':
            attrs['metaflow.production_token'] = self.production_token

        # Add env vars from the optional @environment decorator.
        env_deco = [deco for deco in node.decorators 
                        if deco.name == 'environment']
        env = {}
        if env_deco:
            env = env_deco[0].attributes['vars']

        if node.name == 'start':
            # Initialize parameters for the flow in the `start` step.
            parameters = self._process_parameters()
            if parameters:
                # Get user-defined parameters from State Machine Input.
                # Since AWS Step Functions doesn't allow for optional inputs
                # currently, we have to unfortunately place an artificial 
                # constraint that every parameterized workflow needs to include
                # `Parameters` as a key in the input to the workflow. 
                # `step-functions trigger` already takes care of this 
                # requirement, but within the UI, the users will be required to
                # specify an input with key as `Parameters` and value as a
                # stringified json of the actual parameters - 
                # {"Parameters": "{\"alpha\": \"beta\"}"}
                env['METAFLOW_PARAMETERS'] = '$.Parameters'
                default_parameters = {}
                for parameter in parameters:
                    if parameter['value'] is not None:
                        default_parameters[parameter['name']] = \
                            parameter['value']
                # Dump the default values specified in the flow.
                env['METAFLOW_DEFAULT_PARAMETERS'] = \
                    json.dumps(default_parameters)
            # `start` step has no upstream input dependencies aside from
            # parameters.
            input_paths = None
        else:
            # We need to rely on the `InputPath` of the AWS Step Functions
            # specification to grab task ids and the step names of the parent
            # to properly construct input_paths at runtime. Thanks to the 
            # JsonPath-foo embedded in the parent states, we have this 
            # information easily available. 

            # Handle foreach join.
            if node.type == 'join' and \
                 self.graph[node.split_parents[-1]].type == 'foreach':
                input_paths = \
                    'sfn-${METAFLOW_RUN_ID}/%s/:' \
                        '${METAFLOW_PARENT_TASK_IDS}' % node.in_funcs[0]
                # Unfortunately, AWS Batch only allows strings as value types
                # in it's specification and we don't have any way to concatenate
                # the task ids array from the parent steps within AWS Step 
                # Functions and pass it down to AWS Batch. We instead have to 
                # rely on publishing the state to DynamoDb and fetching it back
                # in within the AWS Batch entry point to set
                # `METAFLOW_PARENT_TASK_IDS`. The state is scoped to the parent
                # foreach task `METAFLOW_SPLIT_PARENT_TASK_ID`. We decided on
                # AWS DynamoDb and not AWS Lambdas, because deploying and 
                # debugging Lambdas would be a nightmare as far as OSS support 
                # is concerned.
                env['METAFLOW_SPLIT_PARENT_TASK_ID'] = \
                    '$.Parameters.split_parent_task_id_%s' % \
                        node.split_parents[-1]
            else:
                # Set appropriate environment variables for runtime replacement.
                if len(node.in_funcs) == 1:
                    input_paths = \
                        'sfn-${METAFLOW_RUN_ID}/%s/${METAFLOW_PARENT_TASK_ID}' \
                            % node.in_funcs[0]
                    env['METAFLOW_PARENT_TASK_ID'] = '$.JobId'
                else:
                    # Generate the input paths in a quasi-compressed format.
                    # See util.decompress_list for why this is written the way 
                    # it is.
                    input_paths = 'sfn-${METAFLOW_RUN_ID}:' + ','.join(
                        '/${METAFLOW_PARENT_%s_STEP}/'
                            '${METAFLOW_PARENT_%s_TASK_ID}' % (idx, idx) 
                            for idx, _ in enumerate(node.in_funcs))
                    for idx, _ in enumerate(node.in_funcs):
                        env['METAFLOW_PARENT_%s_TASK_ID' % idx] = \
                            '$.[%s].JobId' % idx
                        env['METAFLOW_PARENT_%s_STEP' % idx] = \
                            '$.[%s].Parameters.step_name' % idx
            env['METAFLOW_INPUT_PATHS'] = input_paths

            if node.is_inside_foreach:
                # Set the task id of the parent job of the foreach split in 
                # our favorite dumping ground, the AWS Batch attrs. For 
                # subsequent descendent tasks, this attrs blob becomes the 
                # input to those descendent tasks. We set and propagate the
                # task ids pointing to split_parents through every state.
                if any(self.graph[n].type == 'foreach' for n in node.in_funcs):
                    attrs['split_parent_task_id_%s.$' % \
                        node.split_parents[-1]] = '$.SplitParentTaskId'
                    for parent in node.split_parents[:-1]:
                        if self.graph[parent].type == 'foreach':
                            attrs['split_parent_task_id_%s.$' % parent] = \
                                '$.Parameters.split_parent_task_id_%s' % parent
                elif node.type == 'join':
                    if self.graph[node.split_parents[-1]].type == 'foreach':
                        # A foreach join only gets one set of input from the
                        # parent tasks. We filter the Map state to only output
                        # `$.[0]`, since we don't need any of the other outputs,
                        # that information is available to us from AWS DynamoDB.
                        # This has a nice side-effect of making our foreach
                        # splits infinitely scalable because otherwise we would
                        # be bounded by the 32K state limit for the outputs. So,
                        # instead of referencing `Parameters` fields by index
                        # (like in `split-and`), we can just reference them
                        # directly.
                        attrs['split_parent_task_id_%s.$' % \
                            node.split_parents[-1]] = \
                                '$.Parameters.split_parent_task_id_%s' % \
                                    node.split_parents[-1]
                        for parent in node.split_parents[:-1]:
                            if self.graph[parent].type == 'foreach':
                                attrs['split_parent_task_id_%s.$' % parent] = \
                                    '$.Parameters.split_parent_task_id_%s' % \
                                        parent
                    else:
                        for parent in node.split_parents:
                            if self.graph[parent].type == 'foreach':
                                attrs['split_parent_task_id_%s.$' % parent] = \
                                '$.[0].Parameters.split_parent_task_id_%s' % \
                                    parent
                else:
                    for parent in node.split_parents:
                        if self.graph[parent].type == 'foreach':
                            attrs['split_parent_task_id_%s.$' % parent] = \
                                '$.Parameters.split_parent_task_id_%s' % parent

                # Set `METAFLOW_SPLIT_PARENT_TASK_ID_FOR_FOREACH_JOIN` if the 
                # next transition is to a foreach join, so that the 
                # stepfunctions decorator can write the mapping for input path
                # to DynamoDb.
                if any(self.graph[n].type == 'join' and \
                        self.graph[self.graph[n].split_parents[-1]].type == \
                        'foreach'
                            for n in node.out_funcs):
                    env['METAFLOW_SPLIT_PARENT_TASK_ID_FOR_FOREACH_JOIN'] = \
                            attrs['split_parent_task_id_%s.$' % \
                                self.graph[node.out_funcs[0]].split_parents[-1]]

                # Set ttl for the values we set in AWS DynamoDB.
                if node.type == 'foreach':
                    if self.workflow_timeout:
                        env['METAFLOW_SFN_WORKFLOW_TIMEOUT'] = \
                            self.workflow_timeout

            # Handle split index for for-each.
            if any(self.graph[n].type == 'foreach' for n in node.in_funcs):
                env['METAFLOW_SPLIT_INDEX'] = '$.Index'

        env['METAFLOW_CODE_URL'] = self.code_package_url
        env['METAFLOW_FLOW_NAME'] = attrs['metaflow.flow_name']
        env['METAFLOW_STEP_NAME'] = attrs['metaflow.step_name']
        env['METAFLOW_RUN_ID'] = attrs['metaflow.run_id.$']
        env['METAFLOW_PRODUCTION_TOKEN'] = self.production_token
        env['SFN_STATE_MACHINE'] = self.name
        #env['METAFLOW_USER'] = attrs['metaflow.owner']
        # Can't set `METAFLOW_TASK_ID` due to lack of run-scoped identifiers.
        # We will instead rely on `AWS_BATCH_JOB_ID` as the task identifier.
        # Can't set `METAFLOW_RETRY_COUNT` either due to integer casting issue.
        metadata_env = self.metadata.get_runtime_environment('step-functions')
        env.update(metadata_env)

        metaflow_version = self.environment.get_environment_info()
        metaflow_version['flow_name'] = self.graph.name
        metaflow_version['production_token'] = self.production_token
        env['METAFLOW_VERSION'] = json.dumps(metaflow_version)

        # Set AWS DynamoDb Table Name for state tracking for for-eaches.
        # There are three instances when metaflow runtime directly interacts
        # with AWS DynamoDB.
        #   1. To set the cardinality of foreaches (which are subsequently)
        #      read prior to the instantiation of the Map state by AWS Step
        #      Functions.
        #   2. To set the input paths from the parent steps of a foreach join.
        #   3. To read the input paths in a foreach join.
        if node.type == 'foreach' or \
            (node.is_inside_foreach and \
                any(self.graph[n].type == 'join' and \
                    self.graph[self.graph[n].split_parents[-1]].type == \
                        'foreach' for n in node.out_funcs)) or \
            (node.type == 'join' and \
                self.graph[node.split_parents[-1]].type == 'foreach'):
            if SFN_DYNAMO_DB_TABLE is None:
                raise StepFunctionsException("An AWS DynamoDB table is needed "
                                             "to support foreach in your flow. "
                                             "You can create one following the "
                                             "instructions listed at *https://a"
                                             "dmin-docs.metaflow.org/metaflow-o"
                                             "n-aws/deployment-guide/manual-dep"
                                             "loyment#scheduling* and "
                                             "re-configure Metaflow using "
                                             "*metaflow configure aws* on your "
                                             "terminal.")
            env['METAFLOW_SFN_DYNAMO_DB_TABLE'] = SFN_DYNAMO_DB_TABLE

        # Resolve AWS Batch resource requirements.
        batch_deco = [deco for deco in node.decorators
                        if deco.name == 'batch'][0]
        resources = batch_deco.attributes

        # Resolve retry strategy.
        user_code_retries, total_retries= self._get_retries(node)

        return Batch(self.metadata, self.environment) \
                .create_job(
                        step_name=node.name,
                        step_cli=self._step_cli(node,
                                                input_paths,
                                                self.code_package_url,
                                                user_code_retries),
                        code_package_sha=self.code_package.sha,
                        code_package_url=self.code_package_url,
                        code_package_ds=self.datastore.TYPE,
                        image=resources['image'],
                        queue=resources['queue'],
                        iam_role=resources['iam_role'],
                        cpu=resources['cpu'],
                        gpu=resources['gpu'],
                        memory=resources['memory'],
                        run_time_limit=batch_deco.run_time_limit,
                        env=env,
                        attrs=attrs
                ) \
                .attempts(total_retries + 1)

    def _get_retries(self, node):
        max_user_code_retries = 0
        max_error_retries = 0
        # Different decorators may have different retrying strategies, so take
        # the max of them.
        for deco in node.decorators:
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries,
                                        user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        return max_user_code_retries,\
            max_user_code_retries + max_error_retries

    def _step_cli(self, 
                  node,
                  paths, 
                  code_package_url, 
                  user_code_retries):
        cmds = []

        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)

        if R.use_r():
            entrypoint = [R.entrypoint()]
        else:
            entrypoint = [executable, script_name]

        # Use AWS Batch job identifier as the globally unique task identifier.
        task_id = '${AWS_BATCH_JOB_ID}'

        if node.name == 'start':
            # We need a separate unique ID for the special _parameters task
            task_id_params = '%s-params' % task_id
            # Export user-defined parameters into runtime environment
            param_file = ''.join(random.choice(string.ascii_lowercase) 
                for _ in range(10))
            export_params = \
                'python -m ' \
                'metaflow.plugins.aws.step_functions.set_batch_environment ' \
                'parameters %s && . `pwd`/%s' % (param_file, param_file)
            params = entrypoint +\
                ['--quiet',
                 '--metadata=%s' % self.metadata.TYPE,
                 '--environment=%s' % self.environment.TYPE,
                 '--datastore=s3',
                 '--event-logger=%s' % self.event_logger.logger_type,
                 '--monitor=%s' % self.monitor.monitor_type,
                 '--no-pylint',
                 'init',
                 '--run-id sfn-$METAFLOW_RUN_ID',
                 '--task-id %s' % task_id_params]

            # If the start step gets retried, we must be careful not to 
            # regenerate multiple parameters tasks. Hence we check first if 
            # _parameters exists already.
            exists = entrypoint +\
                ['dump',
                 '--max-value-size=0',
                 'sfn-${METAFLOW_RUN_ID}/_parameters/%s' % (task_id_params)]
            cmd = 'if ! %s >/dev/null 2>/dev/null; then %s && %s; fi'\
                  % (' '.join(exists), export_params, ' '.join(params))
            cmds.append(cmd)
            paths = 'sfn-${METAFLOW_RUN_ID}/_parameters/%s' % (task_id_params)
        
        if node.type == 'join' and\
            self.graph[node.split_parents[-1]].type == 'foreach':
            parent_tasks_file = ''.join(random.choice(string.ascii_lowercase) 
                for _ in range(10))
            export_parent_tasks = \
                'python -m ' \
                'metaflow.plugins.aws.step_functions.set_batch_environment ' \
                'parent_tasks %s && . `pwd`/%s' \
                    % (parent_tasks_file, parent_tasks_file)
            cmds.append(export_parent_tasks)

        top_level = [
            '--quiet',
            '--metadata=%s' % self.metadata.TYPE,
            '--environment=%s' % self.environment.TYPE,
            '--datastore=%s' % self.datastore.TYPE,
            '--datastore-root=%s' % self.datastore.datastore_root,
            '--event-logger=%s' % self.event_logger.logger_type,
            '--monitor=%s' % self.monitor.monitor_type,
            '--no-pylint',
            '--with=step_functions_internal'
        ]

        step = [
            'step',
            node.name,
            '--run-id sfn-$METAFLOW_RUN_ID',
            '--task-id %s' % task_id,
            # Since retries are handled by AWS Batch, we can rely on
            # AWS_BATCH_JOB_ATTEMPT as the job counter.
            '--retry-count $((AWS_BATCH_JOB_ATTEMPT-1))',
            '--max-user-code-retries %d' % user_code_retries,
            '--input-paths %s' % paths,
            # Set decorator to batch to execute `task_*` hooks for batch
            # decorator.
            '--with=batch'
        ]
        if any(self.graph[n].type == 'foreach' for n in node.in_funcs):
            # We set the `METAFLOW_SPLIT_INDEX` through JSONPath-foo
            # to pass the state from the parent DynamoDb state for for-each.
            step.append('--split-index $METAFLOW_SPLIT_INDEX')
        if self.tags:
            step.extend('--tag %s' % tag for tag in self.tags)
        if self.namespace:
            step.append('--namespace %s' % self.namespace)
        cmds.append(' '.join(entrypoint + top_level + step))
        return ' && '.join(cmds)


class Workflow(object):
    def __init__(self, name):
        self.name = name
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def start_at(self, start_at):
        self.payload['StartAt'] = start_at
        return self

    def add_state(self, state):
        self.payload['States'][state.name] = state.payload
        return self

    def timeout_seconds(self, timeout_seconds):
        self.payload['TimeoutSeconds'] = timeout_seconds
        return self

    def to_json(self, pretty=False):
        return json.dumps(self.payload, indent=4 if pretty else None)


class State(object):

    def __init__(self, name):
        self.name = name
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload['Type'] = 'Task'

    def resource(self, resource):
        self.payload['Resource'] = resource
        return self

    def next(self, state):
        self.payload['Next'] = state
        return self
        
    def end(self):
        self.payload['End'] = True
        return self

    def parameter(self, name, value):
        self.payload['Parameters'][name] = value
        return self

    def output_path(self, output_path):
        self.payload['OutputPath'] = output_path
        return self

    def result_path(self, result_path):
        self.payload['ResultPath'] = result_path
        return self

    def batch(self, job):
        self.resource('arn:aws:states:::batch:submitJob.sync') \
            .parameter('JobDefinition', job.payload['jobDefinition']) \
            .parameter('JobName', job.payload['jobName']) \
            .parameter('JobQueue', job.payload['jobQueue']) \
            .parameter('Parameters', job.payload['parameters']) \
            .parameter('ContainerOverrides', 
                to_pascalcase(job.payload['containerOverrides'])) \
            .parameter('RetryStrategy', 
                to_pascalcase(job.payload['retryStrategy'])) \
            .parameter('Timeout', 
                to_pascalcase(job.payload['timeout']))
        return self

    def dynamo_db(self, table_name, primary_key, values):
        self.resource('arn:aws:states:::dynamodb:getItem') \
            .parameter('TableName', table_name) \
            .parameter('Key', {
                "pathspec": {
                    "S.$": primary_key
                }
            }) \
            .parameter('ConsistentRead', True) \
            .parameter('ProjectionExpression', values)
        return self  


class Parallel(object):

    def __init__(self, name):
        self.name = name
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload['Type'] = 'Parallel'

    def branch(self, workflow):
        if 'Branches' not in self.payload:
            self.payload['Branches'] = []
        self.payload['Branches'].append(workflow.payload)
        return self

    def next(self, state):
        self.payload['Next'] = state
        return self

    def output_path(self, output_path):
        self.payload['OutputPath'] = output_path
        return self

    def result_path(self, result_path):
        self.payload['ResultPath'] = result_path
        return self   


class Map(object):

    def __init__(self, name):
        self.name = name
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload['Type'] = 'Map'
        self.payload['MaxConcurrency'] = 0

    def iterator(self, workflow):
        self.payload['Iterator'] = workflow.payload
        return self

    def next(self, state):
        self.payload['Next'] = state
        return self

    def items_path(self, items_path):
        self.payload['ItemsPath'] = items_path
        return self

    def parameter(self, name, value):
        self.payload['Parameters'][name] = value
        return self

    def max_concurrency(self, max_concurrency):
        self.payload['MaxConcurrency'] = max_concurrency
        return self

    def output_path(self, output_path):
        self.payload['OutputPath'] = output_path
        return self

    def result_path(self, result_path):
        self.payload['ResultPath'] = result_path
        return self