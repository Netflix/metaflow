from metaflow.metaflow_config import get_authenticated_boto3_client, \
    AWS_SANDBOX_ENABLED, AWS_SANDBOX_REGION


class StepFunctionsClient(object):

    def __init__(self):
        self._client = get_authenticated_boto3_client('stepfunctions')

    def search(self, name):
        paginator = self._client.get_paginator('list_state_machines')
        return next((
            state_machine
            for page in paginator.paginate()
            for state_machine in page['stateMachines']
            if state_machine['name'] == name
        ), None)

    def push(self, name, definition, roleArn):
        try:
            response = self._client.create_state_machine(
                name = name,
                definition = definition,
                roleArn = roleArn
            )
            state_machine_arn = response['stateMachineArn']
        except self._client.exceptions.StateMachineAlreadyExists as e:
            # State Machine already exists, update it instead of creating it.
            state_machine_arn = e.response['Error']['Message'].split("'")[1]
            self._client.update_state_machine(
                stateMachineArn = state_machine_arn,
                definition = definition,
                roleArn = roleArn
            )
        return state_machine_arn

    def get(self, name):
        state_machine_arn = self.get_state_machine_arn(name)
        if state_machine_arn is None:
            return None
        try:
            return self._client.describe_state_machine(
                stateMachineArn = state_machine_arn,
            )
        except self._client.exceptions.StateMachineDoesNotExist:
            return None

    def trigger(self, state_machine_arn, input):
        return self._client.start_execution(
            stateMachineArn = state_machine_arn,
            input = input
        )

    def list_executions(self, state_machine_arn, states):
        if len(states) > 0:
            return (
                execution
                for state in states
                for page in self._client.get_paginator('list_executions')
                                .paginate(
                                    stateMachineArn=state_machine_arn, 
                                    statusFilter=state
                                )
                for execution in page['executions']
            )
        return (
            execution
            for page in self._client.get_paginator('list_executions')
                            .paginate(stateMachineArn=state_machine_arn)
            for execution in page['executions']
        )

    def terminate_execution(self, state_machine_arn, execution_arn):
        #TODO
        pass

    def get_state_machine_arn(self, name):
        if AWS_SANDBOX_ENABLED:
            # We can't execute list_state_machines within the sandbox,
            # but we can construct the statemachine arn since we have
            # explicit access to the region.
            account_id = get_authenticated_boto3_client('sts') \
                                .get_caller_identity().get('Account')
            region = AWS_SANDBOX_REGION
            return 'arn:aws:states:%s:%s:stateMachine:%s' \
                                        % (region, account_id, name)
        else:
            state_machine = self.search(name)
            if state_machine:
                return state_machine['stateMachineArn']
            return None