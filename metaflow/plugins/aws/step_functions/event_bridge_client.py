import json

from metaflow.metaflow_config import get_authenticated_boto3_client


class EventBridgeClient(object):

    def __init__(self, name):
        self._client = get_authenticated_boto3_client('events')
        self.name = name

    def cron(self, cron):
        self.cron = cron
        return self

    def role_arn(self, role_arn):
        self.role_arn = role_arn
        return self

    def state_machine_arn(self, state_machine_arn):
        self.state_machine_arn = state_machine_arn
        return self

    def schedule(self):
        if not self.cron:
            # reset the schedule
            self._disable()
        else:
            self._set()

    def _disable(self):
        try:
            self._client.disable_rule(
                Name=self.name
            )
        except self._client.exceptions.ResourceNotFoundException:
            pass

    def _set(self):
        # Generate a new rule or update existing rule.
        self._client.put_rule(
            Name=self.name,
            ScheduleExpression='cron(%s)' % self.cron,
            Description='Metaflow generated rule for %s' % self.name,
            State='ENABLED'
        )
        # Assign AWS Step Functions ARN to the rule as a target.
        self._client.put_targets(
            Rule=self.name,
            Targets=[
                {
                    'Id':self.name,
                    'Arn':self.state_machine_arn,
                    # Set input parameters to empty.
                    'Input':json.dumps({'Parameters':json.dumps({})}),
                    'RoleArn':self.role_arn
                }
            ]
        )