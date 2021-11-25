from metaflow.decorators import FlowDecorator
from .event_bridge_client import EventBridgeClient
from .step_functions_client import StepFunctionsClient
class ScheduleDefinition():

    def __init__(self, schedule_obj):
        pass

    def generate_eventbridge_rule(self):
        pass

class CronScheduleDefinition(ScheduleDefinition):

    def __init__(self, schedule_obj):
        self.cron = schedule_obj

    def generate_eventbridge_rule(self, name, state_machine_arn, sfn_access_role):
        return (
            EventBridgeClient(name)
                .cron(self.cron)
                .role_arn(sfn_access_role)
                .state_machine_arn(state_machine_arn)
                .schedule()
        )

class FlowTriggerScheduleDefinition(ScheduleDefinition):

    def __init__(self, schedule_obj):
        self.flow_name = schedule_obj

    def _get_state_machine_arn(self):

        flow = StepFunctionsClient().search(self.flow_name)

        if flow:
            return flow.get('stateMachineArn', None)
        else:
            raise Exception("Dependant State Machine Found")

    def generate_eventbridge_rule(self, name, state_machine_arn, sfn_access_role):

        dependant_state_machine_arn = self._get_state_machine_arn()

        return (
            EventBridgeClient(name)
                .dependent_state_machine_arn(dependant_state_machine_arn)
                .role_arn(sfn_access_role)
                .state_machine_arn(state_machine_arn)
                .schedule()
        )

class ScheduleDecorator(FlowDecorator):
    name = "schedule"
    defaults = {"cron": None, "weekly": False, "daily": True, "hourly": False, "flow_name": None}

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        # Currently supports quartz cron expressions in UTC as defined in
        # https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduled-events.html#cron-expressions
        if self.attributes["flow_name"]:
            self.schedule = FlowTriggerScheduleDefinition(self.attributes['flow_name'])
        elif self.attributes["cron"]:
            self.schedule = CronScheduleDefinition(self.attributes["cron"])
        elif self.attributes["weekly"]:
            self.schedule = CronScheduleDefinition("0 0 ? * SUN *")
        elif self.attributes["hourly"]:
            self.schedule = CronScheduleDefinition("0 * * * ? *")
        elif self.attributes["daily"]:
            self.schedule = CronScheduleDefinition("0 0 * * ? *")
        else:
            print("Returned none")
            self.schedule = CronScheduleDefinition(None)
