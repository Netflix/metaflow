from metaflow.decorators import FlowDecorator


class ScheduleDecorator(FlowDecorator):
    name = 'schedule'
    defaults = {'cron': None,
                'weekly': False,
                'daily': True,
                'hourly': False}

    def flow_init(self, flow, graph,  environment, datastore, logger):
        # Currently supports quartz cron expressions in UTC as defined in
        # https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduled-events.html#cron-expressions
        if self.attributes['cron']:
            self.schedule = self.attributes['cron']
        elif self.attributes['weekly']:
            self.schedule = '0 0 ? * SUN *'
        elif self.attributes['hourly']:
            self.schedule = '0 * * * ? *'
        elif self.attributes['daily']:
            self.schedule = '0 0 * * ? *'
        else:
            self.schedule = None