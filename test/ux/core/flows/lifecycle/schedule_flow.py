"""Flow with @schedule decorator for testing deploy/undeploy lifecycle."""

from metaflow import FlowSpec, project, schedule, step


@schedule(cron="0 0 * * *")
@project(name="schedule_flow")
class ScheduleFlow(FlowSpec):
    @step
    def start(self):
        self.message = "scheduled flow ran"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ScheduleFlow()
