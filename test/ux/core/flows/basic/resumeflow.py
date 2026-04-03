import os

from metaflow import FlowSpec, step, project, Parameter


@project(name="resume_test")
class ResumeFlow(FlowSpec):
    """A flow with a configurable failure for testing resume behavior.

    When should_fail is True and no resume is active, the 'process' step
    raises an exception. When resumed, the process step should either be
    cloned (if it succeeded in the origin run) or re-executed.
    """

    should_fail = Parameter(
        "should_fail",
        help="If true, the process step will fail",
        default=False,
        type=bool,
    )

    @step
    def start(self):
        self.start_value = "started"
        self.next(self.process)

    @step
    def process(self):
        if self.should_fail:
            raise RuntimeError("Intentional failure for resume testing")
        self.process_value = "processed"
        self.next(self.end)

    @step
    def end(self):
        self.end_value = "done"


if __name__ == "__main__":
    ResumeFlow()
