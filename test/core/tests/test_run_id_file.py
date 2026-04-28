"""--run-id-file should be written before any step runs (resume needs it)."""

import os

from metaflow import FlowSpec, current, step
from metaflow_test import ResumeFromHere, is_resumed


class RunIdFileFlow(FlowSpec):
    @step
    def start(self):
        # The harness passes --run-id-file run-id; check it exists already.
        assert os.path.isfile("run-id"), "run-id file missing before start"
        run_id_from_file = open("run-id").read().strip()
        assert run_id_from_file == current.run_id

        if not is_resumed():
            raise ResumeFromHere()

        self.next(self.end)

    @step
    def end(self):
        pass


def test_run_id_file(metaflow_runner, executor):
    # First run fails inside `start` via ResumeFromHere — but run-id is
    # written before `start` runs and should match current.run_id.
    first = metaflow_runner(RunIdFileFlow, executor=executor)
    assert not first.successful

    resumed = metaflow_runner(
        RunIdFileFlow, executor=executor, resume=True, resume_step="start"
    )
    assert resumed.successful, resumed.stderr
