from metaflow_test import MetaflowTest, ExpectationFailed, steps


class S3FailureTest(MetaflowTest):
    """
    Test that S3 failures are handled correctly.
    """

    PRIORITY = 1

    HEADER = """
import os

os.environ['TEST_S3_RETRY'] = '1'
"""

    @steps(0, ["singleton-start"], required=True)
    def step_start(self):
        # we need a unique artifact for every run which we can reconstruct
        # independently in the start and end tasks
        from metaflow import current

        self.x = "%s/%s" % (current.flow_name, current.run_id)

    @steps(0, ["end"])
    def step_end(self):
        from metaflow import current

        run_id = "%s/%s" % (current.flow_name, current.run_id)
        assert_equals(self.x, run_id)

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run:
            # we should see TEST_S3_RETRY error in the logs
            # when --datastore=s3
            checker.assert_log("start", "stderr", "TEST_S3_RETRY", exact_match=False)
        run_id = "S3FailureTestFlow/%s" % checker.run_id
        checker.assert_artifact("start", "x", run_id)
        checker.assert_artifact("end", "x", run_id)
