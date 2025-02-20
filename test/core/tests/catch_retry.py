from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag
from metaflow import current


class CatchRetryTest(MetaflowTest):
    PRIORITY = 2

    @tag("retry(times=3,minutes_between_retries=0)")
    @steps(0, ["start"])
    def step_start(self):
        import os
        import sys

        self.test_attempt = current.retry_count
        sys.stdout.write("stdout testing logs %d\n" % self.test_attempt)
        sys.stderr.write("stderr testing logs %d\n" % self.test_attempt)
        if self.test_attempt < 3:
            self.invisible = True
            raise TestRetry()

    # foreach splits don't support @catch but @retry should work
    @tag("retry(times=2,minutes_between_retries=0)")
    @steps(0, ["foreach-split", "parallel-split"])
    def step_split(self):
        import os

        if current.retry_count == 2:
            self.this_is_split = True
        else:
            raise TestRetry()

    @tag("retry(times=2,minutes_between_retries=0)")
    @steps(0, ["join"])
    def step_join(self):
        import os

        if current.retry_count == 2:
            self.test_attempt = inputs[0].test_attempt
        else:
            raise TestRetry()

    @tag('catch(var="end_ex", print_exception=False)')
    @steps(0, ["end"], required=True)
    def step_end(self):
        from metaflow.exception import ExternalCommandFailed

        # make sure we see the latest attempt version of the artifact
        assert_equals(3, self.test_attempt)
        # the test uses a non-trivial derived exception on purpose
        # which is non-trivial to pickle correctly
        self.here = True
        raise ExternalCommandFailed("catch me!")

    @tag('catch(var="ex", print_exception=False)')
    @tag("retry(times=2,minutes_between_retries=0)")
    @steps(1, ["all"])
    def step_all(self):
        # Die a soft death; this should retry and then catch in the end
        self.retry_with_catch = current.retry_count
        raise TestRetry()

    def check_results(self, flow, checker):

        checker.assert_log(
            "start", "stdout", "stdout testing logs 3\n", exact_match=False
        )
        checker.assert_log(
            "start", "stderr", "stderr testing logs 3\n", exact_match=False
        )

        for step in flow:

            if step.name == "start":
                checker.assert_artifact("start", "test_attempt", 3)
                try:
                    for task in checker.artifact_dict("start", "invisible").values():
                        if task:
                            raise Exception(
                                "'invisible' should not be visible in 'start'"
                            )
                except KeyError:
                    pass
            elif step.name == "end":
                checker.assert_artifact("end", "test_attempt", 3)
                for task in checker.artifact_dict(step.name, "end_ex").values():
                    assert_equals("catch me!", str(task["end_ex"].exception))
                    break
                else:
                    raise Exception("No artifact 'end_ex' in step 'end'")

            elif flow._graph[step.name].type == "foreach":
                checker.assert_artifact(step.name, "this_is_split", True)

            elif flow._graph[step.name].type == "join":
                checker.assert_artifact("end", "test_attempt", 3)

            else:
                for task in checker.artifact_dict(step.name, "ex").values():
                    extype = "metaflow_test.TestRetry"
                    assert_equals(extype, str(task["ex"].type))
                    break
                else:
                    raise Exception("No artifact 'ex' in step '%s'" % step.name)
                for task in checker.artifact_dict(
                    step.name, "retry_with_catch"
                ).values():
                    assert_equals(task["retry_with_catch"], 2)
                    break
                else:
                    raise Exception(
                        "No artifact 'retry_with_catch' in step '%s'" % step.name
                    )

        run = checker.get_run()
        if run:
            for step in run:
                if step.id == "end":
                    continue
                if flow._graph[step.id].type in ("foreach", "join"):
                    # 1 normal run + 2 retries = 3 attempts
                    attempts = 3
                elif step.id == "start":
                    attempts = 4  # 1 normal run + 3 retries = 4 attempts
                else:
                    # 1 normal run + 2 retries = 3 attempts
                    attempts = 3
                for task in step:
                    data = task.data
                    got = sorted(m.value for m in task.metadata if m.type == "attempt")
                    assert_equals(list(map(str, range(attempts))), got)

            assert_equals(False, "invisible" in run["start"].task.data)
            assert_equals(3, run["start"].task.data.test_attempt)
            end = run["end"].task
            assert_equals(True, end.data.here)
            assert_equals(3, end.data.test_attempt)
            # task.exception is None since the exception was handled
            assert_equals(None, end.exception)
            assert_equals("catch me!", end.data.end_ex.exception)
            assert_equals(
                "metaflow.exception.ExternalCommandFailed", end.data.end_ex.type
            )
