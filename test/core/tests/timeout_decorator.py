from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class TimeoutDecoratorTest(MetaflowTest):
    """
    Test that checks that the timeout decorator works as intended.
    """

    PRIORITY = 2

    @tag('catch(var="ex", print_exception=False)')
    @tag("timeout(seconds=1)")
    @steps(0, ["singleton-start", "foreach-inner"], required=True)
    def step_sleep(self):
        self.check = True
        import time

        time.sleep(5)

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run:
            timeout_raised = False
            for step in run:
                for task in step:
                    if "check" in task.data:
                        extype = (
                            "metaflow.plugins.timeout_decorator." "TimeoutException"
                        )
                        assert_equals(extype, str(task.data.ex.type))
                        timeout_raised = True
            assert_equals(True, timeout_raised)
