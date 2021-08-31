from metaflow_test import MetaflowTest, ExpectationFailed, steps



class TaskExceptionTest(MetaflowTest):
    """
    Test that an artifact defined in the first step
    is available in all steps downstream.
    """
    PRIORITY = 1
    SHOULD_FAIL = True

    @steps(0, ['singleton-end'],required=True)
    def step_start(self):
        raise KeyError('Something has went wrong')

    @steps(2, ['all'])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is not None:
            for task in run['end']:
                assert_equals('KeyError' in str(task.exception), True)
                assert_equals(task.exception.exception,"'Something has went wrong'")
