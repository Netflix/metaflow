from metaflow_test import MetaflowTest, ExpectationFailed, steps

class ResumeStartStepTest(MetaflowTest):
    """
    Resuming from the start step should work
    """
    RESUME = True
    PRIORITY = 3
    PARAMETERS = {
        'int_param': {'default': 123}
    }

    @steps(0, ['singleton-start'], required=True)
    def step_start(self):
        from metaflow import current
        if is_resumed():
            self.data = 'foo'
            # Verify that the `current` singleton contains the correct origin
            # run_id by double checking with the environment variables used
            # for tests.
            self.actual_origin_run_id = current.origin_run_id
            from metaflow_test import origin_run_id_for_resume
            self.expected_origin_run_id = origin_run_id_for_resume()
            assert len(self.expected_origin_run_id) > 0
        else:
            self.data = 'bar'
            raise ResumeFromHere()

    @steps(2, ['all'])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            for step in flow:
                checker.assert_artifact(step.name, 'data', 'foo')
                checker.assert_artifact(step.name, 'int_param', 123)
        else:
            assert_equals(run.data.expected_origin_run_id,
                          run.data.actual_origin_run_id)
