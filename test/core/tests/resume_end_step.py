from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ResumeEndStepTest(MetaflowTest):
    """
    Resuming from the end step should work
    """

    RESUME = True
    PRIORITY = 3
    PARAMETERS = {"int_param": {"default": 123}}

    @steps(0, ["start"])
    def step_start(self):
        self.data = "start"

    @steps(0, ["singleton-end"], required=True)
    def step_end(self):
        if is_resumed():
            self.data = "foo"
        else:
            self.data = "bar"
            raise ResumeFromHere()

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        for step in flow:
            if step.name == "end":
                checker.assert_artifact(step.name, "data", "foo")
            else:
                checker.assert_artifact(step.name, "data", "start")
        run = checker.get_run()
        if run is not None:
            # We can also check the metadata for all steps
            common_run_id = None
            exclude_keys = ["origin-task-id", "origin-run-id", "python_version"]
            for step in run:
                for task in step:
                    resumed_metadata = task.metadata_dict
                    step_name = step.path_components[-1]
                    if step_name == "end":
                        if common_run_id is None:
                            common_run_id = resumed_metadata["origin-run-id"]
                        assert_equals(common_run_id, resumed_metadata["origin-run-id"])
                        assert "origin-task-id" not in resumed_metadata, "Invalid clone"
                        continue
                    # Here we check if we have the correct metadata
                    assert all(
                        [k in resumed_metadata for k in exclude_keys]
                    ), "Invalid cloned task"
                    if common_run_id is None:
                        common_run_id = resumed_metadata["origin-run-id"]
                    assert_equals(common_run_id, resumed_metadata["origin-run-id"])
                    orig_metadata = run.parent[resumed_metadata["origin-run-id"]][
                        step_name
                    ][resumed_metadata["origin-task-id"]].metadata_dict
                    # Only resumes once so key not present elsewhere
                    assert_equals_metadata(
                        orig_metadata, resumed_metadata, exclude_keys
                    )
