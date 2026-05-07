from metaflow_test import FlowDefinition, steps


class ResumeEndStep(FlowDefinition):
    """
    Resuming from the end step should work
    """

    RESUME = True
    PRIORITY = 3
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]
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
                        assert common_run_id == resumed_metadata["origin-run-id"]
                        assert "origin-task-id" not in resumed_metadata, "Invalid clone"
                        continue
                    # Here we check if we have the correct metadata
                    assert all(
                        [k in resumed_metadata for k in exclude_keys]
                    ), "Invalid cloned task"
                    if common_run_id is None:
                        common_run_id = resumed_metadata["origin-run-id"]
                    assert common_run_id == resumed_metadata["origin-run-id"]
                    orig_metadata = run.parent[resumed_metadata["origin-run-id"]][
                        step_name
                    ][resumed_metadata["origin-task-id"]].metadata_dict
                    # Only resumes once so key not present elsewhere
                    _excl = set(exclude_keys) if exclude_keys else set()
                    _orig_keys = set(orig_metadata) - _excl
                    _res_keys = set(resumed_metadata) - _excl
                    assert (
                        _orig_keys == _res_keys
                    ), "metadata key mismatch: orig=%s resumed=%s" % (
                        sorted(_orig_keys),
                        sorted(_res_keys),
                    )
                    for _k in _orig_keys:
                        assert (
                            orig_metadata[_k] == resumed_metadata[_k]
                        ), "metadata[%s]: expected %r, got %r" % (
                            _k,
                            orig_metadata[_k],
                            resumed_metadata[_k],
                        )
