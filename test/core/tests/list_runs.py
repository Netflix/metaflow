# -*- coding: utf-8 -*-
from metaflow_test import MetaflowTest, assert_equals, steps


class ListRunsTest(MetaflowTest):
    """
    Test that tags are assigned properly.
    """

    PRIORITY = 1

    @steps(1, ["all"])
    def step_all(self):
        self.flow_run_id = current.run_id

    def check_results(self, flow, checker):
        if type(checker).__name__ == "CliCheck":
            from metaflow import Flow

            cli_run_list = checker.list_runs()
            assert cli_run_list, "No Runs Returned in CLI for {name}".format(
                name=flow.name
            )

            latest_run_id = Flow(flow.name).latest_run.data.flow_run_id
            latest_run_found = False
            for run in cli_run_list:
                assert_equals(run["name"], flow.name)
                if run["id"] == latest_run_id:
                    latest_run_found = True

            assert (
                latest_run_found
            ), "Latest run {id} for {name} not listed in cli.".format(
                id=latest_run_id, name=flow.name
            )
