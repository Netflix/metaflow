import time

from metaflow import FlowSpec, Step, current, step


class ForeachLinearForeach(FlowSpec):
    """
    foreach -> linear -> linear -> foreach -> linear -> linear -> join -> join
    """

    log_testing_msg = "Testing logging. Data: {data}"

    @step
    def start(self):
        self.x = "ab"
        self.next(self.linear_1, foreach="x")

    @step
    def linear_1(self):
        # AIP-6717 sleeps to avoid Datadog OOM events because of too many pods
        # being created and decommissioned in a short time frame.
        time.sleep(1)
        self.next(self.linear_2)

    @step
    def linear_2(self):
        time.sleep(1)
        print(self.log_testing_msg.format(data=self.input))
        self.next(self.foreach_split_z)

    @step
    def foreach_split_z(self):
        time.sleep(1)
        self.z = "ef"
        self.next(self.linear_3, foreach="z")

    @step
    def linear_3(self):
        time.sleep(1)
        print(self.log_testing_msg.format(data=self.input))
        self.next(self.linear_4)

    @step
    def linear_4(self):
        time.sleep(1)
        self.next(self.foreach_join_z)

    @step
    def foreach_join_z(self, inputs):
        time.sleep(1)
        self.merge_artifacts(inputs)
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        time.sleep(1)
        self.merge_artifacts(inputs)
        self.next(self.end)

    @step
    def end(self):
        def test_foreach_logging(
            test_name: str, step_name: str, loop_arg, expected_task_count: int
        ):
            print(f"Testing logging for {test_name}")
            logging_step: Step = Step(
                f"{current.flow_name}/{current.run_id}/{step_name}"
            )
            logs = []
            for task in logging_step.tasks():
                actual_log = task.stdout
                print(f"=== Start: stdout from step index {task.index}===")
                print(actual_log)
                print(f"=== End: stdout from step index {task.index}===")
                expected_log = self.log_testing_msg.format(data=loop_arg[task.index])
                assert expected_log in actual_log, f'Expected log: "{expected_log}"'
                logs.append(actual_log)
            assert (
                len(logs) == expected_task_count
            ), f"Expected {expected_task_count} log(s)"
            print(f"\n***Logging for {test_name} works as expected***\n")

        test_foreach_logging(
            test_name="foreach",
            step_name="linear_2",
            loop_arg=self.x,
            expected_task_count=2,
        )

        test_foreach_logging(
            test_name="nested foreach",
            step_name="linear_3",
            loop_arg=self.z,
            expected_task_count=4,
        )


if __name__ == "__main__":
    ForeachLinearForeach()
