from metaflow_test import MetaflowTest, ExpectationFailed, steps


class BasicLogTest(MetaflowTest):
    """
    Test that log messages emitted in the first step
    are saved and readable.
    """

    PRIORITY = 0

    @steps(0, ["singleton"], required=True)
    def step_single(self):
        import sys

        msg1 = "stdout: A regular message.\n"
        msg2 = "stdout: A message with unicode: \u5e74\n"
        sys.stdout.write(msg1)
        if not sys.stdout.encoding:
            sys.stdout.write(msg2.encode("utf8"))
        else:
            sys.stdout.write(msg2)

        msg3 = "stderr: A regular message.\n"
        msg4 = "stderr: A message with unicode: \u5e74\n"
        sys.stderr.write(msg3)
        if not sys.stderr.encoding:
            sys.stderr.write(msg4.encode("utf8"))
        else:
            sys.stderr.write(msg4)

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        msg1 = "stdout: A regular message.\n"
        msg2 = "stdout: A message with unicode: \u5e74\n"
        stdout_combined_msg = "".join([msg1, msg2, ""])

        msg3 = "stderr: A regular message.\n"
        msg4 = "stderr: A message with unicode: \u5e74\n"
        stderr_combined_msg = "".join([msg3, msg4, ""])

        for step in flow:
            if step.name not in ["start", "end"]:
                checker.assert_log(
                    step.name, "stdout", stdout_combined_msg, exact_match=False
                )
                checker.assert_log(
                    step.name, "stderr", stderr_combined_msg, exact_match=False
                )
