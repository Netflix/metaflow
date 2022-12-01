from metaflow_test import MetaflowTest, ExpectationFailed, steps


class LargeArtifactTest(MetaflowTest):
    """
    Test that you can serialize large objects (over 4GB)
    with Python3 - although on OSX, some versions of Python3 fail
    to serialize objects over 2GB - https://bugs.python.org/issue24658
    so YMMV.
    """

    PRIORITY = 2

    @steps(0, ["singleton"], required=True)
    def step_single(self):
        import sys

        if sys.version_info[0] > 2:
            self.large = b"x" * int(4.1 * 1024**3)
            self.noop = False
        else:
            self.noop = True

    @steps(0, ["end"])
    def step_end(self):
        import sys

        if sys.version_info[0] > 2:
            assert_equals(self.large, b"x" * int(4.1 * 1024**3))

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        import sys

        noop = next(iter(checker.artifact_dict("end", "noop").values()))["noop"]
        if not noop and sys.version_info[0] > 2:
            checker.assert_artifact("end", "large", b"x" * int(4.1 * 1024**3))
