from metaflow_test import MetaflowTest, ExpectationFailed, steps

class DetectSegFaultTest(MetaflowTest):
    """
    Test that segmentation faults produce a message in the logs
    """
    PRIORITY = 2
    SHOULD_FAIL = True

    @steps(0, ['singleton-end'], required=True)
    def step_end(self):
        # cause a segfault
        import ctypes
        libc = ctypes.cdll.LoadLibrary('libc.so.6')
        print("Crash and burn!")
        libc.free(123)

    @steps(1, ['all'])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        # CLI logs requires the exact task ID for failed tasks which
        # we don't have here. Let's rely on the MLI checker only.
        run = checker.get_run()
        if run:
            # loglines prior to the segfault should be persisted
            checker.assert_log('end',
                               'stdout',
                               'Crash and burn!',
                               exact_match=False)
            # a message should be printed that mentions "segmentation fault"
            checker.assert_log('end',
                               'stderr',
                               'segmentation fault',
                               exact_match=False)
