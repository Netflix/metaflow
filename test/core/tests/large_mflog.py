from metaflow_test import MetaflowTest, ExpectationFailed, steps

class LargeMflogTest(MetaflowTest):
    """
    Test that we can capture a large amount of log messages with
    accurate timings
    """
    PRIORITY = 2
    HEADER = """
NUM_FOREACH = 32
NUM_LINES = 5000
"""
    @steps(0, ['foreach-split-small'], required=True)
    def split(self):
        self.arr = range(NUM_FOREACH)

    @steps(0, ['foreach-inner-small'], required=True)
    def inner(self):
        ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f"
        from datetime import datetime
        from metaflow import current
        import sys
        self.log_step = current.step_name
        task_id = current.task_id
        for i in range(NUM_LINES):
            now = datetime.utcnow().strftime(ISOFORMAT)
            print('%s stdout %d %s' % (task_id, i, now))
            sys.stderr.write('%s stderr %d %s\n' % (task_id, i, now))

    @steps(0, ['foreach-join-small'], required=True)
    def join(self, inputs):
        self.log_step = inputs[0].log_step

    @steps(1, ['all'])
    def step_all(self):
        pass

    @steps(0, ['end'])
    def step_end(self):
        self.num_foreach = NUM_FOREACH
        self.num_lines = NUM_LINES

    def check_results(self, flow, checker):
        from itertools import groupby
        from datetime import datetime

        ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f"

        _val = lambda n: list(checker.artifact_dict('end', n).values())[0][n]

        step_name = _val('log_step')
        num_foreach = _val('num_foreach')
        num_lines = _val('num_lines')
        run = checker.get_run()

        for stream in ('stdout', 'stderr'):
            log = checker.get_log(step_name, stream)

            # ignore event_logger noise
            lines = [line.split() for line in log.splitlines()
                     if not line.startswith('event_logger:')]

            assert_equals(len(lines), num_foreach * num_lines)

            for task_id, task_lines_iter in groupby(lines, lambda x: x[0]):
                task_lines = list(task_lines_iter)
                assert_equals(len(task_lines), num_lines)

                for i, (_, stream_type, idx, tstamp) in enumerate(task_lines):
                    # test that loglines originate from the correct stream
                    # and they are properly ordered
                    assert_equals(stream_type, stream)
                    assert_equals(int(idx), i)

            if run is not None:
                for task in run[step_name]:
                    # test task.loglines
                    task_lines = [(tstamp, msg)
                                  for tstamp, msg in task.loglines(stream)
                                  if not msg.startswith('event_logger:')]
                    assert_equals(len(task_lines), num_lines)
                    for i, (mf_tstamp, msg) in enumerate(task_lines):
                        task_id, stream_type, idx, tstamp_str = msg.split()

                        assert_equals(task_id, task.id)
                        assert_equals(stream_type, stream)
                        assert_equals(int(idx), i)

                        tstamp = datetime.strptime(tstamp_str, ISOFORMAT)
                        delta = mf_tstamp - tstamp
                        #TODO challenge: optimize local runtime so that
                        # delta.seconds can be made smaller, e.g. 5 secs
                        # enable this line to see a distribution of deltas:
                        # print("DELTA", delta.seconds)
                        if delta.days > 0 or delta.seconds > 60:
                            raise Exception("Time delta too high. "\
                                            "Mflog %s, user %s"\
                                            % (mf_tstamp, tstamp))