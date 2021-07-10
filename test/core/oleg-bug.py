from metaflow import FlowSpec, step, Parameter, project, IncludeFile, JSONType, current
from metaflow_test import assert_equals, assert_exception, ExpectationFailed, is_resumed, ResumeFromHere, TestRetry
from metaflow import catch,retry,retry,retry,retry,catch

class TagCatchTestFlow(FlowSpec):
    @retry(times=1,minutes_between_retries=0)
    @step
    def start(self):
        import sys
        self.test_attempt = current.retry_count
        sys.stdout.write('stdout testing logs %d\n' % self.test_attempt)
        sys.stderr.write('stderr testing logs %d\n' % self.test_attempt)
        #if self.test_attempt < 1:
        #    self.invisible = True
        #    raise TestRetry()
        self.next(self.a)
    #@retry(times=1,minutes_between_retries=0)
    @catch(var="ex", print_exception=False)
    @step
    def a(self):
        import signal, os
        # die an ugly death
        os.kill(os.getpid(), signal.SIGKILL)
        self.next(self.end)
    @catch(var="end_ex", print_exception=False)
    @step
    def end(self):
        from metaflow.exception import ExternalCommandFailed
        # make sure we see the latest attempt version of the artifact
        assert_equals(3, self.test_attempt)
        # the test uses a non-trivial derived exception on purpose
        # which is non-trivial to pickle correctly
        self.here = True
        raise ExternalCommandFailed('catch me!')
if __name__ == '__main__':
    TagCatchTestFlow()
