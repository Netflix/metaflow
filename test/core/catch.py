from metaflow import FlowSpec, catch, batch,retry, step

class CatchFlow(FlowSpec):

    @step
    def start(self):
        self.params = range(1)
        self.next(self.compute, foreach='params')

    @retry(times=1,minutes_between_retries=0)
    @catch(var='compute_failed')
    @batch
    @step
    def compute(self):
        self.x = 'foo'
        print('hello')
        import signal, os
        # die an ugly death
        os.kill(os.getpid(), signal.SIGKILL)
        self.div = self.input
        self.x = 5 / self.div
        self.next(self.join)

    @step
    def join(self, inputs):
        for input in inputs:
            if input.compute_failed:
                print('compute failed for parameter')
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    CatchFlow()
