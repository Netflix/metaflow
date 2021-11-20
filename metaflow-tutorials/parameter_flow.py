from metaflow import FlowSpec, Parameter, step
class ParameterFlow(FlowSpec):
    alpha = Parameter('with',
                      help='Learning rate',
                      default=0.01)
    @step
    def start(self):
        print('alpha is %f' % self.alpha)
        self.next(self.end)
    @step
    def end(self):
        print('alpha is still %f' % self.alpha)
if __name__ == '__main__':
    ParameterFlow()