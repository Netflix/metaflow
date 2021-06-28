from metaflow import FlowSpec, step


class OldBranchingFlow(FlowSpec):
    """Example flow demonstrating a simple split+join pattern"""

    @step
    def start(self):
        self.next(self.one)

    @step
    def one(self):
        self.n = 11
        self.next(self.aaa, self.bbb)

    @step
    def aaa(self):
        self._aaa = "A" * self.n
        self.next(self.join)

    @step
    def bbb(self):
        self._bbb = "B" * self.n
        self.next(self.join)

    @step
    def join(self, inputs):
        assert (inputs.aaa._aaa, inputs.bbb._bbb) == ("AAAAAAAAAAA", "BBBBBBBBBBB")
        self.a, self.b = (inputs.aaa._aaa, inputs.bbb._bbb)
        assert not hasattr(self, "n")
        [self.n] = {inputs.aaa.n, inputs.bbb.n}
        self.done = True
        self.next(self.end)

    @step
    def end(self):
        pass
