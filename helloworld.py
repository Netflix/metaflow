from metaflow import FlowSpec, step


class HelloFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("HelloFlow is starting.")
        self.next(self.hello)

    @step
    def hello(self):
        """
        A step for metaflow to introduce itself.

        """
        import time

        print("Metaflow says: Hi!")
        self.aa = "a" + str(time.time())
        self.bb = "b" + str(time.time())
        self.cc = "c" + str(time.time())
        self.dd = "d" + str(time.time())
        self.ee = "e" + str(time.time())
        self.ff = "f" + str(time.time())
        self.gg = "g" + str(time.time())
        self.hh = "h" + str(time.time())
        self.ii = "i" + str(time.time())
        self.jj = "j" + str(time.time())
        self.kk = "k" + str(time.time())
        self.ll = "l" + str(time.time())
        self.mm = "m" + str(time.time())
        self.nn = "n" + str(time.time())
        self.oo = "o" + str(time.time())
        self.pp = "p" + str(time.time())
        self.qq = "q" + str(time.time())
        self.rr = "r" + str(time.time())
        self.ss = "s" + str(time.time())
        self.tt = "t" + str(time.time())
        self.uu = "u" + str(time.time())
        self.vv = "v" + str(time.time())
        self.ww = "w" + str(time.time())
        self.xx = "x" + str(time.time())
        self.yy = "y" + str(time.time())
        self.zz = "z" + str(time.time())
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done.")


if __name__ == "__main__":
    HelloFlow()
