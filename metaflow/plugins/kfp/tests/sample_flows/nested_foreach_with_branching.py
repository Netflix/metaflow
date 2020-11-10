from metaflow import FlowSpec, step


def truncate(var):
    var = str(var)
    if len(var) > 500:
        var = "%s..." % var[:500]
    return var


class ExpectationFailed(Exception):
    def __init__(self, expected, got):
        super(ExpectationFailed, self).__init__(
            "Expected result: %s, got %s" % (truncate(expected), truncate(got))
        )


def assert_equals(expected, got):
    if expected != got:
        raise ExpectationFailed(expected, got)


class NestedForeachWithBranching(FlowSpec):
    """
    split -> foreach -> foreach -> foreach -> linear -> linear -> join -> join -> join -> join (with below split)
          -> split -> join  (with above split)
    """

    @step
    def start(self):
        self.next(self.foreach_split_x, self.split_w)

    @step
    def foreach_split_x(self):
        self.x = "ab"
        self.next(self.foreach_split_y, foreach="x")

    @step
    def split_w(self):
        self.var1 = 100
        self.next(self.w1, self.w2)

    @step
    def w1(self):
        self.var1 = 150
        self.next(self.join_w)

    @step
    def w2(self):
        self.var1 = 250
        self.next(self.join_w)

    @step
    def join_w(self, w_inp):
        self.var1 = w_inp.w1.var1
        assert self.var1 == 150
        self.next(self.foreach_join_w_x)

    @step
    def foreach_split_y(self):
        self.y = "cd"
        self.next(self.foreach_split_z, foreach="y")

    @step
    def foreach_split_z(self):
        self.z = "ef"
        self.next(self.foreach_inner, foreach="z")

    @step
    def foreach_inner(self):
        [x, y, z] = self.foreach_stack()

        # assert that lengths are correct
        assert_equals(len(self.x), x[1])
        assert_equals(len(self.y), y[1])
        assert_equals(len(self.z), z[1])

        # assert that variables are correct given their indices
        assert_equals(x[2], self.x[x[0]])
        assert_equals(y[2], self.y[y[0]])
        assert_equals(z[2], self.z[z[0]])

        self.combo = x[2] + y[2] + z[2]
        self.next(self.foreach_inner_2)

    @step
    def foreach_inner_2(self):
        assert self.input in "ef"
        self.next(self.foreach_join_z)

    @step
    def foreach_join_z(self, inputs):
        self.next(self.foreach_join_y)

    @step
    def foreach_join_y(self, inputs):
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        self.next(self.foreach_join_w_x)

    @step
    def foreach_join_w_x(self, input):
        assert input.join_w.var1 == 150
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    NestedForeachWithBranching()
