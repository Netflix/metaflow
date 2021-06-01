from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class NestedUnboundedForeachTest(MetaflowTest):
    PRIORITY = 1

    @steps(0, ['foreach-nested-split'], required=True)
    def split_z(self):
        from metaflow.plugins import InternalUnboundedForeachInput
        self.z = InternalUnboundedForeachInput(self.z)

    @tag('unbounded_foreach_internal')
    @steps(0, ['foreach-nested-inner'], required=True)
    def inner(self):
        [x, y, z] = self.foreach_stack()

        # assert that lengths are correct
        assert_equals(len(self.x), x[1])
        assert_equals(len(self.y), y[1])
        # Note: We can't assert the actual num_splits for unbounded-foreach.
        assert_equals(None, z[1]) # expected=len(self.z) for bounded.

        # assert that variables are correct given their indices
        assert_equals(x[2], self.x[x[0]])
        assert_equals(y[2], self.y[y[0]])
        assert_equals(z[2], self.z[z[0]])

        assert_equals(self.input, z[2])
        self.combo = x[2] + y[2] + z[2]

    @steps(1, ['all'])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        from itertools import product

        run = checker.get_run()
        foreach_inner_tasks = {t.pathspec for t in run['foreach_inner'].tasks()}
        assert_equals(36, len(foreach_inner_tasks))
        assert_equals(6, len(list(run['foreach_inner'].control_tasks())))

        artifacts = checker.artifact_dict('foreach_inner', 'combo')
        # Explicitly only consider UBF tasks since the CLIChecker isn't aware of them.
        import os
        got = sorted(val['combo'] for task, val in artifacts.items()
                                  if os.path.join(flow.name, task) in foreach_inner_tasks)
        expected = sorted(''.join(p) for p in product('abc', 'de', 'fghijk'))
        assert_equals(expected, got)