from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class NestedUnboundedForeachTest(MetaflowTest):
    PRIORITY = 1

    @steps(0, ['foreach-nested-split'], required=True)
    def split_z(self):
        from metaflow.plugins.test_unbounded_foreach import TestUnboundedForeachInput
        self.z = TestUnboundedForeachInput(self.z)

    @tag('test_unbounded_foreach')
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

        artifacts = checker.artifact_dict('foreach_inner', 'combo')
        got = sorted(val['combo'] for val in artifacts.values())
        expected = sorted(''.join(p) for p in product('abc', 'de', 'fghijk'))
        assert_equals(expected, got)

        run = checker.get_run()
        assert_equals(36, len(list(run['foreach_inner'].tasks())))
        assert_equals(6, len(list(run['foreach_inner'].control_tasks())))
