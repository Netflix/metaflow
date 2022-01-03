from metaflow_test import MetaflowTest, ExpectationFailed, steps


class FlowOptionsTest(MetaflowTest):
    """
    Test that the metaflow_extensions module is properly loaded
    """

    PRIORITY = 0
    HEADER = """
import os
from metaflow import test_flow_decorator

os.environ['METAFLOW_FOOBAR'] = 'this_is_foobar'
@test_flow_decorator
"""

    @steps(0, ["all"])
    def step_all(self):
        from metaflow import current

        assert_equals(current.foobar_value, "this_is_foobar")
