from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class ProjectProductionTest(MetaflowTest):
    PRIORITY = 1
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
    ]
    HEADER = """
import os

os.environ['METAFLOW_PRODUCTION'] = 'True'
@project(name='project_prod')
"""

    @steps(0, ["singleton"], required=True)
    def step_single(self):
        pass

    @steps(1, ["all"])
    def step_all(self):
        from metaflow import current

        assert_equals(current.branch_name, "prod")
        assert_equals(
            current.project_flow_name, "project_prod.prod.ProjectProductionTestFlow"
        )
