from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class ProjectBranchTest(MetaflowTest):
    PRIORITY = 1

    HEADER = """
import os

os.environ['METAFLOW_BRANCH'] = 'this_is_a_test_branch'
@project(name='project_branch')
"""

    @steps(0, ["singleton"], required=True)
    def step_single(self):
        pass

    @steps(1, ["all"])
    def step_all(self):
        from metaflow import current

        assert_equals(current.branch_name, "test.this_is_a_test_branch")
        assert_equals(
            current.project_flow_name,
            "project_branch.test.this_is_a_test_branch.ProjectBranchTestFlow",
        )
