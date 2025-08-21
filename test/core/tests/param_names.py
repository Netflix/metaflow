from metaflow_test import MetaflowTest, steps


class ParameterNameTest(MetaflowTest):
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
    PARAMETERS = {"foo": {"default": 1}}

    @steps(0, ["all"])
    def step_all(self):
        from metaflow import current

        assert_equals(len(current.parameter_names), 1)
        assert_equals(current.parameter_names[0], "foo")
