from metaflow_test import MetaflowTest, ExpectationFailed, steps


class MergeArtifactsPropagationTest(MetaflowTest):
    # This test simply tests whether things set on a single branch will
    # still get propagated down properly. Other merge_artifacts behaviors
    # are tested in the main test (merge_artifacts.py). This test basically
    # only matches with the small-foreach graph whereas the other test is
    # more generic.
    PRIORITY = 1

    @steps(0, ["start"])
    def start(self):
        self.non_modified_passdown = "a"

    @steps(0, ["foreach-inner-small"], required=True)
    def modify_things(self):
        # Set different names to different things
        val = self.index
        setattr(self, "var%d" % (val), val)

    @steps(0, ["foreach-join-small"], required=True)
    def merge_things(self, inputs):
        self.merge_artifacts(
            inputs,
        )

        # Ensure that everything we expect is passed down
        assert_equals(self.non_modified_passdown, "a")
        for i, _ in enumerate(inputs):
            assert_equals(getattr(self, "var%d" % (i)), i)

    @steps(1, ["all"])
    def step_all(self):
        assert_equals(self.non_modified_passdown, "a")
