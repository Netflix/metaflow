from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ResumeTagPropagationTest(MetaflowTest):
    """
    User tags set via add_tags() on the original run should carry over to the resumed run.
    """

    RESUME = True
    PRIORITY = 3

    # Resume tests only work with linear and branch graphs.
    # Switch graphs have different step semantics.
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]

    @steps(0, ["singleton-start"], required=True)
    def step_start(self):
        from metaflow import current, Task

        if not is_resumed():
            # First run: add a dynamic tag, then fail to trigger resume.
            # Use add_tags() (plural) to match the pattern described in #1406.
            run = Task(current.pathspec).parent.parent
            run.add_tags(["propagated-tag"])
            raise ResumeFromHere()
        else:
            # Resumed run: nothing special
            self.resumed = True

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        # Verify tag propagation — works for both CliCheck (runs `tag list`
        # via CLI) and MetadataCheck (reads Run.user_tags via Python API).
        user_tags = checker.get_user_tags()
        assert "propagated-tag" in user_tags, (
            "Expected 'propagated-tag' to be propagated from original "
            "run to resumed run, but got user_tags=%s" % user_tags
        )
