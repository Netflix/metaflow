# -*- coding: utf-8 -*-
from metaflow_test import MetaflowTest, ExpectationFailed, steps


class TagMutationTest(MetaflowTest):
    """
    Test that tag mutation works
    """

    PRIORITY = 2
    HEADER = "@project(name='tag_mutation')"

    @steps(1, ["all"])
    def step_all(self):
        from metaflow import current, Task

        run = Task(current.pathspec).parent.parent
        for i in range(7):
            tag = str(i)
            run.add_tag(tag)
            assert tag in run.user_tags
            run.remove_tag(tag)
            assert tag not in run.user_tags
        # TODO let's verify Task tags match up with run tags here

    def check_results(self, flow, checker):
        from metaflow import Run
        from metaflow.exception import MetaflowTaggingError
        import random

        run_id = checker.run_id
        flow_name = flow.name

        run = Run("%s/%s" % (flow_name, run_id))

        some_existing_system_tags = random.sample(
            list(run.system_tags), min(len(run.system_tags) // 2, 1)
        )

        # Verify that trying to add a tag that already exists as a system tag is OK (only non system tags get added)
        run.add_tag(["tag_along", *some_existing_system_tags])
        assert "tag_along" in run.tags
        assert len(set(some_existing_system_tags) & run.user_tags) == 0

        # Verify that trying to remove a tag that already exists as a system tag fails (all or nothing)
        assert_exception(
            lambda: run.remove_tag(["tag_along", *some_existing_system_tags]),
            MetaflowTaggingError,
        )
        assert "tag_along" in run.tags
        run.remove_tag("tag_along")
        assert "tag_along" not in run.tags

        # Verify "remove, then add" behavior of replace_tags
        run.add_tag(["AAA", "BBB"])
        assert "AAA" in run.user_tags and "BBB" in run.user_tags
        run.replace_tag(["AAA", "BBB"], ["BBB", "CCC"])
        assert "AAA" not in run.user_tags
        assert "BBB" in run.user_tags
        assert "CCC" in run.user_tags

        # try empty str as tag - should fail
        assert_exception(lambda: run.add_tag(""), MetaflowTaggingError)
        assert "" not in run.tags

        # try int as tag - should fail
        assert_exception(lambda: run.remove_tag(4), MetaflowTaggingError)
        assert 4 not in run.tags

        # try to replace nothing with nothing - should fail
        assert_exception(lambda: run.replace_tag([], []), MetaflowTaggingError)
