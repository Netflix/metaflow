# -*- coding: utf-8 -*-
from metaflow_test import MetaflowTest, ExpectationFailed, steps


class TagMutationTest(MetaflowTest):
    """
    Test that tag mutation works
    """

    PRIORITY = 2
    HEADER = "@project(name='tag_mutation')"

    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current, Task
        import uuid

        run = Task(current.pathspec).parent.parent

        # add 20 tags
        self.added_tags_at_start = ["start-%s" % str(uuid.uuid4()) for _ in range(20)]
        run.add_tags(self.added_tags_at_start)
        for tag in self.added_tags_at_start:
            assert tag in run.tags, "Tags added at start should be in effect"

    @steps(2, ["all"])
    def step_all(self):
        pass

    @steps(1, ["singleton"])
    def step_singleton(self):
        from metaflow import current, Task

        # util function for loading Run data
        run = Task(current.pathspec).parent.parent

        # remove 10, keep 10
        tags_to_remove = self.added_tags_at_start[:10]
        tags_to_remain = self.added_tags_at_start[10:]
        run.remove_tags(tags_to_remove)

        for tag in tags_to_remain:
            assert tag in run.tags
        for tag in tags_to_remove:
            assert tag not in run.tags

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
        run.add_tags(["tag_along", *some_existing_system_tags])
        assert "tag_along" in run.tags
        assert len(set(some_existing_system_tags) & run.user_tags) == 0

        # Verify that trying to remove a tag that already exists as a system tag fails (all or nothing)
        assert_exception(
            lambda: run.remove_tags(["tag_along", *some_existing_system_tags]),
            MetaflowTaggingError,
        )
        assert "tag_along" in run.tags
        run.remove_tag("tag_along")
        assert "tag_along" not in run.tags

        # Verify "remove, then add" behavior of replace_tags
        run.add_tags(["AAA", "BBB"])
        assert "AAA" in run.user_tags and "BBB" in run.user_tags
        run.replace_tags(["AAA", "BBB"], ["BBB", "CCC"])
        assert "AAA" not in run.user_tags
        assert "BBB" in run.user_tags
        assert "CCC" in run.user_tags

        assert_exception(lambda: run.add_tag(""), MetaflowTaggingError)
        assert "" not in run.tags
        assert_exception(lambda: run.remove_tag(4), MetaflowTaggingError)
        assert 4 not in run.tags
