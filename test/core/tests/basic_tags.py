# -*- coding: utf-8 -*-
from metaflow_test import MetaflowTest, ExpectationFailed, steps


class BasicTagTest(MetaflowTest):
    """
    Test that tags are assigned properly.
    """

    PRIORITY = 2
    HEADER = "@project(name='basic_tag')"

    @steps(0, ["all"])
    def step_all(self):
        # TODO we could call self.tag() in some steps, once it is implemented
        from metaflow import get_namespace
        import os

        user = "user:%s" % os.environ.get("METAFLOW_USER")
        assert_equals(user, get_namespace())

    def check_results(self, flow, checker):
        import os
        from metaflow import namespace

        run = checker.get_run()
        if run is None:
            # CliChecker does not return a run object, that's ok
            return
        flow_obj = run.parent
        # test crazy unicode and spaces in tags
        # these tags must be set with --tag option in contexts.json
        tags = (
            "project:basic_tag",
            "project_branch:user.tester",
            "user:%s" % os.environ.get("METAFLOW_USER"),
            "刺身 means sashimi",
            "multiple tags should be ok",
        )
        for tag in tags:
            # test different namespaces: one is a system-tag,
            # another is a user tag
            namespace(tag)
            run = flow_obj[checker.run_id]
            # the flow object should not have tags
            assert_equals(frozenset(), frozenset(flow_obj.tags))
            # the run object should have the namespace tags
            assert_equals([True] * len(tags), [t in run.tags for t in tags])
            # filtering by a non-existent tag should return nothing
            assert_equals([], list(flow_obj.runs("not_a_tag")))
            # a conjunction of a non-existent tag and an existent tag
            # should return nothing
            assert_equals([], list(flow_obj.runs("not_a_tag", tag)))
            # all steps should be returned with tag filtering
            assert_equals(
                frozenset(step.name for step in flow),
                frozenset(step.id.split("/")[-1] for step in run.steps(tag)),
            )
            # a conjunction of two existent tags should return the original list
            assert_equals(
                frozenset(step.name for step in flow),
                frozenset(step.id.split("/")[-1] for step in run.steps(*tags)),
            )
            # all tasks should be returned with tag filtering
            for step in run:
                # the run object should have the tags
                assert_equals([True] * len(tags), [t in step.tags for t in tags])
                # filtering by a non-existent tag should return nothing
                assert_equals([], list(step.tasks("not_a_tag")))
                # filtering by the tag should not exclude any tasks
                assert_equals(
                    [task.id for task in step], [task.id for task in step.tasks(tag)]
                )
                for task in step.tasks(tag):
                    # the task object should have the tags
                    assert_equals([True] * len(tags), [t in task.tags for t in tags])
                    for data in task:
                        # the data artifact should have the tags
                        assert_equals(
                            [True] * len(tags), [t in data.tags for t in tags]
                        )
