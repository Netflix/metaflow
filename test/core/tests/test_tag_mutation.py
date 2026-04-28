"""run.add_tag / remove_tag / replace_tag should mutate user_tags."""

from metaflow import FlowSpec, step


class TagMutationFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_tag_mutation(metaflow_runner, executor):
    result = metaflow_runner(TagMutationFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()

    initial = set(run.user_tags)
    assert "刺身 means sashimi" in initial

    run.add_tag("new_tag")
    assert "new_tag" in run.user_tags

    run.remove_tag("new_tag")
    assert "new_tag" not in run.user_tags

    run.replace_tag("刺身 means sashimi", "刺身 means raw fish")
    assert "刺身 means sashimi" not in run.user_tags
    assert "刺身 means raw fish" in run.user_tags
