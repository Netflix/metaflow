"""User tags supplied at run time should appear on the run object."""

from metaflow import FlowSpec, step


class BasicTagsFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_basic_tags(metaflow_runner, executor):
    result = metaflow_runner(BasicTagsFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    user_tags = set(run.user_tags)
    # The sashimi tag carries non-ASCII chars; the multi-word tag has a space.
    assert "刺身 means sashimi" in user_tags
    assert "multiple tags should be ok" in user_tags
