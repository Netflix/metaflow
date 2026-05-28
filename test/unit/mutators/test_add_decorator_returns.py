"""Tests that MutableStep.add_decorator returns the decorator instance."""


class TestAddDecoratorReturns:
    def test_flow_completes(self, add_decorator_return_run):
        assert add_decorator_return_run.successful

    def test_returned_decorator_is_not_none(self, add_decorator_return_run):
        task = add_decorator_return_run["start"].task
        assert task.data.returned_is_none is False

    def test_returned_decorator_has_name(self, add_decorator_return_run):
        task = add_decorator_return_run["start"].task
        assert task.data.returned_has_name is True

    def test_decorator_was_applied(self, add_decorator_return_run):
        task = add_decorator_return_run["start"].task
        assert task.data.added_var == "from_mutator"

    def test_duplicate_ignore_returns_none(self, add_decorator_return_run):
        task = add_decorator_return_run["start"].task
        assert task.data.duplicate_is_none is True

    def test_duplicate_was_not_applied(self, add_decorator_return_run):
        task = add_decorator_return_run["start"].task
        assert task.data.should_not_exist is None
