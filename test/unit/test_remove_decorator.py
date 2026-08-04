import pytest
from types import SimpleNamespace

from metaflow.user_decorators.mutable_step import MutableStep
from metaflow.user_decorators.user_step_decorator import UserStepDecoratorBase


class DummyStepDecorator:
    def __init__(self, name):
        self.name = name

    def get_args_kwargs(self):
        return [], {}


@pytest.fixture
def mock_step():
    """Provides a MutableStep with pre-configured decorators."""
    step = MutableStep.__new__(MutableStep)
    step._pre_mutate = True
    step._inserted_by = "test"
    step._my_step = SimpleNamespace(
        name="my_step",
        decorators=[
            DummyStepDecorator("retry"),
            DummyStepDecorator("retry"),
        ],
        wrappers=[],
        config_decorators=[],
    )
    return step


def test_remove_decorator_success(mock_step, mocker):
    # Mocking the lookup to identify our DummyStepDecorator
    mocker.patch.object(
        UserStepDecoratorBase, "get_decorator_by_name", return_value=DummyStepDecorator
    )

    removed = mock_step.remove_decorator("retry")

    assert mock_step._my_step.decorators == []
    assert removed is True


def test_remove_decorator_not_found(mock_step, mocker):
    # Mocking a scenario where the decorator is not found
    mocker.patch.object(
        UserStepDecoratorBase, "get_decorator_by_name", return_value=None
    )

    removed = mock_step.remove_decorator("nonexistent")

    assert len(mock_step._my_step.decorators) == 2
    assert removed is False
