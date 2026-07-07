from types import SimpleNamespace

from metaflow.user_decorators.mutable_step import MutableStep
from metaflow.user_decorators.user_step_decorator import UserStepDecoratorBase


class DummyStepDecorator:
    def __init__(self, name):
        self.name = name

    def get_args_kwargs(self):
        return [], {}


def test_remove_decorator(monkeypatch):
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

    monkeypatch.setattr(
        UserStepDecoratorBase,
        "get_decorator_by_name",
        staticmethod(lambda _: object),
    )

    removed = step.remove_decorator("retry")

    assert step._my_step.decorators == []
    assert removed is True
