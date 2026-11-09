"""Regression tests for user-defined card component warnings."""

from metaflow.plugins.cards.card_modules.components import UserComponent


class ComponentWithoutUpdate(UserComponent):
    def render(self):
        return "component without update"


def test_update_without_implementation_warns_instead_of_raising():
    component = ComponentWithoutUpdate()
    logged = []
    component._logger = lambda msg, timestamp=False, bad=False: logged.append(msg)

    component.update("new value")

    assert len(logged) == 1
    assert "ComponentWithoutUpdate" in logged[0]
    assert "not compatible with realtime updates" in logged[0]


def test_update_without_implementation_warns_only_once():
    component = ComponentWithoutUpdate()
    logged = []
    component._logger = lambda msg, timestamp=False, bad=False: logged.append(msg)

    component.update("first")
    component.update("second")

    assert len(logged) == 1


def test_update_without_implementation_and_without_logger_is_a_no_op():
    component = ComponentWithoutUpdate()

    assert component._logger is None
    component.update("new value")  # raised TypeError before the fix
