import pytest
from metaflow.plugins.events_decorator import TriggerDecorator
from metaflow.user_configs.config_parameters import ConfigValue


def make_decorator():
    return TriggerDecorator(
        attributes={"event": None, "events": [], "options": {}},
        statically_defined=True,
    )


def test_process_event_does_not_mutate_config_value():
    """Regression test for https://github.com/Netflix/metaflow/issues/3080
    process_event() must not crash when event is a ConfigValue (immutable dict).
    """
    d = make_decorator()
    event = ConfigValue({"name": "my_event", "parameters": {"alpha": "beta"}})
    # Must not raise TypeError: ConfigValue is immutable
    result = d.process_event(event)
    assert result["name"] == "my_event"
    assert result["parameters"] == {"alpha": "beta"}