from metaflow.tests.flows.step_counter_flow import StepCounterFlow
from metaflow.tests.utils import run


def test_step_count():
    flow = StepCounterFlow
    data = run(
        flow,
        args=(
            "run",
            "--with",
            "step_counter",
        ),
    )
    assert data == {
        "a": 111,
        "b": 222,
        "checked": True,
        "step_count": 5,
    }
