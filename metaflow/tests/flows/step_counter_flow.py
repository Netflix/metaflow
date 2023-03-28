from metaflow.plugins import STEP_DECORATORS

from metaflow.tests.flows import NewLinearFlow
from metaflow.tests.step_counter import StepCounter

STEP_DECORATORS.append(StepCounter)


class StepCounterFlow(NewLinearFlow):
    """Example flow that can receive `--with step_counter` from the CLI

    In order for `--with my_decorator` to work on the CLI, `my_decorator` needs to added to `STEP_DECORATORS`, as above.
    I don't know of a good way to accomplish that other than making a trivial flow (`StepCounterFlow` here) that just
    mixes in the target flow (`NewLinearFlow`) in a file that appends the decorator (`StepCounter`) to `STEP_DECORATORS.

    TODO: formalize an API for registering step- and flow-decorators without having to define a new flow like this.
    """

    pass
