from metaflow.tests.flows.new_linear_flows import (
    NewLinearFlow1,
    NewLinearFlow2,
    NewLinearFlow3,
)


class Flow12(NewLinearFlow1, NewLinearFlow2):
    pass


class Flow123(NewLinearFlow1, NewLinearFlow2, NewLinearFlow3):
    pass


if __name__ == "__main__":
    # Optional: set a default flow / enable running via the current `python <file>` CLI form
    Flow123()
