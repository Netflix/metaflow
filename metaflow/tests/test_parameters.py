from metaflow._vendor.click.exceptions import NoSuchOption
from pytest import raises

from metaflow.tests.flows import (
    NewParameterFlow1,
    NewParameterFlow2,
    NewParameterFlow3,
    ParameterFlow1,
    ParameterFlow2,
    ParameterFlow3,
)


from metaflow.tests.utils import metaflow_bin, parametrize, run


@parametrize(
    "flow",
    [
        NewParameterFlow1,
        ParameterFlow1,
    ],
)
@parametrize(
    "sdk",
    [
        "python",
        "shell",
    ],
)
@parametrize(
    "debug,msg",
    [
        [None, "default mode"],
        [False, "regular mode"],
        [True, "debug mode"],
    ],
)
def test_parameter_flow1(flow, sdk, debug, msg):
    if debug is None:
        args = []
    else:
        args = ["--debug", str(debug)]

    if sdk == "python":
        data = run(
            flow,
            args=[
                "run",
            ]
            + args,
        )
    else:
        name = flow.name
        path_spec = flow.path_spec
        data = run(
            name,
            cmd=[
                metaflow_bin,
                "flow",
                path_spec,
                "run",
            ]
            + args,
        )

    assert data == {
        "debug": debug,
        "msg": msg,
    }


@parametrize(
    "Flow2",
    [
        ParameterFlow2,
        NewParameterFlow2,
    ],
)
@parametrize(
    "Flow3",
    [
        ParameterFlow3,
        NewParameterFlow3,
    ],
)
def test_clear_main_flow(Flow2, Flow3):
    # Normal Flow2 run with "--str" flag
    data = run(Flow2, args=["run", "--str", "bbb"])
    assert data == {
        "string": "bbb",
        "upper": "BBB",
    }

    # Flow3's "--int" flag is not allowed
    with raises(NoSuchOption):
        run(Flow2, args=["run", "--int", "111"])

    # Flow2/"--str" still works
    data = run(Flow2, args=["run", "--str", "cccc"])
    assert data == {
        "string": "cccc",
        "upper": "CCCC",
    }

    # Switch to Flow3 / "--int" flag
    data = run(Flow3, args=["run", "--int", "11"])
    assert data == {
        "int": 11,
        "squared": 121,
    }

    # Flow2's "--str" flag is not allowed
    with raises(NoSuchOption):
        run(Flow3, args=["run", "--str", "ddd"])

    # Flow3/"--int" still works
    data = run(Flow3, args=["run", "--int", "100"])
    assert data == {
        "int": 100,
        "squared": 10_000,
    }
