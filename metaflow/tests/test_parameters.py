from metaflow._vendor.click.exceptions import NoSuchOption
from pytest import raises

from metaflow.tests.flows import ParameterFlow1, ParameterFlow2, ParameterFlow3
from metaflow.tests.utils import metaflow_bin, parametrize, run


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
def test_parameter_flow1(sdk, debug, msg):
    if debug is None:
        args = []
    else:
        args = ["--debug", str(debug)]

    if sdk == "python":
        data = run(
            ParameterFlow1,
            args=[
                "run",
            ]
            + args,
        )
    else:
        data = run(
            "ParameterFlow1",
            cmd=[
                metaflow_bin,
                "flow",
                ParameterFlow1.path_spec,
                "run",
            ]
            + args,
        )

    assert data == {
        "debug": debug,
        "msg": msg,
    }


def test_clear_main_flow():
    # Normal ParameterFlow2 run with "--str" flag
    data = run(ParameterFlow2, args=["run", "--str", "bbb"])
    assert data == {
        "string": "bbb",
        "upper": "BBB",
    }

    # ParameterFlow3's "--int" flag is not allowed
    with raises(NoSuchOption):
        run(ParameterFlow2, args=["run", "--int", "111"])

    # ParameterFlow2/"--str" still works
    data = run(ParameterFlow2, args=["run", "--str", "cccc"])
    assert data == {
        "string": "cccc",
        "upper": "CCCC",
    }

    # Switch to ParameterFlow3 / "--int" flag
    data = run(ParameterFlow3, args=["run", "--int", "11"])
    assert data == {
        "int": 11,
        "squared": 121,
    }

    # ParameterFlow2's "--str" flag is not allowed
    with raises(NoSuchOption):
        run(ParameterFlow3, args=["run", "--str", "ddd"])

    # ParameterFlow3/"--int" still works
    data = run(ParameterFlow3, args=["run", "--int", "100"])
    assert data == {
        "int": 100,
        "squared": 10_000,
    }
