"""Test concordance between a simple "new API" flow (`NewLinearFlow`) and equivalent "current API" flow (`LinearFlow`).
"""

from metaflow.tests.flows import LinearFlow, NewLinearFlow
from metaflow.tests.utils import check_graph, flow_path, parametrize, py37dec, run


# fmt: off
@parametrize(
    "flow,file,name",
    [
        (    LinearFlow,     "linear_flow.py",    "LinearFlow", ),
        ( NewLinearFlow, "new_linear_flow.py", "NewLinearFlow", ),
    ],
)
def test_flowspec_attrs(flow, file, name):
    # Verify class-level properties: `file`, `name`, `path_spec`
    file = flow_path(file)
    assert flow.file == file
    assert flow.name == name
    assert flow.path_spec == ("%s:%s" % (file, name))


line_no_map = {
       LinearFlow: [py37dec(i) for i in [11, 15, 20, 25, 31, ]],
    NewLinearFlow: [11, py37dec(13), py37dec(17), py37dec(21), 32, ],
}


@parametrize(
    "flow,file",
    [
        (    LinearFlow,     "linear_flow.py", ),
        ( NewLinearFlow, "new_linear_flow.py", ),
    ],
)
def test_api(flow, file):
    # Verify graph; note that @step-function line numbers point at the `@step` decorator in Python ≤3.7; in Python ≥3.8,
    # they point at the function `def` line. When Metaflow's CI adds support for newer Python versions, these expected
    # line numbers will have to take the Python version into account. See also: https://bugs.python.org/issue33211.
    # fmt: off
    expected = [
        {'name': 'start', 'type':  'start', 'in_funcs': [       ], 'out_funcs': [  'one'], },
        {'name':   'one', 'type': 'linear', 'in_funcs': ['start'], 'out_funcs': [  'two'], },
        {'name':   'two', 'type': 'linear', 'in_funcs': [  'one'], 'out_funcs': ['three'], },
        {'name': 'three', 'type': 'linear', 'in_funcs': [  'two'], 'out_funcs': [  'end'], },
        {'name':   'end', 'type':    'end', 'in_funcs': ['three'], 'out_funcs': [       ], },
    ]
    # fmt: on

    # Add `@step` line numbers (different for each flow) to expected output
    line_nos = line_no_map[flow]
    expected = [
        {
            **o,
            "func_lineno": lineno,
        }
        for o, lineno in zip(expected, line_nos)
    ]

    # Verify old and new flow graphs match; note that @step-function line numbers point at the `@step` decorator in
    # Python ≤3.7; in Python ≥3.8, they point at the function `def` line. When Metaflow's CI adds support for newer
    # Python versions, these expected line numbers will have to take the Python version into account. See also:
    # https://bugs.python.org/issue33211.
    check_graph(flow, expected)

    # Verify that the flow runs successfully and that data artifacts are as expected.
    data = run(flow)

    # Verify fields set during flow execution
    assert data == {
        "a": 111,
        "b": 222,
        "checked": True,
    }


# fmt: on
