from os.path import join
from pytest import raises

from metaflow.tests.flows import Flow12, Flow123
from metaflow.tests.utils import (
    check_graph,
    metaflow_bin,
    metaflow_version,
    test_flows_dir,
    run,
    verify_output,
    py37dec,
)
from metaflow.util import resolve_identity


def flow_path(name):
    return join(test_flows_dir, name)


# fmt: off
aaa_step = { "name":  "aaa", "type": "linear", "in_funcs": ["start"], "out_funcs": ["bbb"], "split_parents": [], "file": flow_path("new_linear_flows.py"), "func_lineno": py37dec(7), }
bbb_step = { "name":  "bbb", "type": "linear", "in_funcs": [  "aaa"], "out_funcs": ["ccc"], "split_parents": [], "file": flow_path("new_linear_flows.py"), "func_lineno": py37dec(11), }
ccc_step = { "name":  "ccc", "type": "linear", "in_funcs": [  "bbb"], "out_funcs": ["ddd"], "split_parents": [], "file": flow_path("new_linear_flows.py"), "func_lineno": py37dec(18), }
ddd_step = lambda out: { "name":  "ddd", "type": "linear", "in_funcs": [  "ccc"], "out_funcs": [out], "split_parents": [], "file": flow_path("new_linear_flows.py"), "func_lineno": py37dec(22), }
eee_step = { "name":   "eee", "type": "linear", "in_funcs": [  "ddd"], "out_funcs": ["fff"], "split_parents": [], "file": flow_path("new_linear_flows.py"), "func_lineno": py37dec(29), }
fff_step = { "name":   "fff", "type": "linear", "in_funcs": [  "eee"], "out_funcs": ["end"], "split_parents": [], "file": flow_path("new_linear_flows.py"), "func_lineno": py37dec(33), }


flow12_graph = [
    { "name": "start", "type":  "start", "in_funcs": [       ], "out_funcs": ["aaa"], "split_parents": [], "file": flow_path( "inherited_flows.py"), "func_lineno":  8, },
    aaa_step,
    bbb_step,
    ccc_step,
    ddd_step("end"),
    { "name":   "end", "type":    "end", "in_funcs": [  "ddd"], "out_funcs": [     ], "split_parents": [], "file": flow_path( "inherited_flows.py"), "func_lineno": 11, },
]


def test_flow12():
    check_graph(Flow12, flow12_graph)
    data = run(Flow12)
    assert data == {
        "a": 111,
        "b": 222,
        "c": 333,
        "d": 444,
    }


flow123_graph = [
    { "name": "start", "type":  "start", "in_funcs": [       ], "out_funcs": ["aaa"], "split_parents": [], "file": flow_path( "inherited_flows.py"), "func_lineno": 12, },
    aaa_step,
    bbb_step,
    ccc_step,
    ddd_step("eee"),
    eee_step,
    fff_step,
    { "name":   "end", "type":    "end", "in_funcs": [  "fff"], "out_funcs": [     ], "split_parents": [], "file": flow_path( "inherited_flows.py"), "func_lineno": 15, },
]
# fmt: on


def test_flow123():
    check_graph(Flow123, flow123_graph)
    data = run(Flow123)
    assert data == {
        "a": 111,
        "b": 222,
        "c": 333,
        "d": 444,
        "e": 555,
        "f": 666,
    }


def test_flow123_show():
    user = resolve_identity()
    cmd = [metaflow_bin, "flow", Flow123.path_spec, "show"]
    stdout = """
Step start
    ?
    => aaa

Step end
    ?

Step aaa
    ?
    => bbb

Step bbb
    ?
    => ccc

Step ccc
    ?
    => ddd

Step ddd
    ?
    => eee

Step eee
    ?
    => fff

Step fff
    ?
    => end
"""
    stderr = "Metaflow {version} executing {flow} for {user}\n\n\n\n".format(
        version=metaflow_version,
        flow="Flow123",
        user=user,
    )
    verify_output(cmd, stdout, stderr)


def test_broken_inheritance():
    with raises(
        RuntimeError,
        match='Flow A: refusing to mix in multiple implementations of step "a"',
    ):
        from metaflow.tests.flows.broken_inheritance import A
