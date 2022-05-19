from metaflow.tests.flows.branching import NewBranchingFlow, OldBranchingFlow
from metaflow.tests.flows.joins import (
    NewJoinFlow1,
    OldJoinFlow1,
    NewJoinFlow2,
    OldJoinFlow2,
    OldForeachSplit,
    NewForeachSplit,
)

from metaflow.tests.utils import check_graph, parametrize, py37dec, run


# fmt: off
graph = [
    { "name":         "start", "type":   "start", "in_funcs": [               ], "out_funcs": ["generate_ints"], "split_parents": [               ], },
    { "name": "generate_ints", "type": "foreach", "in_funcs": [        "start"], "out_funcs": [   "test_prime"], "split_parents": [               ], },
    { "name":    "test_prime", "type":  "linear", "in_funcs": ["generate_ints"], "out_funcs": [     "fizzbuzz"], "split_parents": ["generate_ints"], },
    { "name":      "fizzbuzz", "type":  "linear", "in_funcs": [   "test_prime"], "out_funcs": [         "join"], "split_parents": ["generate_ints"], },
    { "name":          "join", "type":    "join", "in_funcs": [     "fizzbuzz"], "out_funcs": [          "end"], "split_parents": ["generate_ints"], },
    { "name":           "end", "type":     "end", "in_funcs": [         "join"], "out_funcs": [               ], "split_parents": [               ], },
]


results = [
    { "n":  1, "is_prime": False,                        },
    { "n":  2, "is_prime":  True,                        },
    { "n":  3, "is_prime":  True, "fizzbuzz":     "fizz" },
    { "n":  4, "is_prime": False,                        },
    { "n":  5, "is_prime":  True, "fizzbuzz":     "buzz" },
    { "n":  6, "is_prime": False, "fizzbuzz":     "fizz" },
    { "n":  7, "is_prime":  True,                        },
    { "n":  8, "is_prime": False,                        },
    { "n":  9, "is_prime": False, "fizzbuzz":     "fizz" },
    { "n": 10, "is_prime": False, "fizzbuzz":     "buzz" },
    { "n": 11, "is_prime":  True,                        },
    { "n": 12, "is_prime": False, "fizzbuzz":     "fizz" },
    { "n": 13, "is_prime":  True,                        },
    { "n": 14, "is_prime": False,                        },
    { "n": 15, "is_prime": False, "fizzbuzz": "fizzbuzz" },
]


@parametrize( "flow", [ OldJoinFlow1, NewJoinFlow1, ], )
def test_joins1(flow):
    check_graph(flow, graph)
    data = run(flow)
    # TODO: why can the results sometimes come back out of order? Should that be fixed?
    data["results"] = sorted(data["results"], key=lambda r: r["n"])
    assert data == {
        "results": results,
    }


odds = [
    { "n":  1, "is_prime": False,                        },
    { "n":  3, "is_prime":  True, "fizzbuzz":     "fizz" },
    { "n":  5, "is_prime":  True, "fizzbuzz":     "buzz" },
    { "n":  7, "is_prime":  True,                        },
    { "n":  9, "is_prime": False, "fizzbuzz":     "fizz" },
    { "n": 11, "is_prime":  True,                        },
    { "n": 13, "is_prime":  True,                        },
    { "n": 15, "is_prime": False, "fizzbuzz": "fizzbuzz" },
]


join_flow2_graph = [
    { "name":         "start", "type":   "start", "in_funcs": [               ], "out_funcs": ["generate_ints"], "split_parents": [               ], },
    { "name": "generate_ints", "type": "foreach", "in_funcs": [        "start"], "out_funcs": [   "test_prime"], "split_parents": [               ], },
    { "name":    "test_prime", "type":  "linear", "in_funcs": ["generate_ints"], "out_funcs": [     "fizzbuzz"], "split_parents": ["generate_ints"], },
    { "name":      "fizzbuzz", "type":  "linear", "in_funcs": [   "test_prime"], "out_funcs": [         "join"], "split_parents": ["generate_ints"], },
    { "name":          "join", "type":    "join", "in_funcs": [     "fizzbuzz"], "out_funcs": [  "filter_odds"], "split_parents": ["generate_ints"], },
    { "name":   "filter_odds", "type":  "linear", "in_funcs": [         "join"], "out_funcs": [          "end"], "split_parents": [               ], },
    { "name":           "end", "type":     "end", "in_funcs": [  "filter_odds"], "out_funcs": [               ], "split_parents": [               ], },
]


@parametrize( "flow", [ OldJoinFlow2, NewJoinFlow2, ], )
def test_joins2(flow):
    check_graph(flow, join_flow2_graph)
    data = run(flow)
    # TODO: why can the results sometimes come back out of order? Should that be fixed?
    data["odds"] = sorted(data["odds"], key=lambda r: r["n"])
    data["results"] = sorted(data["results"], key=lambda r: r["n"])
    assert data == {
        "odds": odds,
        "results": results,
    }


branching_graph = [
    { "name": "start", "type":  "start", "in_funcs": [            ], "out_funcs": [       "one"], "split_parents": [     ], },
    { "name":   "one", "type":  "split", "in_funcs": [     "start"], "out_funcs": ["aaa", "bbb"], "split_parents": [     ], },
    { "name":   "aaa", "type": "linear", "in_funcs": [       "one"], "out_funcs": [      "join"], "split_parents": ["one"], },
    { "name":   "bbb", "type": "linear", "in_funcs": [       "one"], "out_funcs": [      "join"], "split_parents": ["one"], },
    { "name":  "join", "type":   "join", "in_funcs": ["aaa", "bbb"], "out_funcs": [       "end"], "split_parents": ["one"], },
    { "name":   "end", "type":    "end", "in_funcs": [      "join"], "out_funcs": [            ], "split_parents": [     ], },
]


@parametrize(
    "flow,func_linenos",
    [
        ( OldBranchingFlow, map(py37dec, [10, 14, 19, 24, 29, 38, ]), ),
        ( NewBranchingFlow, [42, py37dec(44), py37dec(48), py37dec(52), py37dec(56), 62, ], ),
    ],
)
def test_branching_flow(flow, func_linenos):
    graph = [
        dict(**obj, func_lineno=func_lineno)
        for obj, func_lineno in zip(branching_graph, func_linenos)
    ]
    check_graph(flow, graph)
    data = run(flow)
    assert data == {"n": 11, "a": "AAAAAAAAAAA", "b": "BBBBBBBBBBB", "done": True}


foreach_split_graph = [
    { "name":        "start", "type": "foreach", "in_funcs": [                ], "out_funcs": [      "foreach" ], "split_parents": [                    ], },
    { "name":      "foreach", "type":   "split", "in_funcs": [        "start" ], "out_funcs": [     "f1", "f2" ], "split_parents": [ "start",           ], },
    { "name":           "f1", "type":  "linear", "in_funcs": [      "foreach" ], "out_funcs": [           "f3" ], "split_parents": [ "start", "foreach" ], },
    { "name":           "f2", "type":  "linear", "in_funcs": [      "foreach" ], "out_funcs": [           "f3" ], "split_parents": [ "start", "foreach" ], },
    { "name":           "f3", "type":    "join", "in_funcs": [     "f1", "f2" ], "out_funcs": [ "join_foreach" ], "split_parents": [ "start", "foreach" ], },
    { "name": "join_foreach", "type":    "join", "in_funcs": [           "f3" ], "out_funcs": [          "end" ], "split_parents": [ "start",           ], },
    { "name":          "end", "type":     "end", "in_funcs": [ "join_foreach" ], "out_funcs": [                ], "split_parents": [                    ], },
]


@parametrize("flow", [ OldForeachSplit, NewForeachSplit, ], )
def test_foreach_splitand(flow):
    check_graph(flow, foreach_split_graph)


# fmt: on
