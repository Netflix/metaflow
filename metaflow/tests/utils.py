from os.path import dirname, join
from sys import version_info

from metaflow import Flow
from metaflow.exception import MetaflowNotFound


tests_dir = dirname(__file__)
test_flows_dir = join(tests_dir, "flows")


def flow_path(name):
    return join(test_flows_dir, name)


# Run a flow, verify Metaflow sees one new run of that type, and return that run's data
def run(flow):
    if isinstance(flow, str):
        name = flow
        assert cmd
    else:
        name = flow.name

    try:
        # TODO: how does this work when multiple flows have same basename?
        # TODO: enable passing FlowSpec here to ensure loading correct runs
        n_runs_before = len(list(Flow(name)))
    except MetaflowNotFound:
        n_runs_before = 0

    f = flow(args=("run",), standalone_mode=False)
    assert f._success

    runs = list(Flow(name))
    n_runs_after = len(runs)
    # TODO: more rigorously verify we are grabbing the sole new run / use a tmpdir as metaflow db
    assert (
        n_runs_before + 1 == n_runs_after
    ), "n_runs_before=%d + 1 != n_runs_after=%d" % (n_runs_before, n_runs_after)
    run = runs[0]
    data = run.data
    # By default, convert the returned MetaflowData object to a dict
    return {k: v.data for k, v in data._artifacts.items()}


def check_graph(flow_spec, expected):
    # Whichever columns exist in the `expected` array will be verified from the actual flow graph
    cols = []
    for row in expected:
        for k in row:
            if k not in cols:
                cols.append(k)

    flow = flow_spec(use_cli=False)
    graph = flow._graph
    nodes = graph.nodes
    actual = []
    for k, v in nodes.items():
        assert k == v.name
        actual.append({col: getattr(v, col) for col in cols if hasattr(v, col)})

    # pytest doesn't give a clean error msg / diff below, for some reason; pretty-print the `actual` and `expected`
    # values using this helper
    def pretty(arr):
        return "[\n\t%s\n]" % "\n\t".join([str(r) for r in arr])

    assert actual == expected, "%s\n!=\n%s" % (pretty(actual), pretty(expected))


def py37dec(a: int, b: int = 1) -> int:
    assert version_info.major == 3
    return a - b if version_info.minor <= 7 else a
