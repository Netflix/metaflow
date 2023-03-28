from os.path import dirname, join
import subprocess
from subprocess import check_call, PIPE
from sys import executable as python, version_info

import pytest

parametrize = pytest.mark.parametrize

from metaflow import Flow
import metaflow.metaflow_version
from metaflow.exception import MetaflowNotFound
from metaflow.parameters import register_main_flow


metaflow_bin = join(dirname(python), "metaflow")
metaflow_version = metaflow.metaflow_version.get_version()

tests_dir = dirname(__file__)
test_flows_dir = join(tests_dir, "flows")
metaflow_dir = dirname(tests_dir)
tutorials_dir = join(metaflow_dir, "tutorials")


def flow_path(name):
    return join(test_flows_dir, name)


# Run a flow, verify Metaflow sees one new run of that type, and return that run's data
def run(flow, fn=None, cmd=None, args=None, entrypoint=None):
    if isinstance(flow, str):
        name = flow
        assert cmd
    else:
        name = flow.name

    assert not (cmd and (args or entrypoint))

    try:
        # TODO: how does this work when multiple flows have same basename?
        # TODO: enable passing FlowSpec here to ensure loading correct runs
        n_runs_before = len(list(Flow(name)))
    except MetaflowNotFound:
        n_runs_before = 0

    if cmd:
        check_call(cmd)
    else:
        register_main_flow(flow, overwrite=True)

        if args is None:
            args = ("run",)
        f = flow(args=args, entrypoint=entrypoint, standalone_mode=False)
        assert f._success

    runs = list(Flow(name))
    n_runs_after = len(runs)
    # TODO: more rigorously verify we are grabbing the sole new run / use a tmpdir as metaflow db
    assert (
        n_runs_before + 1 == n_runs_after
    ), "n_runs_before=%d + 1 != n_runs_after=%d" % (n_runs_before, n_runs_after)
    run = runs[0]
    data = run.data
    if fn:
        return fn(data)
    else:
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


def verify_output(cmd, expected_stdout, expected_stderr):
    p = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, check=True)
    actual_stdout = p.stdout.decode()
    actual_stderr = p.stderr.decode()
    assert actual_stdout == expected_stdout, '"""%s""" != """%s"""' % (
        actual_stdout,
        expected_stdout,
    )
    assert actual_stderr == expected_stderr, '"""%s""" != """%s"""' % (
        actual_stderr,
        expected_stderr,
    )


def py37dec(a: int, b: int = 1) -> int:
    assert version_info.major == 3
    return a - b if version_info.minor <= 7 else a
