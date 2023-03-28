from os.path import join
import subprocess
from subprocess import PIPE
from sys import executable as python

from metaflow.tests.utils import (
    metaflow_bin,
    metaflow_version,
    parametrize,
    run,
    test_flows_dir,
    verify_output,
)
from metaflow.util import resolve_identity


linear_flow_path = join(test_flows_dir, "linear_flow.py")
linear_flows_path = join(test_flows_dir, "linear_flows.py")
old_flow1_path = join(test_flows_dir, "old_flow1.py")


# TODO: use a fresh metaflow db in a tempdir to avoid races / concurrent runs by different processes

linear_flows_show = """
Step start
    ?
    => end

Step end
    ?
"""

# Map flow names to their source files, for cleaner pytest reporting (tests are parameterized by flow name rather than
# full paths)
#
# The tests in this file exercise various Metaflow CLI forms by passing flow files/names (as strings) to shell commands.
flows = {
    "LinearFlow": dict(
        file=linear_flow_path,
        data=dict(a=111, b=222, checked=True),
        show="""
Step start
    ?
    => one

Step one
    ?
    => two

Step two
    ?
    => three

Step three
    ?
    => end

Step end
    ?
""",
        desc="""Simple flow with 3 linear steps that read/write flow properties

Also includes a `@property` and a method, verifying that they can coexist with `@step` functions
""",
    ),
    "LinearFlow1": dict(
        file=linear_flows_path, data=dict(a=111, b=222), show=linear_flows_show, desc=""
    ),
    "LinearFlow2": dict(
        file=linear_flows_path, data=dict(c=333, d=444), show=linear_flows_show, desc=""
    ),
    "LinearFlow3": dict(
        file=linear_flows_path, data=dict(e=555, f=666), show=linear_flows_show, desc=""
    ),
}


# Verify running flows without a subcommand prints the expected subcommand-listing / help msg (and `metaflow flow …`
# entrypoint form)
@parametrize(
    "flow",
    [
        "LinearFlow",
    ],
)
def test_flow_base_cmd(flow):
    file = flows[flow]["file"]
    flow_path_spec = "%s:%s" % (file, flow)
    user = resolve_identity()
    cmd = [
        metaflow_bin,
        "flow",
        flow_path_spec,
    ]
    expected_stdout = ""
    expected_stderr = """Metaflow {version} executing {flow} for {user}
Validating your flow...
    The graph looks good!
Running pylint on {file}
    Pylint is happy!

'metaflow flow {file}:{flow} show' shows a description of this flow.
'metaflow flow {file}:{flow} run' runs the flow locally.
'metaflow flow {file}:{flow} help' shows all available commands and options.

""".format(
        file=file, flow=flow, user=user, version=metaflow_version
    )
    verify_output(cmd, expected_stdout, expected_stderr)


# Test various command-line forms for running `show`:
# - `metaflow flow <file> show` (assumes file contains exactly 1 flow)
# - `metaflow flow <file>:<name> show`
# - `python -m metaflow.cmd.main_cli flow <file> show` (assumes file contains exactly 1 flow)
# - `python -m metaflow.cmd.main_cli flow <file>:<name> show`
# - `python <file> show` (uses __main__ handler)
cli_forms = {
    "metaflow flow <file>": dict(
        cmd=[
            metaflow_bin,
            "flow",
            "{file}",
        ],
        flows=[
            "LinearFlow",
        ],
    ),
    "metaflow flow <path_spec>": dict(
        cmd=[
            metaflow_bin,
            "flow",
            "{path_spec}",
        ],
        flows=[
            "LinearFlow",
            "LinearFlow1",
            "LinearFlow2",
            "LinearFlow3",
        ],
    ),
    "main_cli <file>": dict(
        cmd=[
            python,
            "-m",
            "metaflow.cmd.main_cli",
            "flow",
            "{file}",
        ],
        flows=[
            "LinearFlow",
        ],
    ),
    "main_cli <path_spec>": dict(
        cmd=[
            python,
            "-m",
            "metaflow.cmd.main_cli",
            "flow",
            "{path_spec}",
        ],
        flows=[
            "LinearFlow",
            "LinearFlow1",
            "LinearFlow2",
            "LinearFlow3",
        ],
    ),
    "python <file>": dict(
        cmd=[
            python,
            "{file}",
        ],
        flows=[
            "LinearFlow",
            "LinearFlow2",
        ],
    ),
}
expanded_forms = [
    (obj["cmd"], flow) for cli_form, obj in cli_forms.items() for flow in obj["flows"]
]


@parametrize("cmd,name", expanded_forms)
def test_show(cmd, name):
    """Test `show`ing a flow via a given CLI entrypoint."""
    flow = flows[name]
    file = flow["file"]
    desc = flow["desc"] or "\n"
    show = flow["show"]
    path_spec = "%s:%s" % (file, name)
    cmd = [arg.format(path_spec=path_spec, file=file) for arg in cmd] + [
        "show",
    ]
    user = resolve_identity()
    expected_stdout = show
    expected_stderr = """Metaflow {v} executing {flow} for {user}

{desc}
""".format(
        v=metaflow_version, flow=name, user=user, desc=desc
    )
    verify_output(cmd, expected_stdout, expected_stderr)


@parametrize("cmd,name", expanded_forms)
def test_run(cmd, name):
    """Test `run`ing a flow via `python -m metaflow.cmd.main_cli flow <flow> run` and `metaflow flow <flow> run`"""
    flow = flows[name]
    file = flow["file"]
    expected = flow["data"]
    path_spec = "%s:%s" % (file, name)
    cmd = [arg.format(path_spec=path_spec, file=file) for arg in cmd] + [
        "run",
    ]
    actual = run(name, cmd=cmd)
    assert actual == expected


@parametrize(
    "id",
    [
        "metaflow flow <file>",
        "main_cli <file>",
    ],
)
def test_ambiguous_flow_error(id):
    cli_form = cli_forms[id]
    cmd = cli_form["cmd"]
    file = linear_flows_path
    cmd = [arg.format(file=file) for arg in cmd] + [
        "show",
    ]
    p = subprocess.run(cmd, stdout=PIPE, stderr=PIPE)
    assert p.returncode == 1
    stderr = p.stderr.decode()
    assert ("RuntimeError: 3 roots found in %s" % file) in stderr
