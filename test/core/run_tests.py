import sys
import os
import json
import uuid
import glob
import importlib
import tempfile
import shutil
import subprocess
from multiprocessing import Pool

import click

from metaflow_test import MetaflowTest
from metaflow_test.formatter import FlowFormatter


def iter_graphs():
    root = os.path.join(os.path.dirname(__file__), "graphs")
    for graphfile in os.listdir(root):
        if graphfile.endswith(".json") and not graphfile[0] == ".":
            with open(os.path.join(root, graphfile)) as f:
                yield json.load(f)


def iter_tests():
    root = os.path.join(os.path.dirname(__file__), "tests")
    sys.path.insert(0, root)
    for testfile in os.listdir(root):
        if testfile.endswith(".py") and not testfile[0] == ".":
            mod = importlib.import_module(testfile[:-3], "metaflow_test")
            for name in dir(mod):
                obj = getattr(mod, name)
                if (
                    name != "MetaflowTest"
                    and isinstance(obj, type)
                    and issubclass(obj, MetaflowTest)
                ):
                    yield obj()


def log(msg, formatter=None, context=None, real_bad=False, real_good=False):
    cstr = ""
    fstr = ""
    if context:
        cstr = " in context '%s'" % context["name"]
    if formatter:
        fstr = " %s" % formatter
    if cstr or fstr:
        line = "###%s%s: %s ###" % (fstr, cstr, msg)
    else:
        line = "### %s ###" % msg
    if real_bad:
        line = click.style(line, fg="red", bold=True)
    elif real_good:
        line = click.style(line, fg="green", bold=True)
    else:
        line = click.style(line, fg="white", bold=True)

    pid = os.getpid()
    click.echo("[pid %s] %s" % (pid, line))


def copy_coverage_files(dstdir):
    for fname in glob.glob(".coverage.*"):
        shutil.copy(fname, dstdir)


def run_test(formatter, context, coverage_dir, debug, checks, env_base):
    def run_cmd(mode):
        cmd = [context["python"], "-B", "test_flow.py"]
        cmd.extend(context["top_options"])
        cmd.extend((mode, "--run-id-file", "run-id"))
        cmd.extend(context["run_options"])
        return cmd

    cwd = os.getcwd()
    tempdir = tempfile.mkdtemp("_metaflow_test")
    package = os.path.dirname(os.path.abspath(__file__))
    try:
        # write scripts
        os.chdir(tempdir)
        with open("test_flow.py", "w") as f:
            f.write(formatter.flow_code)
        with open("check_flow.py", "w") as f:
            f.write(formatter.check_code)
        with open(".coveragerc", "w") as f:
            f.write("[run]\ndisable_warnings = module-not-measured\n")

        shutil.copytree(
            os.path.join(cwd, "metaflow_test"), os.path.join(tempdir, "metaflow_test")
        )

        path = os.path.join(tempdir, "test_flow.py")

        env = {}
        env.update(env_base)
        # expand environment variables
        # nonce can be used to insert entropy in env vars.
        # This is useful e.g. for separating S3 paths of
        # runs, which may have clashing run_ids
        env.update(
            dict(
                (k, v.format(nonce=str(uuid.uuid4())))
                for k, v in context["env"].items()
            )
        )

        pythonpath = os.environ.get("PYTHONPATH", ".")
        env.update(
            {
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": os.environ.get("PATH", "."),
                "PYTHONIOENCODING": "utf_8",
                "PYTHONPATH": "%s:%s" % (package, pythonpath),
            }
        )

        if "pre_command" in context:
            if context["pre_command"].get("metaflow_command"):
                cmd = [context["python"], "test_flow.py"]
                cmd.extend(context["top_options"])
                cmd.extend(context["pre_command"]["command"])
            else:
                cmd = context["pre_command"]["command"]
            pre_ret = subprocess.call(cmd, env=env)
            if pre_ret and not context["pre_command"].get("ignore_errors", False):
                log("pre-command failed", formatter, context)
                return pre_ret, path

        # run flow
        flow_ret = subprocess.call(run_cmd("run"), env=env)
        if flow_ret:
            if formatter.should_fail:
                log("Flow failed as expected.")
            elif formatter.should_resume:
                log("Resuming flow", formatter, context)
                flow_ret = subprocess.call(run_cmd("resume"), env=env)
            else:
                log("flow failed", formatter, context)
                return flow_ret, path
        elif formatter.should_fail:
            log("The flow should have failed but it didn't. Error!", formatter, context)
            return 1, path

        # check results
        run_id = open("run-id").read()
        ret = 0
        for check_name in context["checks"]:
            check = checks[check_name]
            python = check["python"]
            cmd = [python, "check_flow.py", check["class"], run_id]
            cmd.extend(context["top_options"])
            check_ret = subprocess.call(cmd, env=env)
            if check_ret:
                log(
                    "checker '%s' says that results failed" % check_name,
                    formatter,
                    context,
                )
                ret = check_ret
            else:
                log(
                    "checker '%s' says that results are ok" % check_name,
                    formatter,
                    context,
                )

        # copy coverage files
        if coverage_dir:
            copy_coverage_files(coverage_dir)
        return ret, path
    finally:
        os.chdir(cwd)
        if not debug:
            shutil.rmtree(tempdir)


def run_all(
    ok_tests, ok_contexts, ok_graphs, coverage_dir, debug, num_parallel, inherit_env
):

    tests = [
        test
        for test in sorted(iter_tests(), key=lambda x: x.PRIORITY)
        if not ok_tests or test.__class__.__name__.lower() in ok_tests
    ]
    failed = []

    if inherit_env:
        base_env = dict(os.environ)
    else:
        base_env = {}

    if debug or num_parallel is None:
        for test in tests:
            failed.extend(
                run_test_cases(
                    (test, ok_contexts, ok_graphs, coverage_dir, debug, base_env)
                )
            )
    else:
        args = [
            (test, ok_contexts, ok_graphs, coverage_dir, debug, base_env)
            for test in tests
        ]
        for fail in Pool(num_parallel).imap_unordered(run_test_cases, args):
            failed.extend(fail)
    return failed


def run_test_cases(args):
    test, ok_contexts, ok_graphs, coverage_dir, debug, base_env = args
    contexts = json.load(open("contexts.json"))
    graphs = list(iter_graphs())
    test_name = test.__class__.__name__
    log("Loaded test %s" % test_name)
    failed = []

    for graph in graphs:
        if ok_graphs and graph["name"].lower() not in ok_graphs:
            continue

        formatter = FlowFormatter(graph, test)
        if formatter.valid:

            for context in contexts["contexts"]:
                if context.get("disable_parallel", False) and any(
                    "num_parallel" in node for node in graph["graph"].values()
                ):
                    continue
                if ok_contexts:
                    if context["name"].lower() not in ok_contexts:
                        continue
                elif context.get("disabled", False):
                    continue
                if test_name in map(str, context.get("disabled_tests", [])):
                    continue

                enabled_tests = context.get("enabled_tests", [])
                if enabled_tests and (test_name not in map(str, enabled_tests)):
                    continue

                log("running", formatter, context)
                ret, path = run_test(
                    formatter,
                    context,
                    coverage_dir,
                    debug,
                    contexts["checks"],
                    base_env,
                )

                if ret:
                    tstid = "%s in context %s" % (formatter, context["name"])
                    failed.append((tstid, path))
                    log("failed", formatter, context, real_bad=True)
                    if debug:
                        return failed
                else:
                    log("success", formatter, context, real_good=True)
        else:
            log("not a valid combination. Skipped.", formatter)
    return failed


def produce_coverage_report(coverage_dir, coverage_output):
    COVERAGE = sys.executable + " -m coverage "
    cwd = os.getcwd()
    try:
        os.chdir(coverage_dir)
        if os.listdir("."):
            subprocess.check_call(COVERAGE + "combine .coverage*", shell=True)
            subprocess.check_call(
                COVERAGE + "xml -o %s.xml" % coverage_output, shell=True
            )
            subprocess.check_call(COVERAGE + "html -d %s" % coverage_output, shell=True)
            log("Coverage report written to %s" % coverage_output, real_good=True)
        else:
            log("No coverage data was produced", real_bad=True)
    finally:
        os.chdir(cwd)


@click.command(help="Run tests")
@click.option(
    "--contexts",
    default="",
    type=str,
    help="A comma-separated list of contexts to include (default: all).",
)
@click.option(
    "--tests",
    default="",
    type=str,
    help="A comma-separate list of graphs to include (default: all).",
)
@click.option(
    "--graphs",
    default="",
    type=str,
    help="A comma-separate list of graphs to include (default: all).",
)
@click.option(
    "--coverage-output",
    default=None,
    type=str,
    show_default=True,
    help="Output prefix for the coverage reports (default: None)",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Debug mode: Stop at the first failure, " "don't delete test directory",
)
@click.option(
    "--inherit-env", is_flag=True, default=False, help="Inherit env variables"
)
@click.option(
    "--num-parallel",
    show_default=True,
    default=None,
    type=int,
    help="Number of parallel tests to run. By default, " "tests are run sequentially.",
)
def cli(
    tests=None,
    contexts=None,
    graphs=None,
    coverage_output=None,
    num_parallel=None,
    debug=False,
    inherit_env=False,
):

    parse = lambda x: {t.lower() for t in x.split(",") if t}
    coverage_dir = (
        tempfile.mkdtemp("_metaflow_test_coverage") if coverage_output else None
    )
    try:
        failed = run_all(
            parse(tests),
            parse(contexts),
            parse(graphs),
            coverage_dir,
            debug,
            num_parallel,
            inherit_env,
        )

        if coverage_output and not debug:
            produce_coverage_report(coverage_dir, os.path.abspath(coverage_output))

        if failed:
            log("The following tests failed:")
            for fail, path in failed:
                if debug:
                    log("%s (path %s)" % (fail, path), real_bad=True)
                else:
                    log(fail, real_bad=True)
            sys.exit(1)
        else:
            log("All tests were successful!", real_good=True)
            sys.exit(0)
    finally:
        if coverage_dir:
            shutil.rmtree(coverage_dir)


if __name__ == "__main__":
    cli()
