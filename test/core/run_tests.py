import glob
import importlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import uuid
from multiprocessing import Pool

from metaflow._vendor import click
from metaflow.cli import start
from metaflow.cli_components.run_cmds import run

skip_api_executor = False

try:
    from metaflow import Runner
    from metaflow.runner.click_api import (
        MetaflowAPI,
        click_to_python_types,
        extract_all_params,
    )
except ImportError:
    skip_api_executor = True

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


_log_lock = threading.Lock()


def log(
    msg, formatter=None, context=None, real_bad=False, real_good=False, processes=None
):
    # Grab a lock to prevent interleaved output
    with _log_lock:
        if processes is None:
            processes = []
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
        if processes:
            click.echo("STDOUT follows:")
            for p in processes:
                click.echo(p.stdout, nl=False)
            click.echo("STDERR follows:")
            for p in processes:
                click.echo(p.stderr, nl=False)


def run_test(formatter, context, debug, checks, env_base, executor):
    def run_cmd(mode, args=None):
        cmd = [context["python"], "-B", "test_flow.py"]
        cmd.extend(context["top_options"])
        cmd.append(mode)
        if args:
            cmd.extend(args)
        cmd.extend(("--run-id-file", "run-id"))
        cmd.extend(context["run_options"])
        return cmd

    def construct_arg_dict(params_opts, cli_options):
        result_dict = {}
        has_value = False
        secondary_supplied = False

        for arg in cli_options:
            if "=" in arg:
                given_opt, val = arg.split("=", 1)
                has_value = True
            else:
                given_opt = arg

            for key, each_param in params_opts.items():
                py_type = click_to_python_types[type(each_param.type)]
                if given_opt in each_param.opts:
                    secondary_supplied = False
                elif given_opt in each_param.secondary_opts:
                    secondary_supplied = True
                else:
                    continue

                if has_value:
                    value = val
                else:
                    if secondary_supplied:
                        value = False
                    else:
                        value = True

                if each_param.multiple:
                    if key not in result_dict:
                        result_dict[key] = [py_type(value)]
                    else:
                        result_dict[key].append(py_type(value))
                else:
                    result_dict[key] = py_type(value)

            has_value = False
            secondary_supplied = False

        return result_dict

    def construct_arg_dicts_from_click_api():
        _, _, param_opts, _, _ = extract_all_params(start)
        top_level_options = context["top_options"]
        top_level_dict = construct_arg_dict(param_opts, top_level_options)

        _, _, param_opts, _, _ = extract_all_params(run)
        run_level_options = context["run_options"]
        run_level_dict = construct_arg_dict(param_opts, run_level_options)
        run_level_dict["run_id_file"] = "run-id"

        return top_level_dict, run_level_dict

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

        shutil.copytree(
            os.path.join(cwd, "metaflow_test"), os.path.join(tempdir, "metaflow_test")
        )

        # Copy files required by the test
        for file in formatter.copy_files:
            shutil.copy2(os.path.join(cwd, "tests", file), os.path.join(tempdir, file))

        path = os.path.join(tempdir, "test_flow.py")

        original_env = os.environ.copy()
        try:
            # allow passenv = USER in tox.ini to work..
            env = {"USER": original_env.get("USER")}
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
                    "LANG": "en_US.UTF-8",
                    "LC_ALL": "en_US.UTF-8",
                    "PATH": os.environ.get("PATH", "."),
                    "PYTHONIOENCODING": "utf_8",
                    "PYTHONPATH": "%s:%s" % (package, pythonpath),
                }
            )

            os.environ.clear()
            os.environ.update(env)

            called_processes = []
            if "pre_command" in context:
                if context["pre_command"].get("metaflow_command"):
                    cmd = [context["python"], "test_flow.py"]
                    cmd.extend(context["top_options"])
                    cmd.extend(context["pre_command"]["command"])
                else:
                    cmd = context["pre_command"]["command"]
                called_processes.append(
                    subprocess.run(
                        cmd,
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                )
                if called_processes[-1].returncode and not context["pre_command"].get(
                    "ignore_errors", False
                ):
                    log(
                        "pre-command failed",
                        formatter,
                        context,
                        processes=called_processes,
                    )
                    return called_processes[-1].returncode, path

            # run flow
            if executor == "cli":
                called_processes.append(
                    subprocess.run(
                        run_cmd("run"),
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                )
            elif executor == "api":
                top_level_dict, run_level_dict = construct_arg_dicts_from_click_api()
                runner = Runner(
                    "test_flow.py", show_output=False, env=env, **top_level_dict
                )
                result = runner.run(**run_level_dict)
                with open(
                    result.command_obj.log_files["stdout"], encoding="utf-8"
                ) as f:
                    stdout = f.read()
                with open(
                    result.command_obj.log_files["stderr"], encoding="utf-8"
                ) as f:
                    stderr = f.read()
                called_processes.append(
                    subprocess.CompletedProcess(
                        result.command_obj.command,
                        result.command_obj.process.returncode,
                        stdout,
                        stderr,
                    )
                )

            if called_processes[-1].returncode:
                if formatter.should_fail:
                    log("Flow failed as expected.")
                elif formatter.should_resume:
                    log("Resuming flow as expected", formatter, context)
                    if executor == "cli":
                        called_processes.append(
                            subprocess.run(
                                run_cmd(
                                    "resume",
                                    (
                                        [formatter.resume_step]
                                        if formatter.resume_step
                                        else []
                                    ),
                                ),
                                env=env,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                check=False,
                            )
                        )
                    elif executor == "api":
                        _, resume_level_dict = construct_arg_dicts_from_click_api()
                        if formatter.resume_step:
                            resume_level_dict["step_to_rerun"] = formatter.resume_step
                        result = runner.resume(**resume_level_dict)
                        # NOTE: This will include both logs from the original run and resume
                        # so we will remove the last process
                        with open(
                            result.command_obj.log_files["stdout"], encoding="utf-8"
                        ) as f:
                            stdout = f.read()
                        with open(
                            result.command_obj.log_files["stderr"], encoding="utf-8"
                        ) as f:
                            stderr = f.read()
                        called_processes[-1] = subprocess.CompletedProcess(
                            result.command_obj.command,
                            result.command_obj.process.returncode,
                            stdout,
                            stderr,
                        )
                else:
                    log("flow failed", formatter, context, processes=called_processes)
                    return called_processes[-1].returncode, path
            elif formatter.should_fail:
                log(
                    "The flow should have failed but it didn't. Error!",
                    formatter,
                    context,
                    processes=called_processes,
                )
                return 1, path

            # check results
            run_id = open("run-id").read()
            ret = 0
            for check_name in context["checks"]:
                check = checks[check_name]
                python = check["python"]
                cmd = [python, "check_flow.py", check["class"], run_id]
                cmd.extend(context["top_options"])
                called_processes.append(
                    subprocess.run(
                        cmd,
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                )
                if called_processes[-1].returncode:
                    log(
                        "checker '%s' says that results failed" % check_name,
                        formatter,
                        context,
                        processes=called_processes,
                    )
                    ret = called_processes[-1].returncode
                else:
                    log(
                        "checker '%s' says that results are ok" % check_name,
                        formatter,
                        context,
                    )
        finally:
            os.environ.clear()
            os.environ.update(original_env)

        return ret, path
    finally:
        os.chdir(cwd)
        if not debug:
            shutil.rmtree(tempdir)


def run_all(ok_tests, ok_contexts, ok_graphs, debug, num_parallel, inherit_env):
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
                run_test_cases((test, ok_contexts, ok_graphs, debug, base_env))
            )
    else:
        args = [(test, ok_contexts, ok_graphs, debug, base_env) for test in tests]
        for fail in Pool(num_parallel).imap_unordered(run_test_cases, args):
            failed.extend(fail)
    return failed


def run_test_cases(args):
    test, ok_contexts, ok_graphs, debug, base_env = args
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

                for executor in context["executors"]:
                    if executor == "api" and skip_api_executor is True:
                        continue
                    log(
                        "running [using %s executor]" % executor,
                        formatter,
                        context,
                    )
                    ret, path = run_test(
                        formatter,
                        context,
                        debug,
                        contexts["checks"],
                        base_env,
                        executor,
                    )

                    if ret:
                        tstid = "%s in context %s [using %s executor]" % (
                            formatter,
                            context["name"],
                            executor,
                        )
                        failed.append((tstid, path))
                        log(
                            "failed [using %s executor]" % executor,
                            formatter,
                            context,
                            real_bad=True,
                        )
                        if debug:
                            return failed
                    else:
                        log(
                            "success [using %s executor]" % executor,
                            formatter,
                            context,
                            real_good=True,
                        )
        else:
            log("not a valid combination. Skipped.", formatter)
    return failed


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
    help="A comma-separated list of tests to include (default: all).",
)
@click.option(
    "--graphs",
    default="",
    type=str,
    help="A comma-separated list of graphs to include (default: all).",
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
    num_parallel=None,
    debug=False,
    inherit_env=False,
):
    parse = lambda x: {t.lower() for t in x.split(",") if t}

    failed = run_all(
        parse(tests),
        parse(contexts),
        parse(graphs),
        debug,
        num_parallel,
        inherit_env,
    )

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


if __name__ == "__main__":
    cli()
