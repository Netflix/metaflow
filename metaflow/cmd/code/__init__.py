#!/usr/bin/env python
import os
import shutil
import sys
from subprocess import PIPE, run, CompletedProcess
from tempfile import TemporaryDirectory
from typing import List, Optional, Callable, Dict, Any

from metaflow._vendor import click
from metaflow import Run
from metaflow.cli import echo_always

EXCLUSIONS = [
    "metaflow/",
    "metaflow_extensions/",
    "INFO",
    "CONFIG_PARAMETERS",
    "conda.manifest",
]


@click.group()
def cli():
    pass


@cli.group(help="Metaflow code commands")
def code():
    pass


def echo(line: str) -> None:
    echo_always(line, err=True, fg="magenta")


def extract_code_package(runspec: str, exclusions: List[str]) -> TemporaryDirectory:
    try:
        run = Run(runspec, _namespace_check=False)
        echo(f"✅  Run *{runspec}* found, downloading code..")
    except:
        echo(f"❌  Run **{runspec}** not found")
        sys.exit(1)

    if run.code is None:
        echo(
            f"❌  Run **{runspec}** doesn't have a code package. Maybe it's a local run?"
        )
        sys.exit(1)

    tar = run.code.tarball
    members = [
        m
        for m in tar.getmembers()
        if not any(
            (x.endswith("/") and m.name.startswith(x)) or (m.name == x)
            for x in exclusions
        )
    ]

    tmp = TemporaryDirectory()
    tar.extractall(tmp.name, members)
    return tmp


def perform_diff(
    source_dir: str, target_dir: Optional[str] = None, output: bool = False
) -> Optional[List[str]]:
    if target_dir is None:
        target_dir = os.getcwd()

    diffs = []
    for dirpath, dirnames, filenames in os.walk(source_dir):
        for fname in filenames:
            # NOTE: the paths below need to be set up carefully
            # for the `patch` command to work. Better not to touch
            # the directories below. If you must, test that patches
            # work after you changes.
            #
            # target_file is the git repo in the current working directory
            rel = os.path.relpath(dirpath, source_dir)
            target_file = os.path.join(rel, fname)
            # source_file is the run file loaded in a tmp directory
            source_file = os.path.join(dirpath, fname)

            if sys.stdout.isatty() and not output:
                color = ["--color"]
            else:
                color = ["--no-color"]

            if os.path.exists(os.path.join(target_dir, target_file)):
                cmd = (
                    ["git", "diff", "--no-index", "--exit-code"]
                    + color
                    + [
                        target_file,
                        source_file,
                    ]
                )
                result: CompletedProcess = run(
                    cmd, text=True, stdout=PIPE, cwd=target_dir
                )
                if result.returncode == 0:
                    echo(f"✅  {target_file} is identical, skipping")
                    continue

                if output:
                    diffs.append(result.stdout)
                else:
                    run(["less", "-R"], input=result.stdout, text=True)
            else:
                echo(f"❗  {target_file} not in the target directory, skipping")
    return diffs if output else None


def run_op(runspec: str, op: Callable[..., None], op_args: Dict[str, Any]) -> None:
    tmp = None
    try:
        tmp = extract_code_package(runspec, EXCLUSIONS)
        op(tmp.name, **op_args)
    finally:
        if tmp and os.path.exists(tmp.name):
            shutil.rmtree(tmp.name)


def run_op_diff_runs(source_run: str, target_run: str) -> None:
    source_tmp = None
    target_tmp = None
    try:
        source_tmp = extract_code_package(source_run, EXCLUSIONS)
        target_tmp = extract_code_package(target_run, EXCLUSIONS)
        perform_diff(source_tmp.name, target_tmp.name)
    finally:
        if source_tmp and os.path.exists(source_tmp.name):
            shutil.rmtree(source_tmp.name)
        if target_tmp and os.path.exists(target_tmp.name):
            shutil.rmtree(target_tmp.name)


def op_diff(tmpdir: str) -> None:
    perform_diff(tmpdir)


def op_pull(tmpdir: str, dst: str) -> None:
    if os.path.exists(dst):
        echo(f"❌  Directory *{dst}* already exists")
    else:
        shutil.move(tmpdir, dst)
        echo(f"Code downloaded to *{dst}*")


def op_patch(tmpdir: str, dst: str) -> None:
    diffs = perform_diff(tmpdir, output=True) or []
    with open(dst, "w") as f:
        for out in diffs:
            out = out.replace(tmpdir, "/.")
            out = out.replace("+++ b/./", "+++ b/")
            out = out.replace("--- b/./", "--- b/")
            out = out.replace("--- a/./", "--- a/")
            out = out.replace("+++ a/./", "+++ a/")
            f.write(out)
    echo(f"Patch saved in *{dst}*")
    path = run(
        ["git", "rev-parse", "--show-prefix"], text=True, stdout=PIPE
    ).stdout.strip()
    if path:
        diropt = f" --directory={path.rstrip('/')}"
    else:
        diropt = ""
    echo("Apply the patch by running:")
    echo_always(
        f"git apply --verbose{diropt} {dst}", highlight=True, bold=True, err=True
    )


@code.group()
@click.argument("metaflow_run")
def diff(metaflow_run: str) -> None:
    """
    Do a 'git diff' of the current directory and a Metaflow run.
    """
    run_op(metaflow_run, op_diff, {})


@code.group()
@click.argument("source_run")
@click.argument("target_run")
def diff_runs(source_run: str, target_run: str) -> None:
    """
    Do a 'git diff' between two Metaflow runs.
    """
    run_op_diff_runs(source_run, target_run)


@code.group()
@click.argument("metaflow_run")
@click.option(
    "--dir", help="Destination directory (default: {runspec}_code)", default=None
)
def pull(metaflow_run: str, dir: Optional[str] = None) -> None:
    """
    Pull the code of a Metaflow run.
    """
    if dir is None:
        dir = metaflow_run.lower().replace("/", "_") + "_code"
    run_op(metaflow_run, op_pull, {"dst": dir})


@code.group()
@click.argument("metaflow_run")
@click.option("--file", help="Patch file name (default: {runspec}.patch", default=None)
def patch(metaflow_run: str, file: Optional[str] = None) -> None:
    """
    Create a patch file for the current dir with a Metaflow run.
    """
    if file is None:
        file = metaflow_run.lower().replace("/", "_") + ".patch"
    run_op(metaflow_run, op_patch, {"dst": file})
