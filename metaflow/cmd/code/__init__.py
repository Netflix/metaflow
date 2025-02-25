import os
import shutil
import sys
from subprocess import PIPE, CompletedProcess, run
from tempfile import TemporaryDirectory
from typing import Any, Callable, List, Mapping, Optional, cast

from metaflow import Run
from metaflow._vendor import click
from metaflow.cli import echo_always


@click.group()
def cli():
    pass


@cli.group(help="Metaflow code commands")
def code():
    pass


def echo(line: str) -> None:
    echo_always(line, err=True, fg="magenta")


def extract_code_package(runspec: str) -> TemporaryDirectory:
    try:
        mf_run = Run(runspec, _namespace_check=False)
        echo(f"✅  Run *{runspec}* found, downloading code..")
    except Exception as e:
        echo(f"❌  Run **{runspec}** not found")
        raise e

    if mf_run.code is None:
        echo(
            f"❌  Run **{runspec}** doesn't have a code package. Maybe it's a local run?"
        )
        raise RuntimeError("no code package found")

    return mf_run.code.extract()


def perform_diff(
    source_dir: str,
    target_dir: Optional[str] = None,
    output: bool = False,
    **kwargs: Mapping[str, Any],
) -> Optional[List[str]]:
    if target_dir is None:
        target_dir = os.getcwd()

    diffs = []
    for dirpath, dirnames, filenames in os.walk(source_dir):
        for fname in filenames:
            # NOTE: the paths below need to be set up carefully
            # for the `patch` command to work. Better not to touch
            # the directories below. If you must, test that patches
            # work after your changes.
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
                    if not output:
                        echo(f"✅  {target_file} is identical, skipping")
                    continue

                if output:
                    diffs.append(result.stdout)
                else:
                    run(["less", "-R"], input=result.stdout, text=True)
            else:
                if not output:
                    echo(f"❗  {target_file} not in the target directory, skipping")
    return diffs if output else None


def run_op(
    runspec: str, op: Callable[..., Optional[List[str]]], **op_args: Mapping[str, Any]
) -> Optional[List[str]]:
    tmp = None
    try:
        tmp = extract_code_package(runspec)
        return op(tmp.name, **op_args)
    finally:
        if tmp and os.path.exists(tmp.name):
            shutil.rmtree(tmp.name)


def run_op_diff_runs(
    source_run: str, target_run: str, **op_args: Mapping[str, Any]
) -> Optional[List[str]]:
    source_tmp = None
    target_tmp = None
    try:
        source_tmp = extract_code_package(source_run)
        target_tmp = extract_code_package(target_run)
        return perform_diff(source_tmp.name, target_tmp.name)
    finally:
        for d in [source_tmp, target_tmp]:
            if d and os.path.exists(d.name):
                shutil.rmtree(d.name)


def op_diff(tmpdir: str, **kwargs: Mapping[str, Any]) -> Optional[List[str]]:
    kwargs_dict = dict(kwargs)
    target_dir = cast(Optional[str], kwargs_dict.pop("target_dir", None))
    output: bool = bool(kwargs_dict.pop("output", False))
    op_args: Mapping[str, Any] = {**kwargs_dict}
    return perform_diff(tmpdir, target_dir=target_dir, output=output, **op_args)


def op_pull(tmpdir: str, dst: str, **kwargs: Mapping[str, Any]) -> None:
    if os.path.exists(dst):
        echo(f"❌  Directory *{dst}* already exists")
    else:
        shutil.move(tmpdir, dst)
        echo(f"Code downloaded to *{dst}*")


def op_patch(tmpdir: str, dst: str, **kwargs: Mapping[str, Any]) -> None:
    diffs = perform_diff(tmpdir, output=True) or []
    with open(dst, "w", encoding="utf-8") as f:
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


@code.command()
@click.argument("metaflow-run")
def diff(metaflow_run: str, **kwargs: Mapping[str, Any]) -> None:
    """
    Do a 'git diff' of the current directory and a Metaflow run.
    """
    _ = run_op(metaflow_run, op_diff, **kwargs)


@code.command()
@click.argument("source_run")
@click.argument("target_run")
def diff_runs(source_run: str, target_run: str, **kwargs: Mapping[str, Any]) -> None:
    """
    Do a 'git diff' between two Metaflow runs.
    """
    _ = run_op_diff_runs(source_run, target_run, **kwargs)


@code.command()
@click.argument("metaflow_run")
@click.option(
    "--dir", help="Destination directory (default: {runspec}_code)", default=None
)
def pull(
    metaflow_run: str, dir: Optional[str] = None, **kwargs: Mapping[str, Any]
) -> None:
    """
    Pull the code of a Metaflow run.
    """
    if dir is None:
        dir = metaflow_run.lower().replace("/", "_") + "_code"
    op_args: Mapping[str, Any] = {**kwargs, "dst": dir}
    run_op(metaflow_run, op_pull, **op_args)


@code.command()
@click.argument("metaflow_run")
@click.option("--file", help="Patch file name (default: {runspec}.patch", default=None)
def patch(
    metaflow_run: str, file: Optional[str] = None, **kwargs: Mapping[str, Any]
) -> None:
    """
    Create a patch file for the current dir with a Metaflow run.
    """
    if file is None:
        file = metaflow_run.lower().replace("/", "_") + ".patch"
    if confirm_overwrite(file):
        op_args: Mapping[str, Any] = {**kwargs, "dst": file}
        run_op(metaflow_run, op_patch, **op_args)


def confirm_overwrite(file):
    if os.path.exists(file):
        response = (
            input(f"{file} already exists. Overwrite? (Y/n) [default: Y]: ")
            .strip()
            .lower()
        )
        return response in ("y", "")
    return True
