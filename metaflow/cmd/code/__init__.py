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


@cli.group(help="Access, compare, and manage code associated with Metaflow runs.")
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
    for dirpath, dirnames, filenames in os.walk(source_dir, followlinks=True):
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
    source_run_pathspec: str, target_run_pathspec: str, **op_args: Mapping[str, Any]
) -> Optional[List[str]]:
    source_tmp = None
    target_tmp = None
    try:
        source_tmp = extract_code_package(source_run_pathspec)
        target_tmp = extract_code_package(target_run_pathspec)
        return perform_diff(source_tmp.name, target_tmp.name, **op_args)
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


def op_pull(tmpdir: str, dst: str, **op_args: Mapping[str, Any]) -> None:
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
@click.argument("run_pathspec")
def diff(run_pathspec: str, **kwargs: Mapping[str, Any]) -> None:
    """
    Do a 'git diff' of the current directory and a Metaflow run.
    """
    _ = run_op(run_pathspec, op_diff, **kwargs)


@code.command()
@click.argument("source_run_pathspec")
@click.argument("target_run_pathspec")
def diff_runs(
    source_run_pathspec: str, target_run_pathspec: str, **kwargs: Mapping[str, Any]
) -> None:
    """
    Do a 'git diff' between two Metaflow runs.
    """
    _ = run_op_diff_runs(source_run_pathspec, target_run_pathspec, **kwargs)


@code.command()
@click.argument("run_pathspec")
@click.option(
    "--dir", help="Destination directory (default: {run_pathspec}_code)", default=None
)
def pull(
    run_pathspec: str, dir: Optional[str] = None, **kwargs: Mapping[str, Any]
) -> None:
    """
    Pull the code of a Metaflow run.
    """
    if dir is None:
        dir = run_pathspec.lower().replace("/", "_") + "_code"
    op_args: Mapping[str, Any] = {**kwargs, "dst": dir}
    run_op(run_pathspec, op_pull, **op_args)


@code.command()
@click.argument("run_pathspec")
@click.option(
    "--file_path",
    help="Patch file name. If not provided, defaults to a sanitized version of RUN_PATHSPEC "
    "with slashes replaced by underscores, plus '.patch'.",
    show_default=False,
)
@click.option(
    "--overwrite", is_flag=True, help="Overwrite the patch file if it exists."
)
def patch(
    run_pathspec: str,
    file_path: Optional[str] = None,
    overwrite: bool = False,
    **kwargs: Mapping[str, Any],
) -> None:
    """
    Create a patch by comparing current dir with a Metaflow run.
    """
    if file_path is None:
        file_path = run_pathspec.lower().replace("/", "_") + ".patch"
    if os.path.exists(file_path) and not overwrite:
        echo(f"File *{file_path}* already exists. To overwrite, specify --overwrite.")
        return
    op_args: Mapping[str, Any] = {**kwargs, "dst": file_path}
    run_op(run_pathspec, op_patch, **op_args)
