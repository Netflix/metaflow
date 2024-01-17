import importlib
import os
import subprocess
import sys
import tempfile

from typing import Any, Optional, Tuple

from metaflow._vendor import click

from . import develop
from .stub_generator import StubGenerator


@develop.group(short_help="Stubs management")
@click.pass_context
def stubs(ctx: Any):
    """
    Stubs provide type hints and documentation hints to IDEs and are typically provided
    inline with the code where a static analyzer can pick them up. In Metaflow's case,
    however, proper stubs rely on dynamic behavior (ie: the decorators are
    generated at runtime). This makes it necessary to have separate stub files.

    This CLI provides utilities to check and generate stubs for your current Metaflow
    installation.
    """
    pass


@stubs.command(short_help="Check validity of stubs")
@click.pass_context
def check(ctx: Any):
    """
    Checks the currently installed stubs (if they exist) and validates that they
    match the currently installed version of Metaflow.
    """

    res = internal_check(ctx.obj.quiet)
    if ctx.obj.quiet:
        ctx.obj.echo_always(res)
    else:
        ctx.obj.echo(res)


@stubs.command(short_help="Generate Python stubs")
@click.pass_context
@click.option(
    "--force/--no-force",
    default=False,
    show_default=True,
    help="Force installation of stubs even if they exist and are valid",
)
def install(ctx: Any, force: bool):
    """
    Generates the Python stubs for Metaflow considering the installed version of
    Metaflow. The stubs will be generated if they do not exist or do not match the
    current version of Metaflow and installed in the Python environment.
    """
    try:
        import build
    except ImportError:
        raise RuntimeError(
            "Installating stubs requires 'build' -- " "please install it and try again"
        )

    if internal_check(True) == "valid" and not force:
        if ctx.obj.quiet:
            ctx.obj.echo_always("already_installed")
        else:
            ctx.obj.echo(
                "Stubs are already installed and valid -- use --force to reinstall"
            )
        return
    mf_version, extensions = get_mf_version()
    with tempfile.TemporaryDirectory() as tmp_dir:
        with open(os.path.join(tmp_dir, "setup.py"), "w") as f:
            f.write(
                f"""
from setuptools import setup
setup(
    include_package_data=True,
    name="metaflow-stubs",
    version="{mf_version}",
    description="Metaflow: More Data Science, Less Engineering",
    author="Metaflow Developers",
    author_email="help@metaflow.org",
    license="Apache Software License",
    packages=["metaflow-stubs"],
    package_data={{"metaflow-stubs": ["generated_for.txt", "py.typed", "**/*.pyi"]}},
    install_requires=["metaflow=={mf_version}"],
    python_requires=">=3.5.2",
)
                """
            )

        StubGenerator(os.path.join(tmp_dir, "metaflow-stubs")).write_out()

        subprocess.check_call(
            [sys.executable, "-m", "build", "--wheel"],
            cwd=tmp_dir,
            stderr=subprocess.DEVNULL if ctx.obj.quiet else None,
            stdout=subprocess.DEVNULL if ctx.obj.quiet else None,
        )
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--force-reinstall",
                "--no-deps",
                "--no-index",
                "--find-links",
                os.path.join(tmp_dir, "dist"),
                "metaflow-stubs",
            ],
            cwd=tmp_dir,
            stderr=subprocess.DEVNULL if ctx.obj.quiet else None,
            stdout=subprocess.DEVNULL if ctx.obj.quiet else None,
        )
    if ctx.obj.quiet:
        ctx.obj.echo_always("installed")
    else:
        ctx.obj.echo("Stubs successfully installed")


def split_version(vers: str) -> Tuple[str, Optional[str]]:
    vers_split = vers.split("+", 1)
    if len(vers_split) == 1:
        return vers_split[0], None
    return vers_split[0], vers_split[1]


def get_mf_version() -> Tuple[str, Optional[str]]:
    from metaflow.metaflow_version import get_version

    return split_version(get_version())


def get_stubs_version(stubs_root_path: Optional[str]) -> Tuple[str, Optional[str]]:
    if stubs_root_path is None:
        # The stubs are NOT an integrated part of metaflow
        return None, None
    if not os.path.isfile(os.path.join(stubs_root_path, "generated_for.txt")):
        return None, None

    with open(
        os.path.join(stubs_root_path, "generated_for.txt"), "r", encoding="utf-8"
    ) as f:
        return split_version(f.read().strip().split(" ", 1)[0])


def internal_check(quiet: bool) -> str:
    mf_version = get_mf_version()
    try:
        m = importlib.import_module("metaflow-stubs")
        stubs_path = list(m.__path__)
        if len(stubs_path) == 1:
            stubs_path = stubs_path[0]
        else:
            stubs_path = None  # Should not be a real namespace package. It has a .pyi
            # so will look like a namespace package but it is not.

    except ImportError:
        stubs_path = None
    stub_version = get_stubs_version(stubs_path)

    if stub_version == (None, None):
        if quiet:
            return "invalid"
        else:
            return "The stubs package is invalid or not installed"
    elif stub_version != mf_version:
        if quiet:
            return "invalid"
        else:
            return (
                "The stubs package was generated for Metaflow version %s%s "
                "but you have Metaflow version %s%s installed."
                % (
                    stub_version[0],
                    " and extensions %s" % stub_version[1] if stub_version[1] else "",
                    mf_version[0],
                    " and extensions %s" % mf_version[1] if mf_version[1] else "",
                )
            )
    else:
        if quiet:
            return "valid"
        else:
            return "The stubs package installed matches your current Metaflow version"
