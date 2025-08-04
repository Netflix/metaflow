import importlib
import os
import subprocess
import sys
import tempfile

from typing import Any, List, Optional, Tuple

from metaflow._vendor import click

from . import develop
from .stub_generator import StubGenerator

_py_ver = sys.version_info[:2]

if _py_ver >= (3, 8):
    from importlib import metadata
elif _py_ver >= (3, 7):
    from metaflow._vendor.v3_7 import importlib_metadata as metadata
else:
    from metaflow._vendor.v3_6 import importlib_metadata as metadata


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


@stubs.command(short_help="Check validity of stubs")
@click.pass_context
def check(ctx: Any):
    """
    Checks the currently installed stubs (if they exist) and validates that they
    match the currently installed version of Metaflow.
    """

    dist_packages, paths = get_packages_for_stubs()

    if len(dist_packages) + len(paths) == 0:
        return print_status(ctx, "no package provides `metaflow-stubs`", False)
    if len(dist_packages) + len(paths) == 1:
        if dist_packages:
            return print_status(
                ctx, *internal_check(dist_packages[0][1], dist_packages[0][0])
            )
        return print_status(ctx, *internal_check(paths[0]))

    pkg_names = None
    pkg_paths = None
    if dist_packages:
        pkg_names = " packages " + ", ".join([p[0] for p in dist_packages])
    if paths:
        pkg_paths = "directories at " + ", ".join(paths)
    return print_status(
        ctx,
        "metaflow-stubs is provided multiple times by%s %s%s"
        % (
            pkg_names if pkg_names else "",
            "and " if pkg_names and pkg_paths else "",
            pkg_paths if pkg_paths else "",
        ),
        False,
    )


@stubs.command(short_help="Remove all packages providing metaflow stubs")
@click.pass_context
def remove(ctx: Any):
    """
    Removes all packages that provide metaflow-stubs from the current Python environment.
    """
    dist_packages, paths = get_packages_for_stubs()
    if len(dist_packages) + len(paths) == 0:
        if ctx.obj.quiet:
            ctx.obj.echo_always("not_installed")
        else:
            ctx.obj.echo("No packages provide `metaflow-stubs")

    if paths:
        raise RuntimeError(
            "Cannot remove stubs when metaflow-stubs is already provided by a directory. "
            "Please remove the following and try again: %s" % ", ".join(paths)
        )

    pkgs_to_remove = [p[0] for p in dist_packages]
    ctx.obj.echo(
        "Uninstalling existing packages providing metaflow-stubs: %s"
        % ", ".join(pkgs_to_remove)
    )

    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "uninstall",
            "-y",
            *pkgs_to_remove,
        ],
        stderr=subprocess.DEVNULL if ctx.obj.quiet else None,
        stdout=subprocess.DEVNULL if ctx.obj.quiet else None,
    )
    if ctx.obj.quiet:
        ctx.obj.echo_always("ok")
    else:
        ctx.obj.echo("All packages providing metaflow-stubs have been removed.")


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
            "Installing stubs requires 'build' -- please install it and try again"
        )

    dist_packages, paths = get_packages_for_stubs()
    if paths:
        raise RuntimeError(
            "Cannot install stubs when metaflow-stubs is already provided by a directory. "
            "Please remove the following and try again: %s" % ", ".join(paths)
        )

    if len(dist_packages) == 1:
        if internal_check(dist_packages[0][1])[1] == True and not force:
            if ctx.obj.quiet:
                ctx.obj.echo_always("already_installed")
            else:
                ctx.obj.echo(
                    "Metaflow stubs are already installed and valid -- use --force to reinstall"
                )
            return
    mf_version, _ = get_mf_version(True)
    with tempfile.TemporaryDirectory() as tmp_dir:
        with open(os.path.join(tmp_dir, "setup.py"), "w") as f:
            f.write(
                f"""
from setuptools import setup, find_namespace_packages
setup(
    include_package_data=True,
    name="metaflow-stubs",
    version="{mf_version}",
    description="Metaflow: More Data Science, Less Engineering",
    author="Metaflow Developers",
    author_email="help@metaflow.org",
    license="Apache Software License",
    packages=find_namespace_packages(),
    package_data={{"metaflow-stubs": ["generated_for.txt", "py.typed", "**/*.pyi"]}},
    install_requires=["metaflow=={mf_version}"],
    python_requires=">=3.6.1",
)
                """
            )
        with open(os.path.join(tmp_dir, "MANIFEST.in"), "w") as f:
            f.write(
                """
include metaflow-stubs/generated_for.txt
include metaflow-stubs/py.typed
global-include *.pyi
                """
            )

        StubGenerator(os.path.join(tmp_dir, "metaflow-stubs")).write_out()

        subprocess.check_call(
            [sys.executable, "-m", "build", "--wheel"],
            cwd=tmp_dir,
            stderr=subprocess.DEVNULL if ctx.obj.quiet else None,
            stdout=subprocess.DEVNULL if ctx.obj.quiet else None,
        )

        if dist_packages:
            # We need to uninstall all the other packages first
            pkgs_to_remove = [p[0] for p in dist_packages]
            ctx.obj.echo(
                "Uninstalling existing packages providing metaflow-stubs: %s"
                % ", ".join(pkgs_to_remove)
            )

            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "uninstall",
                    "-y",
                    *pkgs_to_remove,
                ],
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
        ctx.obj.echo("Metaflow stubs successfully installed")


def split_version(vers: str) -> Tuple[str, Optional[str]]:
    vers_split = vers.split("+", 1)
    if len(vers_split) == 1:
        return vers_split[0], None
    return vers_split[0], vers_split[1]


def get_mf_version(public: bool = False) -> Tuple[str, Optional[str]]:
    from metaflow.metaflow_version import get_version

    return split_version(get_version(public))


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


def internal_check(stubs_path: str, pkg_name: Optional[str] = None) -> Tuple[str, bool]:
    mf_version = get_mf_version()
    stub_version = get_stubs_version(stubs_path)

    if stub_version == (None, None):
        return "the installed stubs package does not seem valid", False
    elif stub_version != mf_version:
        return (
            "the stubs package was generated for Metaflow version %s%s "
            "but you have Metaflow version %s%s installed."
            % (
                stub_version[0],
                " and extensions %s" % stub_version[1] if stub_version[1] else "",
                mf_version[0],
                " and extensions %s" % mf_version[1] if mf_version[1] else "",
            ),
            False,
        )
    return (
        "the stubs package %s matches your current Metaflow version"
        % (pkg_name if pkg_name else "installed at '%s'" % stubs_path),
        True,
    )


def get_packages_for_stubs() -> Tuple[List[Tuple[str, str]], List[str]]:
    """
    Gets the packages that provide metaflow-stubs.

    This returns two lists:
      - the first list contains tuples of package names and root path for the package
      - the second list contains all non package names (ie: things in path for example)

    Returns
    -------
    Tuple[List[Tuple[str, str]], Optional[List[Tuple[str, str]]]]
        Packages or paths providing metaflow-stubs
    """
    try:
        m = importlib.import_module("metaflow-stubs")
        all_paths = set(m.__path__)
    except:
        return [], []

    dist_list = []

    # We check the type because if the user has multiple importlib metadata, for
    # some reason it shows up multiple times.
    interesting_dists = [
        d
        for d in metadata.distributions()
        if any(
            [
                p == "metaflow-stubs"
                for p in (d.read_text("top_level.txt") or "").split()
            ]
        )
        and isinstance(d, metadata.PathDistribution)
    ]

    for dist in interesting_dists:
        # This is a package we care about
        root_path = dist.locate_file("metaflow-stubs").as_posix()
        dist_list.append((dist.metadata["Name"], root_path))
        all_paths.discard(root_path)
    return dist_list, list(all_paths)


def print_status(ctx: click.Context, msg: str, valid: bool):
    if ctx.obj.quiet:
        ctx.obj.echo_always("valid" if valid else "invalid")
    else:
        ctx.obj.echo("Metaflow stubs are ", nl=False)
        if valid:
            ctx.obj.echo("valid", fg="green", nl=False)
        else:
            ctx.obj.echo("invalid", fg="red", nl=False)
        ctx.obj.echo(": " + msg)
    return
