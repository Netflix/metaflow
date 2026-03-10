import os
import shutil
import subprocess
import sys

from metaflow._vendor import click
from metaflow.client import Run, namespace
from metaflow.cli import echo_always


def _echo(line, **kwargs):
    echo_always(line, err=True, fg="magenta", **kwargs)


def _echo_error(line, **kwargs):
    echo_always(line, err=True, fg="red", bold=True, **kwargs)


@click.group()
def cli():
    pass


@cli.command(
    help="Resume a previous run without needing the original source code.\n\n"
    "Downloads the code package from FLOW_NAME/RUN_ID and re-executes\n"
    "the flow's resume command from the specified step (or from the\n"
    "first failed step by default).\n\n"
    "Examples:\n\n"
    "  metaflow resume MyFlow/12345\n\n"
    "  metaflow resume MyFlow/12345 --step train\n\n"
    "  metaflow resume MyFlow/12345 --step train --max-workers 4",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("run_pathspec")
@click.option(
    "--step",
    "step_name",
    default=None,
    help="Step to resume from. If not specified, resumes from the first failed step.",
)
@click.pass_context
def resume(ctx, run_pathspec, step_name):
    """Resume a previous run using its stored code package."""

    # Disable the global namespace filter so we can find any run
    namespace(None)

    # Parse and validate the run pathspec
    parts = run_pathspec.strip("/").split("/")
    if len(parts) != 2:
        raise click.BadParameter(
            "RUN_PATHSPEC must be in the form FLOW_NAME/RUN_ID (e.g. MyFlow/12345)",
            param_hint="'run_pathspec'",
        )
    flow_name, run_id = parts

    # Look up the run
    _echo("Looking up run *%s/%s*..." % (flow_name, run_id))
    try:
        mf_run = Run("%s/%s" % (flow_name, run_id), _namespace_check=False)
    except Exception as e:
        _echo_error("Could not find run *%s/%s*: %s" % (flow_name, run_id, e))
        raise SystemExit(1)

    # Get the code package
    code = mf_run.code
    if code is None:
        _echo_error(
            "Run *%s/%s* does not have a code package. "
            "Code packages are only available for runs that executed at least "
            "one step remotely." % (flow_name, run_id)
        )
        raise SystemExit(1)

    script_name = code.script_name
    _echo("Found code package with script *%s*" % script_name)

    # Extract code to a temporary directory
    _echo("Extracting code package...")
    tmp_dir = code.extract()
    tmp_path = tmp_dir.name

    try:
        script_path = os.path.join(tmp_path, script_name)
        if not os.path.isfile(script_path):
            _echo_error(
                "Script *%s* not found in the extracted code package." % script_name
            )
            raise SystemExit(1)

        # Build the resume command
        cmd = [sys.executable, script_path, "resume", "--origin-run-id", run_id]

        if step_name is not None:
            cmd.append(step_name)

        # Pass through any extra arguments (e.g. --max-workers, --tag, etc.)
        cmd.extend(ctx.args)

        _echo("Resuming with: *%s*" % " ".join(cmd))
        _echo("")

        result = subprocess.run(cmd, cwd=tmp_path)
        raise SystemExit(result.returncode)
    finally:
        # Clean up the temporary directory
        if os.path.exists(tmp_path):
            shutil.rmtree(tmp_path, ignore_errors=True)
