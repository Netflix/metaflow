from metaflow._vendor import click
from metaflow.doctor import DoctorStatus, format_diagnostic_report, run_diagnostics


@click.group()
def cli():
    pass


@cli.command(name="doctor", help="Diagnose the local Metaflow environment and configuration.")
def doctor():
    results = run_diagnostics()
    click.echo(format_diagnostic_report(results))

    if any(check.status == DoctorStatus.ERROR for check in results):
        raise click.ClickException(
            "Metaflow detected critical configuration problems. Fix the issues above and run 'metaflow doctor' again."
        )
