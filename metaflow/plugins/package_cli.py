import click
from metaflow.package import MetaflowPackage

@click.group()
def cli():
    pass

@cli.group(help='Commands related to code packages.')
@click.pass_obj
def package(obj):
    # Prepare the package before any of the sub-commands are invoked.
    obj.package = MetaflowPackage(obj.flow, obj.environment, obj.logger, obj.package_suffixes)

@package.command(help='Output information about the current code package.')
@click.pass_obj
def info(obj):
    obj.echo('Status of the current working directory:', fg='magenta', bold=False)
    obj.echo_always('Hash: *%s*' % obj.package.sha,
                    highlight='green',
                    highlight_bold=False)
    obj.echo_always('Package size: *%d* KB' % (len(obj.package.blob) / 1024),
                    highlight='green',
                    highlight_bold=False)
    num = sum(1 for _ in obj.package.path_tuples())
    obj.echo_always('Number of files: *%d*' % num,
                    highlight='green',
                    highlight_bold=False)

@package.command(help='List files included in the code package.')
@click.pass_obj
def list(obj):
    obj.echo('Files included in the code package '
             '(change with --package-suffixes):',
             fg='magenta',
             bold=False)
    obj.echo_always('\n'.join(path for path, _ in obj.package.path_tuples()))

@package.command(help='Save the current code package in a tar file')
@click.argument('path')
@click.pass_obj
def save(obj, path):
    with open(path, 'wb') as f:
        f.write(obj.package.blob)
    obj.echo('Code package saved in *%s*.' % path,
             fg='magenta',
             bold=False)
