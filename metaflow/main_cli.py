import click
import json
import os
import shutil

from os.path import expanduser

from metaflow.datastore.local import LocalDataStore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR, DEFAULT_METADATA


def makedirs(path):
    # This is for python2 compatibility.
    # Python3 has os.makedirs(exist_ok=True).
    try:
        os.makedirs(path)
    except OSError as x:
        if x.errno == 17:
            return
        else:
            raise

def echo_dev_null(*args, **kwargs):
    pass

def echo_always(line, **kwargs):
    click.secho(line, **kwargs)

@click.group(invoke_without_command=True)
@click.pass_context
def main(ctx):
    global echo
    echo = echo_always

    import metaflow
    echo('Metaflow ',
         fg='magenta',
         bold=True,
         nl=False)

    if ctx.invoked_subcommand is None:
        echo('(%s): ' % metaflow.__version__,
             fg='magenta',
             bold=False,
             nl=False)
    else:
        echo('(%s)\n' % metaflow.__version__,
             fg='magenta',
             bold=False)

    if ctx.invoked_subcommand is None:
        echo("More data science, less engineering\n",
             fg='magenta')

        # metaflow URL
        echo('http://docs.metaflow.org', fg='cyan', nl=False)
        echo(' - Read the documentation')

        # metaflow chat
        echo('http://chat.metaflow.org', fg='cyan', nl=False)
        echo(' - Chat with us')

        # metaflow help email
        echo('help@metaflow.org', fg='cyan', nl=False)
        echo('        - Get help by email\n')

        # print a short list of next steps.
        short_help = {'tutorials': 'Browse and access metaflow tutorials.',
                      'configure': 'Configure metaflow to access the cloud.',
                      'status': 'Display the current working tree.',
                      'help': 'Show all available commands to run.'}

        echo('Commands:', bold=False)

        for cmd, desc in short_help.items():
            echo('  metaflow {0:<10} '.format(cmd),
                 fg='cyan',
                 bold=False,
                 nl=False)

            echo('%s' % desc)

@main.command(help='Show all available commands.')
@click.pass_context
def help(ctx):
    print(ctx.parent.get_help())

@main.command(help='Show flows accessible from the current working tree.')
def status():
    from metaflow.client import get_metadata
    res = get_metadata()
    if res:
        res = res.split('@')
    else:
        raise click.ClickException('Unknown status: cannot find a Metadata provider')
    if res[0] == 'service':
        echo('Using Metadata provider at: ', nl=False)
        echo('"%s"\n' % res[1], fg='cyan')
        echo('To list available flows, type:\n')
        echo('1. python')
        echo('2. from metaflow import Metaflow')
        echo('3. list(Metaflow())')
        return

    from metaflow.client import namespace, metadata, Metaflow

    # Get the local data store path
    path = LocalDataStore.get_datastore_root_from_config(echo,
                                                         create_on_absent=False)
    # Throw an exception
    if path is None:
        raise click.ClickException("Could not find " +\
                                   click.style('"%s"' % DATASTORE_LOCAL_DIR,
                                               fg='red') +\
                                   " in the current working tree.")

    stripped_path = os.path.dirname(path)
    namespace(None)
    metadata('local@%s' % stripped_path)
    echo('Working tree found at: ', nl=False)
    echo('"%s"\n' % stripped_path, fg='cyan')
    echo('Available flows:', fg='cyan', bold=True)
    for flow in Metaflow():
        echo('* %s' % flow, fg='cyan')

@main.group(help="Browse and access the metaflow tutorial episodes.")
def tutorials():
    pass

def get_tutorials_dir():
    metaflow_dir = os.path.dirname(__file__)
    package_dir = os.path.dirname(metaflow_dir)
    tutorials_dir = os.path.join(package_dir, 'metaflow', 'tutorials')

    return tutorials_dir

def get_tutorial_metadata(tutorial_path):
    metadata = {}
    with open(os.path.join(tutorial_path, 'README.md')) as readme:
        content =  readme.read()

    paragraphs = [paragraph.strip() \
                  for paragraph \
                  in content.split('#') if paragraph]
    metadata['description'] = paragraphs[0].split('**')[1]
    header = paragraphs[0].split('\n')
    header = header[0].split(':')
    metadata['episode'] = header[0].strip()[len('Episode '):]
    metadata['title'] = header[1].strip()

    for paragraph in paragraphs[1:]:
        if paragraph.startswith('Before playing'):
            lines = '\n'.join(paragraph.split('\n')[1:])
            metadata['prereq'] = lines.replace('```', '')

        if paragraph.startswith('Showcasing'):
            lines = '\n'.join(paragraph.split('\n')[1:])
            metadata['showcase'] = lines.replace('```', '')

        if paragraph.startswith('To play'):
            lines = '\n'.join(paragraph.split('\n')[1:])
            metadata['play'] = lines.replace('```', '')

    return metadata

def get_all_episodes():
    episodes = []
    for name in sorted(os.listdir(get_tutorials_dir())):
        # Skip hidden files (like .gitignore)
        if not name.startswith('.'):
            episodes.append(name)
    return episodes

@tutorials.command(help="List the available episodes.")
def list():
    echo('Episodes:', fg='cyan', bold=True)
    for name in get_all_episodes():
        path = os.path.join(get_tutorials_dir(), name)
        metadata = get_tutorial_metadata(path)
        echo('* {0: <20} '.format(metadata['episode']),
             fg='cyan',
             nl=False)
        echo('- {0}'.format(metadata['title']))

    echo('\nTo pull the episodes, type: ')
    echo('metaflow tutorials pull', fg='cyan')

def validate_episode(episode):
    src_dir = os.path.join(get_tutorials_dir(), episode)
    if not os.path.isdir(src_dir):
        raise click.BadArgumentUsage("Episode " + \
                                     click.style("\"{0}\"".format(episode),
                                                 fg='red') + " does not exist."\
                                     " To see a list of available episodes, "\
                                     "type:\n" + \
                                     click.style("metaflow tutorials list",
                                                 fg='cyan'))

def autocomplete_episodes(ctx, args, incomplete):
    return [k for k in get_all_episodes() if incomplete in k]

@tutorials.command(help="Pull episodes "\
                   "into your current working directory.")
@click.option('--episode', default="", help="Optional episode name "\
              "to pull only a single episode.")
def pull(episode):
    tutorials_dir = get_tutorials_dir()
    if not episode:
        episodes = get_all_episodes()
    else:
        episodes = [episode]
        # Validate that the list is valid.
        for episode in episodes:
            validate_episode(episode)
    # Create destination `metaflow-tutorials` dir.
    dst_parent = os.path.join(os.getcwd(), 'metaflow-tutorials')
    makedirs(dst_parent)

    # Pull specified episodes.
    for episode in episodes:
        dst_dir = os.path.join(dst_parent, episode)
        # Check if episode has already been pulled before.
        if os.path.exists(dst_dir):
            if click.confirm("Episode " + \
                             click.style("\"{0}\"".format(episode), fg='red') +\
                             " has already been pulled before. Do you wish "\
                             "to delete the existing version?"):
                shutil.rmtree(dst_dir)
            else:
                continue
        echo('Pulling episode ', nl=False)
        echo('\"{0}\"'.format(episode), fg='cyan', nl=False)
        # TODO: Is the following redudant?
        echo(' into your current working directory.')
        # Copy from (local) metaflow package dir to current.
        src_dir = os.path.join(tutorials_dir, episode)
        shutil.copytree(src_dir, dst_dir)

    echo('\nTo know more about an episode, type:\n', nl=False)
    echo('metaflow tutorials info [EPISODE]', fg='cyan')

@tutorials.command(help='Find out more about an episode.')
@click.argument('episode', autocompletion=autocomplete_episodes)
def info(episode):
    validate_episode(episode)
    src_dir = os.path.join(get_tutorials_dir(), episode)
    metadata = get_tutorial_metadata(src_dir)
    echo('Synopsis:', fg='cyan', bold=True)
    echo('%s' % metadata['description'])

    echo('\nShowcasing:', fg='cyan', bold=True, nl=True)
    echo('%s' % metadata['showcase'])

    if 'prereq' in metadata:
        echo('\nBefore playing:', fg='cyan', bold=True, nl=True)
        echo('%s' % metadata['prereq'])

    echo('\nTo play:', fg='cyan', bold=True)
    echo('%s' % metadata['play'])

# NOTE: This code needs to be in sync with metaflow/metaflow_config.py.
METAFLOW_CONFIGURATION_DIR =\
    expanduser(os.environ.get('METAFLOW_HOME', '~/.metaflowconfig'))

@main.group(help="Configure Metaflow to access the cloud.")
def configure():
    makedirs(METAFLOW_CONFIGURATION_DIR)

def get_config_path(profile):
    config_file = 'config.json' if not profile else ('config_%s.json' % profile)
    path = os.path.join(METAFLOW_CONFIGURATION_DIR, config_file)
    return path

def prompt_config_overwrite(profile):
    path = get_config_path(profile)
    if os.path.exists(path):
        if click.confirm('Overwrite existing configuration '
                         'in ' + click.style('"%s"' % path, fg='cyan') + '?',
                         abort=True):
            return

def check_for_missing_profile(profile):
    path = get_config_path(profile)
    # Absence of default config is equivalent to running locally.
    if profile and not os.path.exists(path):
        raise click.ClickException("Couldn't find configuration for profile " +
                                   click.style('"%s"' % profile, fg='red') +
                                   " in " +
                                   click.style('"%s"' % path, fg='red'))

def persist_env(env_dict, profile):
    # TODO: Should we persist empty env_dict or notify user differently?
    path = get_config_path(profile)

    with open(path, 'w') as f:
        json.dump(env_dict, f, indent=4, sort_keys=True)

    echo('\nConfiguration successfully written to ', nl=False)
    echo('"%s"' % path, fg='cyan')

@configure.command(help='Reset configuration to disable cloud access.')
@click.option('--profile', '-p', default='',
              help="Optional named profile.")
def reset(profile):
    check_for_missing_profile(profile)
    path = get_config_path(profile)
    if os.path.exists(path):
        if click.confirm('Do you really wish to reset the configuration in ' +\
                         click.style('"%s"' % path, fg='cyan'), abort=True):
            os.remove(path)
            echo('Configuration successfully reset to run locally.')
    else:
        echo('Configuration is already reset to run locally.')

@configure.command(help='Show existing configuration.')
@click.option('--profile', '-p', default='',
              help="Optional named profile.")
def show(profile):
    check_for_missing_profile(profile)
    path = get_config_path(profile)
    env_dict = {}
    if os.path.exists(path):
        with open(path, 'r') as f:
            env_dict = json.load(f)
    if env_dict:
        echo('Showing configuration in ', nl=False)
        echo('"%s"\n' % path, fg='cyan')
        for k,v in env_dict.items():
            echo('%s=%s' % (k, v))
    else:
        echo('Configuration is set to run locally.')

@configure.command(help='Export configuration to a file.')
@click.option('--profile', '-p', default='',
              help="Optional named profile whose configuration must be "
                   "exported.")
@click.argument('output_filename', type=click.Path(resolve_path=True))
def export(profile, output_filename):
    check_for_missing_profile(profile)
    # Export its contents to a new file.
    path = get_config_path(profile)
    env_dict = {}
    if os.path.exists(path):
        with open(path, 'r') as f:
            env_dict = json.load(f)
    # resolve_path doesn't expand `~` in `path`.
    output_path = expanduser(output_filename)
    if os.path.exists(output_path):
        if click.confirm('Do you wish to overwrite the contents in ' +
                         click.style('"%s"' % output_path, fg='cyan') + '?',
                         abort=True):
            pass
    # Write to file.
    with open(output_path, 'w') as f:
        json.dump(env_dict, f, indent=4, sort_keys=True)
    echo('Configuration successfully exported to: ', nl=False)
    echo('"%s"' % output_path, fg='cyan')

@configure.command(help='Import configuration from a file.', name='import')
@click.option('--profile', '-p', default='',
              help="Optional named profile to which the configuration must be "
                   "imported into.")
@click.argument('input_filename', type=click.Path(exists=True,
                                                  resolve_path=True))
def import_from(profile, input_filename):
    check_for_missing_profile(profile)
    # Import configuration.
    input_path = expanduser(input_filename)
    env_dict = {}
    with open(input_path, 'r') as f:
        env_dict = json.load(f)
    echo('Configuration successfully read from: ', nl=False)
    echo('"%s"' % input_path, fg='cyan')

    # Persist configuration.
    prompt_config_overwrite(profile)
    persist_env(env_dict, profile)

@configure.command(help='Configure metaflow to access hosted sandbox.')
@click.option('--profile', '-p', default='',
                help='Configure a named profile. Activate the profile by setting ' 
                    '`METAFLOW_PROFILE` environment variable.')
def sandbox(profile):
    prompt_config_overwrite(profile)
    # Prompt for user input.
    encoded_str = click.prompt('Following instructions from '
                               'https://metaflow.org/sandbox, '
                               'please paste the encoded magic string',
                               type=str)
    # Decode the bytes to env_dict.
    try:
        import base64, zlib
        from metaflow.util import to_bytes
        env_dict =\
            json.loads(zlib.decompress(base64.b64decode(to_bytes(encoded_str))))
    except:
        # TODO: Add the URL for contact us page in the error?
        raise click.BadArgumentUsage('Could not decode the sandbox '\
                                     'configuration. Please contact us.')
    # Persist to a file.
    persist_env(env_dict, profile)

@configure.command(help='Configure metaflow to access self-managed AWS resources.')
@click.option('--profile', '-p', default='',
                help='Configure a named profile. Activate the profile by setting ' 
                    '`METAFLOW_PROFILE` environment variable.')
def aws(profile):
    def cyan(string):
        return click.style(string, fg='cyan')

    def yellow(string):
        return click.style(string, fg='yellow')

    prompt_config_overwrite(profile)
    if not click.confirm('Already configured ' + cyan('AWS access credentials') + '?',
          default=True):
        echo('\nSetup your access credentials first by following this guide: ', nl=False)
        echo('https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html',
             fg='cyan')
    else:
        env_dict = {}
        # Datastore configuration.
        use_s3 = click.confirm('\nConfigure Amazon '
                                'S3 as default datastore (required for AWS Batch)?',
                              default=True, abort=False)
        if use_s3:
            env_dict['METAFLOW_DEFAULT_DATASTORE'] = 's3'
            env_dict['METAFLOW_DATASTORE_SYSROOT_S3'] =\
                click.prompt('\t' + cyan('METAFLOW_DATASTORE_SYSROOT_S3:') + 
                  ' Amazon S3 url for metaflow datastore (s3://<bucket>/<prefix>)')

            env_dict['METAFLOW_DATATOOLS_SYSROOT_S3'] =\
                click.prompt('\t' + cyan('METAFLOW_DATATOOLS_SYSROOT_S3:') + 
                  yellow(' (optional)') + ' Amazon S3 url for metaflow datatools (s3://<bucket>/<prefix>)',
                    default='%s/data' % env_dict['METAFLOW_DATASTORE_SYSROOT_S3'],
                    show_default=True)
            # AWS Batch configuration (only if Amazon S3 is being used).
            use_batch =\
                click.confirm('\nConfigure AWS Batch for compute?',
                              default=True, abort=False)
            if use_batch:
                env_dict['METAFLOW_BATCH_JOB_QUEUE'] =\
                    click.prompt('\t' + cyan('METAFLOW_BATCH_JOB_QUEUE:') + 
                      ' AWS Batch Job Queue')
                env_dict['METAFLOW_ECS_S3_ACCESS_IAM_ROLE'] =\
                    click.prompt('\t' + cyan('METAFLOW_ECS_S3_ACCESS_IAM_ROLE:') + 
                      ' IAM role granting AWS Batch access to Amazon S3')
                metaflow_batch_container_registry =\
                    click.prompt('\t' + cyan('METAFLOW_BATCH_CONTAINER_REGISTRY:') + 
                      yellow(' (optional)') + ' Default docker image repository for AWS Batch jobs',
                        default='', show_default=False)
                if metaflow_batch_container_registry:
                    env_dict['METAFLOW_BATCH_CONTAINER_REGISTRY'] = metaflow_batch_container_registry
                metaflow_batch_container_image =\
                    click.prompt('\t' + cyan('METAFLOW_BATCH_CONTAINER_IMAGE:') + 
                      yellow(' (optional)') + ' Default docker image for AWS Batch jobs',
                        default='', show_default=False)
                if metaflow_batch_container_image:
                    env_dict['METAFLOW_BATCH_CONTAINER_IMAGE'] = metaflow_batch_container_image
        # Metadata service configuration.
        use_metadata = click.confirm('\nConfigure Metadata Service as default metadata provider?', 
            default=True, abort=False)
        if use_metadata:
            env_dict['METAFLOW_DEFAULT_METADATA'] = 'service'
            env_dict['METAFLOW_SERVICE_URL'] =\
                click.prompt('\t' + cyan('METAFLOW_SERVICE_URL:') +
                  ' URL for Metadata Service (Open to Public Access)')
            env_dict['METAFLOW_SERVICE_INTERNAL_URL'] =\
                click.prompt('\t' + cyan('METAFLOW_SERVICE_INTERNAL_URL:') +
                  yellow(' (optional)') +  ' URL for Metadata Service (Accessible within VPC)',
                    default=env_dict['METAFLOW_SERVICE_URL'], show_default=True)
            metaflow_service_auth_key =\
                click.prompt('\t' + cyan('METAFLOW_SERVICE_AUTH_KEY:') + 
                  yellow(' (optional)') + ' Auth key for Metadata Service',
                    default='', show_default=False)
            if metaflow_service_auth_key:
                    env_dict['METAFLOW_SERVICE_AUTH_KEY'] = metaflow_service_auth_key
        persist_env(env_dict, profile)
