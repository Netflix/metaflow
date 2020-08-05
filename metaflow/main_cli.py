import click
import json
import os
import shutil

from os.path import expanduser

from metaflow.datastore.local import LocalDataStore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR, DEFAULT_METADATA
from metaflow.util import to_unicode


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

def overwrite_config(profile):
    path = get_config_path(profile)
    if os.path.exists(path):
        if not click.confirm(
            click.style('We found an existing configuration for your ' +
                        'profile. Do you want to modify the existing ' +
                        'configuration?', fg='red', bold=True)):
            echo('You can configure a different named profile by using the '
                 '--profile argument. You can activate this profile by setting '
                 'the environment variable METAFLOW_PROFILE to the named '
                 'profile.', fg='yellow')
            return False
    return True


def check_for_missing_profile(profile):
    path = get_config_path(profile)
    # Absence of default config is equivalent to running locally.
    if profile and not os.path.exists(path):
        raise click.ClickException("Couldn't find configuration for profile " +
                                   click.style('"%s"' % profile, fg='red') +
                                   " in " +
                                   click.style('"%s"' % path, fg='red'))

def get_env(profile):
    path = get_config_path(profile)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}

def persist_env(env_dict, profile):
    # TODO: Should we persist empty env_dict or notify user differently?
    path = get_config_path(profile)

    with open(path, 'w') as f:
        json.dump(env_dict, f, indent=4, sort_keys=True)

    echo('\nConfiguration successfully written to ', nl=False, bold=True)
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
    overwrite_config(profile)
    persist_env(env_dict, profile)

@configure.command(help='Configure metaflow to access hosted sandbox.')
@click.option('--profile', '-p', default='',
                help='Configure a named profile. Activate the profile by setting ' 
                    '`METAFLOW_PROFILE` environment variable.')
def sandbox(profile):
    overwrite_config(profile)
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
            json.loads(to_unicode(zlib.decompress(base64.b64decode(to_bytes(encoded_str)))))
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
@click.pass_context
def aws(ctx, profile):
    def cyan(string):
        return click.style(string, fg='cyan')

    def yellow(string):
        return click.style(string, fg='yellow')

    # Greet the user!
    echo('Welcome to Metaflow! Follow the prompts to configure your '
         'installation.\n',
         bold=True)

    # Check for existing configuration.
    if not overwrite_config(profile):
        ctx.abort()

    # Verify that the user has configured AWS credentials on their computer.
    if not click.confirm('\nMetaflow relies on ' +  
                         yellow('AWS access credentials') + 
                         ' present on your computer to access resources on AWS.'
                         '\nBefore proceeding further, please confirm that you '
                         'have already configured these access credentials on '
                         'this computer.',
                         default=True):
        echo('There are many ways to setup your AWS access credentials. You '
             'can get started by following this guide: ',
             nl=False,
             fg='yellow')
        echo('https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html',
             fg='cyan')
        ctx.abort()

    existing_env = get_env(profile)
    empty_profile = False
    if not existing_env:
        empty_profile = True
    env = {}

    # Configure Amazon S3 as the datastore.
    use_s3_as_datastore = click.confirm('\nMetaflow can use ' +
                            yellow('Amazon S3 as the storage backend') +
                            ' for all code and data artifacts on ' +
                            'AWS.\nAmazon S3 is a strict requirement if you ' +
                            'intend to execute your flows on AWS Batch ' +
                            'and/or schedule them on AWS Step ' +
                            'Functions.\nWould you like to configure Amazon ' +
                            'S3 as the default storage backend?',
                            default=empty_profile or \
                            existing_env.get(
                                'METAFLOW_DEFAULT_DATASTORE', '') == 's3',
                            abort=False)
    if use_s3_as_datastore:
        # Set Amazon S3 as default datastore.
        env['METAFLOW_DEFAULT_DATASTORE'] = 's3'
        # Set Amazon S3 folder for datastore.
        env['METAFLOW_DATASTORE_SYSROOT_S3'] =\
            click.prompt(cyan('[METAFLOW_DATASTORE_SYSROOT_S3]') + 
                         ' Amazon S3 folder for Metaflow artifact storage ' +
                         '(s3://<bucket>/<prefix>).',
                         default=\
                            existing_env.get('METAFLOW_DATASTORE_SYSROOT_S3'),
                         show_default=True)
        # Set Amazon S3 folder for datatools.
        env['METAFLOW_DATATOOLS_SYSROOT_S3'] =\
                click.prompt(cyan('[METAFLOW_DATATOOLS_SYSROOT_S3]') + 
                             yellow(' (optional)') + 
                             ' Amazon S3 folder for Metaflow datatools ' +
                             '(s3://<bucket>/<prefix>).',
                             default=\
                                existing_env.get('METAFLOW_DATATOOLS_SYSROOT_S3',
                                    os.path.join(
                                        env['METAFLOW_DATASTORE_SYSROOT_S3'],
                                            'data')),
                             show_default=True)

    # Configure Metadata service for tracking.
    if click.confirm('\nMetaflow can use a ' +
                     yellow('remote Metadata Service to track') + 
                     ' and persist flow execution metadata.\nConfiguring the '
                     'service is a requirement if you intend to schedule your '
                     'flows with AWS Step Functions.\nWould you like to '
                     'configure the Metadata Service?',
                     default=empty_profile or\
                        existing_env.get('METAFLOW_DEFAULT_METADATA', '') ==\
                            'service' or\
                        'METAFLOW_SFN_IAM_ROLE' in env,
                     abort=False):
        # Set Metadata Service as default.
        env['METAFLOW_DEFAULT_METADATA'] = 'service'
        # Set URL for the Metadata Service.
        env['METAFLOW_SERVICE_URL'] =\
                click.prompt(cyan('[METAFLOW_SERVICE_URL]') +
                             ' URL for Metaflow Service.',
                             default=existing_env.get('METAFLOW_SERVICE_URL'),
                             show_default=True)
        # Set internal URL for the Metadata Service.
        env['METAFLOW_SERVICE_INTERNAL_URL'] =\
                click.prompt(cyan('[METAFLOW_SERVICE_INTERNAL_URL]') +
                             yellow(' (optional)') +
                             ' URL for Metaflow Service ' +
                             '(Accessible only within VPC).',
                             default=\
                                existing_env.get('METAFLOW_SERVICE_INTERNAL_URL',
                                    env['METAFLOW_SERVICE_URL']),
                             show_default=True)
        # Set Auth Key for the Metadata Service.
        env['METAFLOW_SERVICE_AUTH_KEY'] =\
                click.prompt(cyan('[METAFLOW_SERVICE_AUTH_KEY]') + 
                             yellow(' (optional)') +
                             ' Auth Key for Metaflow Service.',
                             default=\
                                existing_env.get('METAFLOW_SERVICE_AUTH_KEY', ''),
                             show_default=True)

    # Configure AWS Batch for compute.
    if use_s3_as_datastore:
        if click.confirm('\nMetaflow can scale your flows by ' +
                         yellow('executing your steps on AWS Batch') + 
                         '.\nAWS Batch is a strict requirement if you intend '
                         'to schedule your flows on AWS Step Functions.\nWould '
                         'you like to configure AWS Batch as your compute '
                         'backend?',
                         default=empty_profile or 
                            'METAFLOW_BATCH_JOB_QUEUE' in existing_env,
                         abort=False):
            # Set AWS Batch Job Queue.
            env['METAFLOW_BATCH_JOB_QUEUE'] =\
                    click.prompt(cyan('[METAFLOW_BATCH_JOB_QUEUE]') +
                                 ' AWS Batch Job Queue.',
                                 default=\
                                    existing_env.get('METAFLOW_BATCH_JOB_QUEUE'),
                                 show_default=True)
            # Set IAM role for AWS Batch jobs to assume.
            env['METAFLOW_ECS_S3_ACCESS_IAM_ROLE'] =\
                    click.prompt(cyan('[METAFLOW_ECS_S3_ACCESS_IAM_ROLE]') + 
                                 ' IAM role for AWS Batch jobs to access AWS ' +
                                 'resources (Amazon S3 etc.).',
                                 default=\
                                    existing_env.get('METAFLOW_ECS_S3_ACCESS_IAM_ROLE'),
                                 show_default=True)
            # Set default Docker repository for AWS Batch jobs.
            env['METAFLOW_BATCH_CONTAINER_REGISTRY'] =\
                    click.prompt(cyan('[METAFLOW_BATCH_CONTAINER_REGISTRY]') + 
                                 yellow(' (optional)') +
                                 ' Default Docker image repository for AWS ' +
                                 'Batch jobs. If nothing is specified, ' +
                                 'dockerhub (hub.docker.com/) is ' +
                                 'used as default.',
                                 default=\
                                    existing_env.get('METAFLOW_BATCH_CONTAINER_REGISTRY', ''),
                                 show_default=True)
            # Set default Docker image for AWS Batch jobs.
            env['METAFLOW_BATCH_CONTAINER_IMAGE'] =\
                    click.prompt(cyan('[METAFLOW_BATCH_CONTAINER_IMAGE]') + 
                                 yellow(' (optional)') +
                                 ' Default Docker image for AWS Batch jobs. ' +
                                 'If nothing is specified, an appropriate ' +
                                 'python image is used as default.',
                                 default=\
                                    existing_env.get('METAFLOW_BATCH_CONTAINER_IMAGE', ''),
                                 show_default=True)

            # Configure AWS Step Functions for scheduling.
            if click.confirm('\nMetaflow can ' +
                             yellow('schedule your flows on AWS Step '
                                    'Functions') + 
                             ' and trigger them at a specific cadence using '
                             'Amazon EventBridge.\nTo support flows involving '
                             'foreach steps, you would need access to AWS '
                             'DynamoDB.\nWould you like to configure AWS Step '
                             'Functions for scheduling?',
                             default=empty_profile or
                                'METAFLOW_SFN_IAM_ROLE' in existing_env,
                             abort=False):
                # Configure IAM role for AWS Step Functions.
                env['METAFLOW_SFN_IAM_ROLE'] =\
                        click.prompt(cyan('[METAFLOW_SFN_IAM_ROLE]') + 
                                     ' IAM role for AWS Step Functions to ' +
                                     'access AWS resources (AWS Batch, ' +
                                     'AWS DynamoDB).',
                                     default=\
                                        existing_env.get('METAFLOW_SFN_IAM_ROLE'),
                                     show_default=True)
                # Configure IAM role for AWS Events Bridge.
                env['METAFLOW_EVENTS_SFN_ACCESS_IAM_ROLE'] =\
                        click.prompt(cyan('[METAFLOW_EVENTS_SFN_ACCESS_IAM_ROLE]') + 
                                     ' IAM role for Amazon EventBridge to ' +
                                     'access AWS Step Functions.',
                                     default=\
                                        existing_env.get('METAFLOW_EVENTS_SFN_ACCESS_IAM_ROLE'),
                                     show_default=True)
                # Configure AWS DynamoDB Table for AWS Step Functions.
                env['METAFLOW_SFN_DYNAMO_DB_TABLE'] =\
                        click.prompt(cyan('[METAFLOW_SFN_DYNAMO_DB_TABLE]') + 
                                    ' AWS DynamoDB table name for tracking '+
                                    'AWS Step Functions execution metadata.',
                                     default=\
                                        existing_env.get('METAFLOW_SFN_DYNAMO_DB_TABLE'),
                                     show_default=True)
    persist_env({k: v for k, v in env.items() if v}, profile)
