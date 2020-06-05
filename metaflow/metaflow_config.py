import os
import json
import logging
import pkg_resources
import sys


from metaflow.exception import MetaflowException

# Disable multithreading security on MacOS
if sys.platform == "darwin":
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"


def init_config():
    # Read configuration from $METAFLOW_HOME/config_<profile>.json.
    home = os.environ.get('METAFLOW_HOME', '~/.metaflowconfig')
    profile = os.environ.get('METAFLOW_PROFILE')
    path_to_config = os.path.join(home, 'config.json')
    if profile:
        path_to_config = os.path.join(home, 'config_%s.json' % profile)
    path_to_config = os.path.expanduser(path_to_config)
    config = {}
    if os.path.exists(path_to_config):
        with open(path_to_config) as f:
            return json.load(f)
    elif profile:
        raise MetaflowException('Unable to locate METAFLOW_PROFILE \'%s\' in \'%s\')' %
                                (profile, home))
    return config


# Initialize defaults required to setup environment variables.
METAFLOW_CONFIG = init_config()


def from_conf(name, default=None):
    return os.environ.get(name, METAFLOW_CONFIG.get(name, default))


###
# Default configuration
###
DEFAULT_DATASTORE = from_conf('METAFLOW_DEFAULT_DATASTORE', 'local')
DEFAULT_METADATA = from_conf('METAFLOW_DEFAULT_METADATA', 'local')

###
# Datastore configuration
###
# Path to the local directory to store artifacts for 'local' datastore.
DATASTORE_LOCAL_DIR = '.metaflow'
DATASTORE_SYSROOT_LOCAL = from_conf('METAFLOW_DATASTORE_SYSROOT_LOCAL')
# S3 bucket and prefix to store artifacts for 's3' datastore.
DATASTORE_SYSROOT_S3 = from_conf('METAFLOW_DATASTORE_SYSROOT_S3')
# S3 datatools root location
DATATOOLS_SUFFIX = from_conf('METAFLOW_DATATOOLS_SUFFIX', 'data')
DATATOOLS_S3ROOT = from_conf(
    'METAFLOW_DATATOOLS_S3ROOT', 
        '%s/%s' % (from_conf('METAFLOW_DATASTORE_SYSROOT_S3'), DATATOOLS_SUFFIX)
            if from_conf('METAFLOW_DATASTORE_SYSROOT_S3') else None)
# Local datatools root location
DATATOOLS_LOCALROOT = from_conf(
    'METAFLOW_DATATOOLS_LOCALROOT',
        '%s/%s' % (from_conf('METAFLOW_DATASTORE_SYSROOT_LOCAL'), DATATOOLS_SUFFIX)
            if from_conf('METAFLOW_DATASTORE_SYSROOT_LOCAL') else None)

# S3 endpoint url 
S3_ENDPOINT_URL = from_conf('METAFLOW_S3_ENDPOINT_URL', None)
S3_VERIFY_CERTIFICATE = from_conf('METAFLOW_S3_VERIFY_CERTIFICATE', None)

###
# Datastore local cache
###
# Path to the client cache
CLIENT_CACHE_PATH = from_conf('METAFLOW_CLIENT_CACHE_PATH', '/tmp/metaflow_client')
# Maximum size (in bytes) of the cache
CLIENT_CACHE_MAX_SIZE = from_conf('METAFLOW_CLIENT_CACHE_MAX_SIZE', 10000)

###
# Metadata configuration
###
METADATA_SERVICE_URL = from_conf('METAFLOW_SERVICE_URL')
METADATA_SERVICE_NUM_RETRIES = from_conf('METAFLOW_SERVICE_RETRY_COUNT', 5)
METADATA_SERVICE_AUTH_KEY = from_conf('METAFLOW_SERVICE_AUTH_KEY')
METADATA_SERVICE_HEADERS = json.loads(from_conf('METAFLOW_SERVICE_HEADERS', '{}'))
if METADATA_SERVICE_AUTH_KEY is not None:
    METADATA_SERVICE_HEADERS['x-api-key'] = METADATA_SERVICE_AUTH_KEY

###
# AWS Batch configuration
###
# IAM role for AWS Batch container with Amazon S3 access 
# (and AWS DynamoDb access for AWS StepFunctions, if enabled)
ECS_S3_ACCESS_IAM_ROLE = from_conf('METAFLOW_ECS_S3_ACCESS_IAM_ROLE')
# Job queue for AWS Batch
BATCH_JOB_QUEUE = from_conf('METAFLOW_BATCH_JOB_QUEUE')
# Default container image for AWS Batch
BATCH_CONTAINER_IMAGE = from_conf("METAFLOW_BATCH_CONTAINER_IMAGE")
# Default container registry for AWS Batch
BATCH_CONTAINER_REGISTRY = from_conf("METAFLOW_BATCH_CONTAINER_REGISTRY")
# Metadata service URL for AWS Batch
BATCH_METADATA_SERVICE_URL = from_conf('METAFLOW_SERVICE_INTERNAL_URL', METADATA_SERVICE_URL)
BATCH_METADATA_SERVICE_HEADERS = METADATA_SERVICE_HEADERS

###
# AWS Step Functions configuration
###
# IAM role for AWS Step Functions with AWS Batch and AWS DynamoDb access
# https://docs.aws.amazon.com/step-functions/latest/dg/batch-iam.html
SFN_IAM_ROLE = from_conf("METAFLOW_SFN_IAM_ROLE")
# AWS DynamoDb Table name (with partition key - `pathspec` of type string)
SFN_DYNAMO_DB_TABLE = from_conf("METAFLOW_SFN_DYNAMO_DB_TABLE")
# IAM role for AWS Events with AWS Step Functions access
# https://docs.aws.amazon.com/eventbridge/latest/userguide/auth-and-access-control-eventbridge.html
EVENTS_SFN_ACCESS_IAM_ROLE = from_conf("METAFLOW_EVENTS_SFN_ACCESS_IAM_ROLE")
# Prefix for AWS Step Functions state machines. Set to stack name for Metaflow
# sandbox.
SFN_STATE_MACHINE_PREFIX = None

###
# Conda configuration
###
# Conda package root location on S3
CONDA_PACKAGE_S3ROOT = from_conf(
    'METAFLOW_CONDA_PACKAGE_S3ROOT', 
        '%s/conda' % from_conf('METAFLOW_DATASTORE_SYSROOT_S3'))

###
# Debug configuration
###
DEBUG_OPTIONS = ['subcommand', 'sidecar', 's3client']

for typ in DEBUG_OPTIONS:
    vars()['METAFLOW_DEBUG_%s' % typ.upper()] = from_conf('METAFLOW_DEBUG_%s' % typ.upper())

###
# AWS Sandbox configuration
###
# Boolean flag for metaflow AWS sandbox access
AWS_SANDBOX_ENABLED = bool(from_conf('METAFLOW_AWS_SANDBOX_ENABLED', False))
# Metaflow AWS sandbox auth endpoint
AWS_SANDBOX_STS_ENDPOINT_URL = from_conf('METAFLOW_SERVICE_URL')
# Metaflow AWS sandbox API auth key
AWS_SANDBOX_API_KEY = from_conf('METAFLOW_AWS_SANDBOX_API_KEY')
# Internal Metadata URL
AWS_SANDBOX_INTERNAL_SERVICE_URL = from_conf('METAFLOW_AWS_SANDBOX_INTERNAL_SERVICE_URL')
# AWS region
AWS_SANDBOX_REGION = from_conf('METAFLOW_AWS_SANDBOX_REGION')


# Finalize configuration
if AWS_SANDBOX_ENABLED:
    os.environ['AWS_DEFAULT_REGION'] = AWS_SANDBOX_REGION
    BATCH_METADATA_SERVICE_URL = AWS_SANDBOX_INTERNAL_SERVICE_URL
    METADATA_SERVICE_HEADERS['x-api-key'] = AWS_SANDBOX_API_KEY
    SFN_STATE_MACHINE_PREFIX = from_conf('METAFLOW_AWS_SANDBOX_STACK_NAME')


# MAX_ATTEMPTS is the maximum number of attempts, including the first
# task, retries, and the final fallback task and its retries.
#
# Datastore needs to check all attempt files to find the latest one, so
# increasing this limit has real performance implications for all tasks.
# Decreasing this limit is very unsafe, as it can lead to wrong results
# being read from old tasks.
MAX_ATTEMPTS = 6


# the naughty, naughty driver.py imported by lib2to3 produces
# spam messages to the root logger. This is what is required
# to silence it:
class Filter(logging.Filter):
    def filter(self, record):
        if record.pathname.endswith('driver.py') and \
           'grammar' in record.msg:
            return False
        return True


logger = logging.getLogger()
logger.addFilter(Filter())


def get_version(pkg):
    return pkg_resources.get_distribution(pkg).version


# PINNED_CONDA_LIBS are the libraries that metaflow depends on for execution
# and are needed within a conda environment
def get_pinned_conda_libs():
    return {
        'click': '7.0',
        'requests': '2.22.0',
        'boto3': '1.9.235',
        'coverage': '4.5.3'
    }


cached_aws_sandbox_creds = None

def get_authenticated_boto3_client(module, params={}):
    from metaflow.exception import MetaflowException
    import requests
    try:
        import boto3
    except (NameError, ImportError):
        raise MetaflowException(
            "Could not import module 'boto3'. Install boto3 first.")

    if AWS_SANDBOX_ENABLED:
        global cached_aws_sandbox_creds
        if cached_aws_sandbox_creds is None:
            # authenticate using STS
            url = "%s/auth/token" % AWS_SANDBOX_STS_ENDPOINT_URL
            headers = {
                'x-api-key': AWS_SANDBOX_API_KEY
            }
            try:
                r = requests.get(url, headers=headers)
                r.raise_for_status()
                cached_aws_sandbox_creds = r.json()
            except requests.exceptions.HTTPError as e:
                raise MetaflowException(repr(e))
        return boto3.session.Session(**cached_aws_sandbox_creds) \
            .client(module, **params)
    return boto3.client(module, **params)