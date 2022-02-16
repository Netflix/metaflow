import os
import json
import logging
import pkg_resources
import sys
import types


from metaflow.exception import MetaflowException

# Disable multithreading security on MacOS
if sys.platform == "darwin":
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"


def init_config():
    # Read configuration from $METAFLOW_HOME/config_<profile>.json.
    home = os.environ.get("METAFLOW_HOME", "~/.metaflowconfig")
    profile = os.environ.get("METAFLOW_PROFILE")
    path_to_config = os.path.join(home, "config.json")
    if profile:
        path_to_config = os.path.join(home, "config_%s.json" % profile)
    path_to_config = os.path.expanduser(path_to_config)
    config = {}
    if os.path.exists(path_to_config):
        with open(path_to_config) as f:
            return json.load(f)
    elif profile:
        raise MetaflowException(
            "Unable to locate METAFLOW_PROFILE '%s' in '%s')" % (profile, home)
        )
    return config


# Initialize defaults required to setup environment variables.
METAFLOW_CONFIG = init_config()


def from_conf(name, default=None):
    return os.environ.get(name, METAFLOW_CONFIG.get(name, default))


###
# Default configuration
###
DEFAULT_DATASTORE = from_conf("METAFLOW_DEFAULT_DATASTORE", "local")
DEFAULT_ENVIRONMENT = from_conf("METAFLOW_DEFAULT_ENVIRONMENT", "local")
DEFAULT_EVENT_LOGGER = from_conf("METAFLOW_DEFAULT_EVENT_LOGGER", "nullSidecarLogger")
DEFAULT_METADATA = from_conf("METAFLOW_DEFAULT_METADATA", "local")
DEFAULT_MONITOR = from_conf("METAFLOW_DEFAULT_MONITOR", "nullSidecarMonitor")
DEFAULT_PACKAGE_SUFFIXES = from_conf("METAFLOW_DEFAULT_PACKAGE_SUFFIXES", ".py,.R,.RDS")
DEFAULT_AWS_CLIENT_PROVIDER = from_conf("METAFLOW_DEFAULT_AWS_CLIENT_PROVIDER", "boto3")


###
# Datastore configuration
###
# Path to the local directory to store artifacts for 'local' datastore.
DATASTORE_LOCAL_DIR = ".metaflow"
DATASTORE_SYSROOT_LOCAL = from_conf("METAFLOW_DATASTORE_SYSROOT_LOCAL")
# S3 bucket and prefix to store artifacts for 's3' datastore.
DATASTORE_SYSROOT_S3 = from_conf("METAFLOW_DATASTORE_SYSROOT_S3")
# S3 datatools root location
DATATOOLS_SUFFIX = from_conf("METAFLOW_DATATOOLS_SUFFIX", "data")
DATATOOLS_S3ROOT = from_conf(
    "METAFLOW_DATATOOLS_S3ROOT",
    os.path.join(from_conf("METAFLOW_DATASTORE_SYSROOT_S3"), DATATOOLS_SUFFIX)
    if from_conf("METAFLOW_DATASTORE_SYSROOT_S3")
    else None,
)
# Local datatools root location
DATATOOLS_LOCALROOT = from_conf(
    "METAFLOW_DATATOOLS_LOCALROOT",
    os.path.join(from_conf("METAFLOW_DATASTORE_SYSROOT_LOCAL"), DATATOOLS_SUFFIX)
    if from_conf("METAFLOW_DATASTORE_SYSROOT_LOCAL")
    else None,
)

# Cards related config variables
DATASTORE_CARD_SUFFIX = "mf.cards"
DATASTORE_CARD_LOCALROOT = from_conf("METAFLOW_CARD_LOCALROOT")
DATASTORE_CARD_S3ROOT = from_conf(
    "METAFLOW_CARD_S3ROOT",
    os.path.join(from_conf("METAFLOW_DATASTORE_SYSROOT_S3"), DATASTORE_CARD_SUFFIX)
    if from_conf("METAFLOW_DATASTORE_SYSROOT_S3")
    else None,
)
CARD_NO_WARNING = from_conf("METAFLOW_CARD_NO_WARNING", False)

# S3 endpoint url
S3_ENDPOINT_URL = from_conf("METAFLOW_S3_ENDPOINT_URL", None)
S3_VERIFY_CERTIFICATE = from_conf("METAFLOW_S3_VERIFY_CERTIFICATE", None)

# S3 retry configuration
# This is useful if you want to "fail fast" on S3 operations; use with caution
# though as this may increase failures. Note that this is the number of *retries*
# so setting it to 0 means each operation will be tried once.
S3_RETRY_COUNT = int(from_conf("METAFLOW_S3_RETRY_COUNT", 7))

###
# Datastore local cache
###
# Path to the client cache
CLIENT_CACHE_PATH = from_conf("METAFLOW_CLIENT_CACHE_PATH", "/tmp/metaflow_client")
# Maximum size (in bytes) of the cache
CLIENT_CACHE_MAX_SIZE = int(from_conf("METAFLOW_CLIENT_CACHE_MAX_SIZE", 10000))
# Maximum number of cached Flow and TaskDatastores in the cache
CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT = int(
    from_conf("METAFLOW_CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT", 50)
)
CLIENT_CACHE_MAX_TASKDATASTORE_COUNT = int(
    from_conf(
        "METAFLOW_CLIENT_CACHE_MAX_TASKDATASTORE_COUNT",
        CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT * 100,
    )
)


###
# Metadata configuration
###
METADATA_SERVICE_URL = from_conf("METAFLOW_SERVICE_URL")
METADATA_SERVICE_NUM_RETRIES = int(from_conf("METAFLOW_SERVICE_RETRY_COUNT", 5))
METADATA_SERVICE_AUTH_KEY = from_conf("METAFLOW_SERVICE_AUTH_KEY")
METADATA_SERVICE_HEADERS = json.loads(from_conf("METAFLOW_SERVICE_HEADERS", "{}"))
if METADATA_SERVICE_AUTH_KEY is not None:
    METADATA_SERVICE_HEADERS["x-api-key"] = METADATA_SERVICE_AUTH_KEY

# Default container image
DEFAULT_CONTAINER_IMAGE = from_conf("METAFLOW_DEFAULT_CONTAINER_IMAGE")
# Default container registry
DEFAULT_CONTAINER_REGISTRY = from_conf("METAFLOW_DEFAULT_CONTAINER_REGISTRY")

###
# AWS Batch configuration
###
# IAM role for AWS Batch container with Amazon S3 access
# (and AWS DynamoDb access for AWS StepFunctions, if enabled)
ECS_S3_ACCESS_IAM_ROLE = from_conf("METAFLOW_ECS_S3_ACCESS_IAM_ROLE")
# IAM role for AWS Batch container for AWS Fargate
ECS_FARGATE_EXECUTION_ROLE = from_conf("METAFLOW_ECS_FARGATE_EXECUTION_ROLE")
# Job queue for AWS Batch
BATCH_JOB_QUEUE = from_conf("METAFLOW_BATCH_JOB_QUEUE")
# Default container image for AWS Batch
BATCH_CONTAINER_IMAGE = (
    from_conf("METAFLOW_BATCH_CONTAINER_IMAGE") or DEFAULT_CONTAINER_IMAGE
)
# Default container registry for AWS Batch
BATCH_CONTAINER_REGISTRY = (
    from_conf("METAFLOW_BATCH_CONTAINER_REGISTRY") or DEFAULT_CONTAINER_REGISTRY
)
# Metadata service URL for AWS Batch
BATCH_METADATA_SERVICE_URL = from_conf(
    "METAFLOW_SERVICE_INTERNAL_URL", METADATA_SERVICE_URL
)
BATCH_METADATA_SERVICE_HEADERS = METADATA_SERVICE_HEADERS

# Assign resource tags to AWS Batch jobs. Set to False by default since
# it requires `Batch:TagResource` permissions which may not be available
# in all Metaflow deployments. Hopefully, some day we can flip the
# default to True.
BATCH_EMIT_TAGS = from_conf("METAFLOW_BATCH_EMIT_TAGS", False)

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
SFN_STATE_MACHINE_PREFIX = from_conf("METAFLOW_SFN_STATE_MACHINE_PREFIX")
# Optional AWS CloudWatch Log Group ARN for emitting AWS Step Functions state
# machine execution logs. This needs to be available when using the
# `step-functions create --log-execution-history` command.
SFN_EXECUTION_LOG_GROUP_ARN = from_conf("METAFLOW_SFN_EXECUTION_LOG_GROUP_ARN")

###
# Kubernetes configuration
###
# Kubernetes namespace to use for all objects created by Metaflow
KUBERNETES_NAMESPACE = from_conf("METAFLOW_KUBERNETES_NAMESPACE", "default")
# Service account to use by K8S jobs created by Metaflow
KUBERNETES_SERVICE_ACCOUNT = from_conf("METAFLOW_KUBERNETES_SERVICE_ACCOUNT")
# Default container image for K8S
KUBERNETES_CONTAINER_IMAGE = (
    from_conf("METAFLOW_KUBERNETES_CONTAINER_IMAGE") or DEFAULT_CONTAINER_IMAGE
)
# Default container registry for K8S
KUBERNETES_CONTAINER_REGISTRY = (
    from_conf("METAFLOW_KUBERNETES_CONTAINER_REGISTRY") or DEFAULT_CONTAINER_REGISTRY
)
#

###
# Conda configuration
###
# Conda package root location on S3
CONDA_PACKAGE_S3ROOT = from_conf(
    "METAFLOW_CONDA_PACKAGE_S3ROOT",
    "%s/conda" % from_conf("METAFLOW_DATASTORE_SYSROOT_S3"),
)

# Use an alternate dependency resolver for conda packages instead of conda
# Mamba promises faster package dependency resolution times, which
# should result in an appreciable speedup in flow environment initialization.
CONDA_DEPENDENCY_RESOLVER = from_conf("METAFLOW_CONDA_DEPENDENCY_RESOLVER", "conda")

###
# Debug configuration
###
DEBUG_OPTIONS = ["subcommand", "sidecar", "s3client"]

for typ in DEBUG_OPTIONS:
    vars()["METAFLOW_DEBUG_%s" % typ.upper()] = from_conf(
        "METAFLOW_DEBUG_%s" % typ.upper()
    )

###
# AWS Sandbox configuration
###
# Boolean flag for metaflow AWS sandbox access
AWS_SANDBOX_ENABLED = bool(from_conf("METAFLOW_AWS_SANDBOX_ENABLED", False))
# Metaflow AWS sandbox auth endpoint
AWS_SANDBOX_STS_ENDPOINT_URL = from_conf("METAFLOW_SERVICE_URL")
# Metaflow AWS sandbox API auth key
AWS_SANDBOX_API_KEY = from_conf("METAFLOW_AWS_SANDBOX_API_KEY")
# Internal Metadata URL
AWS_SANDBOX_INTERNAL_SERVICE_URL = from_conf(
    "METAFLOW_AWS_SANDBOX_INTERNAL_SERVICE_URL"
)
# AWS region
AWS_SANDBOX_REGION = from_conf("METAFLOW_AWS_SANDBOX_REGION")


# Finalize configuration
if AWS_SANDBOX_ENABLED:
    os.environ["AWS_DEFAULT_REGION"] = AWS_SANDBOX_REGION
    BATCH_METADATA_SERVICE_URL = AWS_SANDBOX_INTERNAL_SERVICE_URL
    METADATA_SERVICE_HEADERS["x-api-key"] = AWS_SANDBOX_API_KEY
    SFN_STATE_MACHINE_PREFIX = from_conf("METAFLOW_AWS_SANDBOX_STACK_NAME")


# MAX_ATTEMPTS is the maximum number of attempts, including the first
# task, retries, and the final fallback task and its retries.
#
# Datastore needs to check all attempt files to find the latest one, so
# increasing this limit has real performance implications for all tasks.
# Decreasing this limit is very unsafe, as it can lead to wrong results
# being read from old tasks.
#
# Note also that DataStoreSet resolves the latest attempt_id using
# lexicographic ordering of attempts. This won't work if MAX_ATTEMPTS > 99.
MAX_ATTEMPTS = 6


# the naughty, naughty driver.py imported by lib2to3 produces
# spam messages to the root logger. This is what is required
# to silence it:
class Filter(logging.Filter):
    def filter(self, record):
        if record.pathname.endswith("driver.py") and "grammar" in record.msg:
            return False
        return True


logger = logging.getLogger()
logger.addFilter(Filter())


def get_version(pkg):
    return pkg_resources.get_distribution(pkg).version


# PINNED_CONDA_LIBS are the libraries that metaflow depends on for execution
# and are needed within a conda environment
def get_pinned_conda_libs(python_version):
    return {
        "requests": ">=2.21.0",
        "boto3": ">=1.14.0",
    }


# Check if there are extensions to Metaflow to load and override everything
try:
    from metaflow.extension_support import get_modules

    ext_modules = get_modules("config")
    for m in ext_modules:
        # We load into globals whatever we have in extension_module
        # We specifically exclude any modules that may be included (like sys, os, etc)
        for n, o in m.module.__dict__.items():
            if n == "DEBUG_OPTIONS":
                DEBUG_OPTIONS.extend(o)
                for typ in o:
                    vars()["METAFLOW_DEBUG_%s" % typ.upper()] = from_conf(
                        "METAFLOW_DEBUG_%s" % typ.upper()
                    )
            elif not n.startswith("__") and not isinstance(o, types.ModuleType):
                globals()[n] = o
finally:
    # Erase all temporary names to avoid leaking things
    for _n in ["m", "n", "o", "ext_modules", "get_modules"]:
        try:
            del globals()[_n]
        except KeyError:
            pass
    del globals()["_n"]
