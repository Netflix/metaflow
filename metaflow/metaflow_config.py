import json
import logging
import os
import sys
import types

import pkg_resources

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

_all_configs = {}


def config_values():
    for name, (value, conv_func) in _all_configs.items():
        if value is not None:
            yield name, conv_func(value)


def from_conf(name, default=None, validate_fn=None, propagate=True):
    """
    First try to pull value from environment, then from metaflow config JSON.

    Prior to a value being returned, we will validate using validate_fn (if provided).
    Only non-None values are validated.

    validate_fn should accept (name, value).
    If the value validates, return None, else raise an MetaflowException.
    """
    env_name = "METAFLOW_%s" % name
    value = os.environ.get(env_name, METAFLOW_CONFIG.get(env_name, default))
    if validate_fn and value is not None:
        validate_fn(env_name, value)
    if default is not None:
        if default == "{}":
            try:
                value = json.loads(value)
                _all_configs[env_name] = (value, json.dumps)
                return value
            except json.JSONDecodeError:
                raise ValueError(
                    "Expected a valid JSON for %s, got: %s" % (env_name, value)
                )
        else:
            try:
                value = type(default)(value)
            except ValueError:
                raise ValueError(
                    "Expected a %s for %s, got: %s" % (type(default), env_name, value)
                )
    _all_configs[env_name] = (value, str)
    return value


def _get_validate_choice_fn(choices):
    """Returns a validate_fn for use with from_conf().
    The validate_fn will check a value against a list of allowed choices.
    """

    def _validate_choice(name, value):
        if value not in choices:
            raise MetaflowException(
                "%s must be set to one of %s. Got '%s'." % (name, choices, value)
            )

    return _validate_choice


###
# Default configuration
###

# Propagate defaults to be able to read what the user sets in configuration files/envs;
# this will *not* propagate things set on the command line
DEFAULT_DATASTORE = from_conf("DEFAULT_DATASTORE", "local")
DEFAULT_ENVIRONMENT = from_conf("DEFAULT_ENVIRONMENT", "local")
DEFAULT_EVENT_LOGGER = from_conf("DEFAULT_EVENT_LOGGER", "nullSidecarLogger")
DEFAULT_METADATA = from_conf("DEFAULT_METADATA", "local")
DEFAULT_MONITOR = from_conf("DEFAULT_MONITOR", "nullSidecarMonitor")
DEFAULT_PACKAGE_SUFFIXES = from_conf("DEFAULT_PACKAGE_SUFFIXES", ".py,.R,.RDS")
DEFAULT_AWS_CLIENT_PROVIDER = from_conf("DEFAULT_AWS_CLIENT_PROVIDER", "boto3")


###
# Datastore configuration
###
# Path to the local directory to store artifacts for 'local' datastore.
DATASTORE_LOCAL_DIR = ".metaflow"
DATASTORE_SYSROOT_LOCAL = from_conf("DATASTORE_SYSROOT_LOCAL", propagate=False)
# S3 bucket and prefix to store artifacts for 's3' datastore.
DATASTORE_SYSROOT_S3 = from_conf("DATASTORE_SYSROOT_S3")
# Azure Blob Storage container and blob prefix
DATASTORE_SYSROOT_AZURE = from_conf("DATASTORE_SYSROOT_AZURE")


###
# Datastore local cache
###
# Cache is only local on the host machine so do not propagate values

# Path to the client cache
CLIENT_CACHE_PATH = from_conf(
    "CLIENT_CACHE_PATH", "/tmp/metaflow_client", propagate=False
)
# Maximum size (in bytes) of the cache
CLIENT_CACHE_MAX_SIZE = from_conf("CLIENT_CACHE_MAX_SIZE", 10000, propagate=False)
# Maximum number of cached Flow and TaskDatastores in the cache
CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT = from_conf(
    "CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT", 50, propagate=False
)
CLIENT_CACHE_MAX_TASKDATASTORE_COUNT = from_conf(
    "CLIENT_CACHE_MAX_TASKDATASTORE_COUNT",
    CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT * 100,
    propagate=False,
)


###
# Datatools (S3) configuration
###
# Both values are included in CLIENT_PARAMS so don't propagate
# S3 endpoint url
S3_ENDPOINT_URL = from_conf("S3_ENDPOINT_URL", propagate=False)
S3_VERIFY_CERTIFICATE = from_conf("S3_VERIFY_CERTIFICATE", propagate=False)

# S3 retry configuration
# This is useful if you want to "fail fast" on S3 operations; use with caution
# though as this may increase failures. Note that this is the number of *retries*
# so setting it to 0 means each operation will be tried once.
S3_RETRY_COUNT = from_conf("S3_RETRY_COUNT", 7)

# Threshold to start printing warnings for an AWS retry
RETRY_WARNING_THRESHOLD = 3

# S3 datatools root location
DATATOOLS_SUFFIX = from_conf("DATATOOLS_SUFFIX", "data")
DATATOOLS_S3ROOT = from_conf(
    "DATATOOLS_S3ROOT",
    os.path.join(DATASTORE_SYSROOT_S3, DATATOOLS_SUFFIX)
    if DATASTORE_SYSROOT_S3
    else None,
)

DATATOOLS_CLIENT_PARAMS = from_conf("DATATOOLS_CLIENT_PARAMS", "{}")
if S3_ENDPOINT_URL:
    DATATOOLS_CLIENT_PARAMS["endpoint_url"] = S3_ENDPOINT_URL
if S3_VERIFY_CERTIFICATE:
    DATATOOLS_CLIENT_PARAMS["verify"] = S3_VERIFY_CERTIFICATE

DATATOOLS_SESSION_VARS = from_conf("DATATOOLS_SESSION_VARS", "{}")

# Azure datatools root location
# Note: we do not expose an actual datatools library for Azure (like we do for S3)
# Similar to DATATOOLS_LOCALROOT, this is used ONLY by the IncludeFile's internal implementation.
DATATOOLS_AZUREROOT = from_conf(
    "DATATOOLS_AZUREROOT",
    os.path.join(DATASTORE_SYSROOT_AZURE, DATATOOLS_SUFFIX)
    if DATASTORE_SYSROOT_AZURE
    else None,
)
# Local datatools root location
DATATOOLS_LOCALROOT = from_conf(
    "DATATOOLS_LOCALROOT",
    os.path.join(DATASTORE_SYSROOT_LOCAL, DATATOOLS_SUFFIX)
    if DATASTORE_SYSROOT_LOCAL
    else None,
    propagate=False,
)

# The root directory to save artifact pulls in, when using S3 or Azure
ARTIFACT_LOCALROOT = from_conf("ARTIFACT_LOCALROOT", os.getcwd(), propagate=False)

# Cards related config variables
CARD_SUFFIX = "mf.cards"
CARD_LOCALROOT = from_conf("METAFLOW_CARD_LOCALROOT", propagate=False)
CARD_S3ROOT = from_conf(
    "CARD_S3ROOT",
    os.path.join(DATASTORE_SYSROOT_S3, CARD_SUFFIX) if DATASTORE_SYSROOT_S3 else None,
)
CARD_AZUREROOT = from_conf(
    "CARD_AZUREROOT",
    os.path.join(DATASTORE_SYSROOT_AZURE, CARD_SUFFIX)
    if DATASTORE_SYSROOT_AZURE
    else None,
)
CARD_NO_WARNING = from_conf("CARD_NO_WARNING", False)

SKIP_CARD_DUALWRITE = from_conf("SKIP_CARD_DUALWRITE", False)

# Azure storage account URL
AZURE_STORAGE_BLOB_SERVICE_ENDPOINT = from_conf("AZURE_STORAGE_BLOB_SERVICE_ENDPOINT")

# Azure storage can use process-based parallelism instead of threads.
# Processes perform better for high throughput workloads (e.g. many huge artifacts)
AZURE_STORAGE_WORKLOAD_TYPE = from_conf(
    "AZURE_STORAGE_WORKLOAD_TYPE",
    default=None,
    validate_fn=_get_validate_choice_fn(["general", "high_throughput"]),
)


###
# Metadata configuration
###
SERVICE_URL = from_conf("SERVICE_URL")
SERVICE_NUM_RETRIES = from_conf("SERVICE_RETRY_COUNT", 5)
SERVICE_AUTH_KEY = from_conf("METAFLOW_SERVICE_AUTH_KEY", propagate=False)
SERVICE_HEADERS = from_conf("METAFLOW_SERVICE_HEADERS", "{}")
if SERVICE_AUTH_KEY is not None:
    SERVICE_HEADERS["x-api-key"] = SERVICE_AUTH_KEY
# Checks version compatibility with Metadata service
SERVICE_VERSION_CHECK = from_conf(
    "METAFLOW_SERVICE_VERSION_CHECK", True, propagate=False
)

# Default container image
DEFAULT_CONTAINER_IMAGE = from_conf("DEFAULT_CONTAINER_IMAGE", propagate=False)
# Default container registry
DEFAULT_CONTAINER_REGISTRY = from_conf("DEFAULT_CONTAINER_REGISTRY", propagate=False)

###
# AWS Batch configuration
###
# IAM role for AWS Batch container with Amazon S3 access
# (and AWS DynamoDb access for AWS StepFunctions, if enabled)
ECS_S3_ACCESS_IAM_ROLE = from_conf("ECS_S3_ACCESS_IAM_ROLE", propagate=False)
# IAM role for AWS Batch container for AWS Fargate
ECS_FARGATE_EXECUTION_ROLE = from_conf("ECS_FARGATE_EXECUTION_ROLE", propagate=False)
# Job queue for AWS Batch
BATCH_JOB_QUEUE = from_conf("BATCH_JOB_QUEUE", propagate=False)
# Default container image for AWS Batch
BATCH_CONTAINER_IMAGE = from_conf(
    "BATCH_CONTAINER_IMAGE", DEFAULT_CONTAINER_IMAGE, propagate=False
)
# Default container registry for AWS Batch
BATCH_CONTAINER_REGISTRY = from_conf(
    "BATCH_CONTAINER_REGISTRY", DEFAULT_CONTAINER_REGISTRY, propagate=False
)
# Metadata service URL for AWS Batch
SERVICE_INTERNAL_URL = from_conf("SERVICE_INTERNAL_URL", SERVICE_URL)

# Assign resource tags to AWS Batch jobs. Set to False by default since
# it requires `Batch:TagResource` permissions which may not be available
# in all Metaflow deployments. Hopefully, some day we can flip the
# default to True.
BATCH_EMIT_TAGS = from_conf("BATCH_EMIT_TAGS", False, propagate=False)

###
# AWS Step Functions configuration
###
# IAM role for AWS Step Functions with AWS Batch and AWS DynamoDb access
# https://docs.aws.amazon.com/step-functions/latest/dg/batch-iam.html
SFN_IAM_ROLE = from_conf("SFN_IAM_ROLE", propagate=False)
# AWS DynamoDb Table name (with partition key - `pathspec` of type string)
SFN_DYNAMO_DB_TABLE = from_conf("SFN_DYNAMO_DB_TABLE", propagate=False)
# IAM role for AWS Events with AWS Step Functions access
# https://docs.aws.amazon.com/eventbridge/latest/userguide/auth-and-access-control-eventbridge.html
EVENTS_SFN_ACCESS_IAM_ROLE = from_conf("EVENTS_SFN_ACCESS_IAM_ROLE", propagate=False)
# Prefix for AWS Step Functions state machines. Set to stack name for Metaflow
# sandbox.
SFN_STATE_MACHINE_PREFIX = from_conf("SFN_STATE_MACHINE_PREFIX", propagate=False)
# Optional AWS CloudWatch Log Group ARN for emitting AWS Step Functions state
# machine execution logs. This needs to be available when using the
# `step-functions create --log-execution-history` command.
SFN_EXECUTION_LOG_GROUP_ARN = from_conf("SFN_EXECUTION_LOG_GROUP_ARN", propagate=False)

###
# Kubernetes configuration
###
# Kubernetes namespace to use for all objects created by Metaflow
KUBERNETES_NAMESPACE = from_conf("KUBERNETES_NAMESPACE", "default", propagate=False)
# Default service account to use by K8S jobs created by Metaflow
KUBERNETES_SERVICE_ACCOUNT = from_conf("KUBERNETES_SERVICE_ACCOUNT", propagate=False)
# Default node selectors to use by K8S jobs created by Metaflow - foo=bar,baz=bab
KUBERNETES_NODE_SELECTOR = from_conf("KUBERNETES_NODE_SELECTOR", "", propagate=False)
KUBERNETES_SECRETS = from_conf("KUBERNETES_SECRETS", "", propagate=False)
# Default GPU vendor to use by K8S jobs created by Metaflow (supports nvidia, amd)
KUBERNETES_GPU_VENDOR = from_conf("KUBERNETES_GPU_VENDOR", "nvidia", propagate=False)
# Default container image for K8S
KUBERNETES_CONTAINER_IMAGE = from_conf(
    "KUBERNETES_CONTAINER_IMAGE", DEFAULT_CONTAINER_IMAGE, propagate=False
)
# Default container registry for K8S
KUBERNETES_CONTAINER_REGISTRY = from_conf(
    "KUBERNETES_CONTAINER_REGISTRY", DEFAULT_CONTAINER_REGISTRY, propagate=False
)

##
# Airflow Configuration
##
# This configuration sets `startup_timeout_seconds` in airflow's KubernetesPodOperator.
AIRFLOW_KUBERNETES_STARTUP_TIMEOUT_SECONDS = from_conf(
    "AIRFLOW_KUBERNETES_STARTUP_TIMEOUT_SECONDS", 60 * 60, propagate=False
)
# This configuration sets `kubernetes_conn_id` in airflow's KubernetesPodOperator.
AIRFLOW_KUBERNETES_CONN_ID = from_conf(
    "AIRFLOW_KUBERNETES_CONN_ID", None, propagate=False
)


###
# Conda configuration
###
# Conda package root location on S3
CONDA_PACKAGE_S3ROOT = from_conf("CONDA_PACKAGE_S3ROOT")
# Conda package root location on Azure
CONDA_PACKAGE_AZUREROOT = from_conf("CONDA_PACKAGE_AZUREROOT")

# Use an alternate dependency resolver for conda packages instead of conda
# Mamba promises faster package dependency resolution times, which
# should result in an appreciable speedup in flow environment initialization.
CONDA_DEPENDENCY_RESOLVER = from_conf(
    "CONDA_DEPENDENCY_RESOLVER", "conda", propagate=False
)

###
# Debug configuration
###
DEBUG_OPTIONS = ["subcommand", "sidecar", "s3client"]

for typ in DEBUG_OPTIONS:
    vars()["DEBUG_%s" % typ.upper()] = from_conf("DEBUG_%s" % typ.upper())

###
# AWS Sandbox configuration
###
# Boolean flag for metaflow AWS sandbox access
AWS_SANDBOX_ENABLED = from_conf("AWS_SANDBOX_ENABLED", False, propagate=False)
# Metaflow AWS sandbox auth endpoint
AWS_SANDBOX_STS_ENDPOINT_URL = SERVICE_URL
# Metaflow AWS sandbox API auth key
AWS_SANDBOX_API_KEY = from_conf("AWS_SANDBOX_API_KEY", propagate=False)
# Internal Metadata URL
AWS_SANDBOX_INTERNAL_SERVICE_URL = from_conf(
    "AWS_SANDBOX_INTERNAL_SERVICE_URL", propagate=False
)
# AWS region
AWS_SANDBOX_REGION = from_conf("AWS_SANDBOX_REGION", propagate=False)


# Finalize configuration
if AWS_SANDBOX_ENABLED:
    os.environ["AWS_DEFAULT_REGION"] = AWS_SANDBOX_REGION
    SERVICE_INTERNAL_URL = AWS_SANDBOX_INTERNAL_SERVICE_URL
    SERVICE_HEADERS["x-api-key"] = AWS_SANDBOX_API_KEY
    SFN_STATE_MACHINE_PREFIX = from_conf("AWS_SANDBOX_STACK_NAME", propagate=False)

KUBERNETES_SANDBOX_INIT_SCRIPT = from_conf(
    "KUBERNETES_SANDBOX_INIT_SCRIPT", propagate=False
)

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
def get_pinned_conda_libs(python_version, datastore_type):
    pins = {
        "requests": ">=2.21.0",
    }
    if datastore_type == "s3":
        pins["boto3"] = ">=1.14.0"
    elif datastore_type == "azure":
        pins["azure-identity"] = ">=1.10.0"
        pins["azure-storage-blob"] = ">=12.12.0"
    elif datastore_type == "local":
        pass
    else:
        raise MetaflowException(
            msg="conda lib pins for datastore %s are undefined" % (datastore_type,)
        )
    return pins


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
                    vars()["DEBUG_%s" % typ.upper()] = from_conf(
                        "DEBUG_%s" % typ.upper()
                    )
            elif n == "get_pinned_conda_libs":

                def _new_get_pinned_conda_libs(
                    python_version, datastore_type, f1=globals()[n], f2=o
                ):
                    d1 = f1(python_version, datastore_type)
                    d2 = f2(python_version, datastore_type)
                    for k, v in d2.items():
                        d1[k] = v if k not in d1 else ",".join([d1[k], v])
                    return d1

                globals()[n] = _new_get_pinned_conda_libs
            elif not n.startswith("__") and not isinstance(o, types.ModuleType):
                globals()[n] = o
finally:
    # Erase all temporary names to avoid leaking things
    for _n in [
        "m",
        "n",
        "o",
        "type",
        "ext_modules",
        "get_modules",
        "_new_get_pinned_conda_libs",
        "d1",
        "d2",
        "k",
        "v",
        "f1",
        "f2",
    ]:
        try:
            del globals()[_n]
        except KeyError:
            pass
    del globals()["_n"]
