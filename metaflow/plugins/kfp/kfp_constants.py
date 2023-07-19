# Constants used in run MF flow on KFP

# Defaults for running MF on KFP
import os

KFP_METAFLOW_FOREACH_SPLITS_PATH = "/tmp/kfp_metaflow_foreach_splits_dict.json"
PRECEDING_COMPONENT_INPUTS_PATH = "/tmp/preceding_component_inputs.json"

SPLIT_INDEX_SEPARATOR = "_"
PASSED_IN_SPLIT_INDEXES_ENV_NAME = "PASSED_IN_SPLIT_INDEXES_ENV_NAME"
TASK_ID_ENV_NAME = "TASK_ID_ENV_NAME"
SPLIT_INDEX_ENV_NAME = "SPLIT_INDEX_ENV_NAME"
INPUT_PATHS_ENV_NAME = "INPUT_PATHS_ENV_NAME"
RETRY_COUNT = "MF_ATTEMPT"
S3_SENSOR_RETRY_COUNT = 7
EXIT_HANDLER_RETRY_COUNT = 7

STEP_ENVIRONMENT_VARIABLES = "/tmp/step-environment-variables.sh"

# Log Arguments
LOGS_DIR = "/opt/metaflow_volume/metaflow_logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)

KFP_CLI_DEFAULT_RETRY = 3
