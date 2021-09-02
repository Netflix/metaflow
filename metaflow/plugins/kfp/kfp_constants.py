# Constants used in run MF flow on KFP

# Defaults for running MF on KFP
import os

BASE_IMAGE = "hsezhiyan/metaflow-zillow:2.0"

KFP_METAFLOW_FOREACH_SPLITS_PATH = "/tmp/kfp_metaflow_foreach_splits_dict.json"
preceding_component_inputs_PATH = "/tmp/preceding_component_inputs.json"

SPLIT_INDEX_SEPARATOR = "_"
PASSED_IN_SPLIT_INDEXES_ENV_NAME = "PASSED_IN_SPLIT_INDEXES_ENV_NAME"
TASK_ID_ENV_NAME = "TASK_ID_ENV_NAME"
SPLIT_INDEX_ENV_NAME = "SPLIT_INDEX_ENV_NAME"
INPUT_PATHS_ENV_NAME = "INPUT_PATHS_ENV_NAME"
RETRY_COUNT = "MF_ATTEMPT"

STEP_ENVIRONMENT_VARIABLES = "/tmp/step-environment-variables.sh"

# Log Arguments
LOGS_DIR = "/opt/metaflow_volume/metaflow_logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)
