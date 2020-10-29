# Constants used in run MF flow on KFP

# Defaults for running MF on KFP
DEFAULT_KFP_YAML_OUTPUT_PATH = "kfp_pipeline.yaml"
DEFAULT_RUN_NAME = "run_mf_on_kfp"
DEFAULT_EXPERIMENT_NAME = "mf-on-kfp-experiments"

BASE_IMAGE = "hsezhiyan/metaflow-zillow:1.1"

KFP_METAFLOW_FOREACH_SPLITS_PATH = "/tmp/kfp_metaflow_foreach_splits_dict.json"

SPLIT_INDEX_SEPARATOR = "_"
PASSED_IN_SPLIT_INDEXES_ENV_NAME = "PASSED_IN_SPLIT_INDEXES_ENV_NAME"
TASK_ID_ENV_NAME = "TASK_ID_ENV_NAME"
SPLIT_INDEX_ENV_NAME = "SPLIT_INDEX_ENV_NAME"
INPUT_PATHS_ENV_NAME = "INPUT_PATHS_ENV_NAME"
