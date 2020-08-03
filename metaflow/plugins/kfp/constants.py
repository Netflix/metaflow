# Constants used in run MF flow on KFP

# Defaults for running MF on KFP
# TODO: remove this when we're able to automatically package code and use it on KFP. We currently need the user supplied code URL (or this default URL) in order to download the code that is to be run on KFP
DEFAULT_FLOW_CODE_URL = 'https://gist.githubusercontent.com/mon95/98d9d88571a0307a53b637d841295702/raw/df8bc1fa363f629271cfc1d34f3c2f7ca0b2e2cf/helloworld.py'
DEFAULT_DOWNLOADED_FLOW_FILENAME = 'downloaded_flow.py'

DEFAULT_KFP_YAML_OUTPUT_PATH = 'kfp_pipeline.yaml'
DEFAULT_RUN_NAME = 'run_mf_on_kfp'
DEFAULT_EXPERIMENT_NAME = 'mf-on-kfp-experiments'

# TODO: This should (probably) be moved outside
RUN_LINK_PREFIX = "https://kubeflow.corp.zillow-analytics-dev.zg-int.net/pipeline/#/runs/details/"
