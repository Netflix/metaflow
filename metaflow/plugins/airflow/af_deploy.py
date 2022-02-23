from metaflow.plugins.airflow.airflow_utils import Workflow

CONFIG = "{{{metaflow_workflow_compile_params}}}"

dag = Workflow.from_json(CONFIG).compile()
with dag:
    pass
