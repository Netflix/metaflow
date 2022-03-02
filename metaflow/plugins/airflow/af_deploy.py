import base64

CONFIG = "{{{metaflow_workflow_compile_params}}}"

{{{AIRFLOW_UTILS}}}

dag = Workflow.from_json(base64.b64decode(CONFIG).decode("utf-8")).compile()
with dag:
    pass
