import base64

CONFIG = {{{metaflow_workflow_compile_params}}}

{{{AIRFLOW_UTILS}}}

dag = Workflow.from_dict(CONFIG).compile()
with dag:
    pass
