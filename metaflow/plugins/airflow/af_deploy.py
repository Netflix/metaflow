# Deployed on {{deployed_on}}

CONFIG = {{{metaflow_workflow_compile_params}}}

{{{AIRFLOW_UTILS}}}

dag = Workflow.from_dict(CONFIG).compile()
with dag:
    pass
