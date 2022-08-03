# Deployed on {{deployed_on}}

CONFIG = {{{config}}}

{{{utils}}}

dag = Workflow.from_dict(CONFIG).compile()
with dag:
    pass
