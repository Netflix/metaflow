# Deployed on {{deployed_on}}

CONFIG = {{{config}}}  # type: ignore[name-defined]

{{{utils}}}  # type: ignore[name-defined]

dag = Workflow.from_dict(CONFIG).compile()  # type: ignore[name-defined]
with dag:
    pass
