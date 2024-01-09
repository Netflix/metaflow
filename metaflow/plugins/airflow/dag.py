# pyright: reportGeneralTypeIssues=false


# Deployed on {{deployed_on}}

CONFIG = {{{config}}}  # pyright: ignore [reportUndefinedVariable]

{{{utils}}}  # pyright: ignore [reportUndefinedVariable, reportUnusedExpression]

dag = Workflow.from_dict(CONFIG).compile()  # pyright: ignore [reportUndefinedVariable]
with dag:
    pass
