from typing import Dict
from metaflow._vendor import click
import logging

from metaflow.decorators import flow_decorators, FlowDecorator
from metaflow.graph import FlowGraph
from metaflow.plugins.aip import run_id_to_url

_logger = logging.getLogger(__name__)


def invoke_user_defined_exit_handler(
    graph: FlowGraph,
    flow_name: str,
    status: str,
    run_id: str,
    argo_workflow_uid: str,
    env_variables_json: str,
    flow_parameters_json: str,
    metaflow_configs_json: str,
    retries: int,
):
    """
    The environment variables that this depends on:
        METAFLOW_NOTIFY_ON_SUCCESS
        METAFLOW_NOTIFY_ON_ERROR
        METAFLOW_NOTIFY_EMAIL_SMTP_HOST
        METAFLOW_NOTIFY_EMAIL_SMTP_PORT
        METAFLOW_NOTIFY_EMAIL_FROM
        METAFLOW_SQS_URL_ON_ERROR
        METAFLOW_SQS_ROLE_ARN_ON_ERROR
        K8S_CLUSTER_ENV
        POD_NAMESPACE
        MF_ARGO_WORKFLOW_NAME
        METAFLOW_NOTIFY_EMAIL_BODY
    """
    import json
    import os

    env_variables: Dict[str, str] = json.loads(env_variables_json)

    def get_env(name, default=None) -> str:
        return env_variables.get(name, os.environ.get(name, default=default))

    argo_workflow_name = get_env("MF_ARGO_WORKFLOW_NAME", "")
    k8s_namespace = get_env("POD_NAMESPACE", "")
    argo_ui_url = run_id_to_url(argo_workflow_name, k8s_namespace, argo_workflow_uid)

    metaflow_configs: Dict[str, str] = json.loads(metaflow_configs_json)
    metaflow_configs_new: Dict[str, str] = {
        name: value for name, value in metaflow_configs.items() if value
    }
    if (
        not "METAFLOW_USER" in metaflow_configs_new
        or metaflow_configs_new["METAFLOW_USER"] is None
    ):
        metaflow_configs_new["METAFLOW_USER"] = "aip-user"

    # update os.environ if the value is not None
    # from metaflow_configs_new
    for name, value in metaflow_configs_new.items():
        if value is not None and os.environ.get(name, None) is None:
            os.environ[name] = value

    print(f"Flow completed with status={status}")

    udf_exit_handler: FlowDecorator = next(
        d for d in flow_decorators() if d.name == "exit_handler"
    )
    udf_exit_handler.attributes["func"](
        status=status,
        flow_parameters=json.loads(flow_parameters_json),
        argo_workflow_run_name=argo_workflow_name,
        metaflow_run_id=run_id,
        argo_ui_url=argo_ui_url,
        retries=int(retries),
    )
