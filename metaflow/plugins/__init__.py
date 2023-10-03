from metaflow.extension_support.plugins import (
    merge_lists,
    process_plugins,
    resolve_plugins,
)

# Add new CLI commands here
CLIS_DESC = [
    ("package", ".package_cli.cli"),
    ("batch", ".aws.batch.batch_cli.cli"),
    ("kubernetes", ".kubernetes.kubernetes_cli.cli"),
    ("step-functions", ".aws.step_functions.step_functions_cli.cli"),
    ("airflow", ".airflow.airflow_cli.cli"),
    ("argo-workflows", ".argo.argo_workflows_cli.cli"),
    ("card", ".cards.card_cli.cli"),
    ("tag", ".tag_cli.cli"),
]

from .test_unbounded_foreach_decorator import InternalTestUnboundedForeachInput

# Add new step decorators here
STEP_DECORATORS_DESC = [
    ("catch", ".catch_decorator.CatchDecorator"),
    ("timeout", ".timeout_decorator.TimeoutDecorator"),
    ("environment", ".environment_decorator.EnvironmentDecorator"),
    ("secrets", ".secrets.secrets_decorator.SecretsDecorator"),
    ("parallel", ".parallel_decorator.ParallelDecorator"),
    ("retry", ".retry_decorator.RetryDecorator"),
    ("resources", ".resources_decorator.ResourcesDecorator"),
    ("batch", ".aws.batch.batch_decorator.BatchDecorator"),
    ("kubernetes", ".kubernetes.kubernetes_decorator.KubernetesDecorator"),
    (
        "argo_workflows_internal",
        ".argo.argo_workflows_decorator.ArgoWorkflowsInternalDecorator",
    ),
    (
        "step_functions_internal",
        ".aws.step_functions.step_functions_decorator.StepFunctionsInternalDecorator",
    ),
    (
        "unbounded_test_foreach_internal",
        ".test_unbounded_foreach_decorator.InternalTestUnboundedForeachDecorator",
    ),
    ("card", ".cards.card_decorator.CardDecorator"),
    ("pytorch_parallel", ".frameworks.pytorch.PytorchParallelDecorator"),
    ("airflow_internal", ".airflow.airflow_decorator.AirflowInternalDecorator"),
    ("pypi", ".pypi.pypi_decorator.PyPIStepDecorator"),
    ("conda", ".pypi.conda_decorator.CondaStepDecorator"),
]

# Add new flow decorators here
# Every entry here becomes a class-level flow decorator.
# Add an entry here if you need a new flow-level annotation. Be
# careful with the choice of name though - they become top-level
# imports from the metaflow package.
FLOW_DECORATORS_DESC = [
    ("schedule", ".aws.step_functions.schedule_decorator.ScheduleDecorator"),
    ("project", ".project_decorator.ProjectDecorator"),
    ("trigger", ".events_decorator.TriggerDecorator"),
    ("trigger_on_finish", ".events_decorator.TriggerOnFinishDecorator"),
    ("pypi_base", ".pypi.pypi_decorator.PyPIFlowDecorator"),
    ("conda_base", ".pypi.conda_decorator.CondaFlowDecorator"),
]

# Add environments here
ENVIRONMENTS_DESC = [
    ("conda", ".pypi.conda_environment.CondaEnvironment"),
    ("pypi", ".pypi.pypi_environment.PyPIEnvironment"),
]

# Add metadata providers here
METADATA_PROVIDERS_DESC = [
    ("service", ".metadata.service.ServiceMetadataProvider"),
    ("local", ".metadata.local.LocalMetadataProvider"),
]

# Add datastore here
DATASTORES_DESC = [
    ("local", ".datastores.local_storage.LocalStorage"),
    ("s3", ".datastores.s3_storage.S3Storage"),
    ("azure", ".datastores.azure_storage.AzureStorage"),
    ("gs", ".datastores.gs_storage.GSStorage"),
]

# Add non monitoring/logging sidecars here
SIDECARS_DESC = [
    (
        "save_logs_periodically",
        "..mflog.save_logs_periodically.SaveLogsPeriodicallySidecar",
    ),
    ("heartbeat", "metaflow.metadata.heartbeat.MetadataHeartBeat"),
]

# Add logging sidecars here
LOGGING_SIDECARS_DESC = [
    ("debugLogger", ".debug_logger.DebugEventLogger"),
    ("nullSidecarLogger", "metaflow.event_logger.NullEventLogger"),
]

# Add monitor sidecars here
MONITOR_SIDECARS_DESC = [
    ("debugMonitor", ".debug_monitor.DebugMonitor"),
    ("nullSidecarMonitor", "metaflow.monitor.NullMonitor"),
]

# Add AWS client providers here
AWS_CLIENT_PROVIDERS_DESC = [("boto3", ".aws.aws_client.Boto3ClientProvider")]

# Add Airflow sensor related flow decorators
SENSOR_FLOW_DECORATORS = [
    ("airflow_external_task_sensor", ".airflow.sensors.ExternalTaskSensorDecorator"),
    ("airflow_s3_key_sensor", ".airflow.sensors.S3KeySensorDecorator"),
]

FLOW_DECORATORS_DESC += SENSOR_FLOW_DECORATORS

SECRETS_PROVIDERS_DESC = [
    ("inline", ".secrets.inline_secrets_provider.InlineSecretsProvider"),
    (
        "aws-secrets-manager",
        ".aws.secrets_manager.aws_secrets_manager_secrets_provider.AwsSecretsManagerSecretsProvider",
    ),
]

process_plugins(globals())


def get_plugin_cli():
    return resolve_plugins("cli")


STEP_DECORATORS = resolve_plugins("step_decorator")
FLOW_DECORATORS = resolve_plugins("flow_decorator")
ENVIRONMENTS = resolve_plugins("environment")
METADATA_PROVIDERS = resolve_plugins("metadata_provider")
DATASTORES = resolve_plugins("datastore")
SIDECARS = resolve_plugins("sidecar")
LOGGING_SIDECARS = resolve_plugins("logging_sidecar")
MONITOR_SIDECARS = resolve_plugins("monitor_sidecar")

SIDECARS.update(LOGGING_SIDECARS)
SIDECARS.update(MONITOR_SIDECARS)

AWS_CLIENT_PROVIDERS = resolve_plugins("aws_client_provider")
SECRETS_PROVIDERS = resolve_plugins("secrets_provider")

from .cards.card_modules import MF_EXTERNAL_CARDS

# Cards; due to the way cards were designed, it is harder to make them fit
# in the resolve_plugins mechanism. This should be OK because it is unlikely that
# cards will need to be *removed*. No card should be too specific (for example, no
# card should be something just for Airflow, or Argo or step-functions -- those should
# be added externally).
from .cards.card_modules.basic import (
    BlankCard,
    DefaultCard,
    DefaultCardJSON,
    ErrorCard,
    TaskSpecCard,
)
from .cards.card_modules.test_cards import (
    TestEditableCard,
    TestEditableCard2,
    TestErrorCard,
    TestMockCard,
    TestNonEditableCard,
    TestPathSpecCard,
    TestTimeoutCard,
)

CARDS = [
    DefaultCard,
    TaskSpecCard,
    ErrorCard,
    BlankCard,
    TestErrorCard,
    TestTimeoutCard,
    TestMockCard,
    TestPathSpecCard,
    TestEditableCard,
    TestEditableCard2,
    TestNonEditableCard,
    BlankCard,
    DefaultCardJSON,
]
merge_lists(CARDS, MF_EXTERNAL_CARDS, "type")
