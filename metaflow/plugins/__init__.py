import importlib
import traceback

from metaflow.extension_support import get_modules, multiload_all, _ext_debug

_plugin_categories = {
    "step_decorator": lambda x: x.name,
    "flow_decorator": lambda x: x.name,
    "environment": lambda x: x.TYPE,
    "metadata": lambda x: x.TYPE,
    "datastore": lambda x: x.TYPE,
    "sidecar": None,
    "logging_sidecar": None,
    "monitor_sidecar": None,
    "aws_provider": lambda x: x.name,
    "cli": lambda x: list(x.commands)[0]
    if len(x.commands) == 1
    else "too many commands",
}


def add_plugin_support(g):
    for category in _plugin_categories:
        g.update(
            {
                "__%ss_exclusions" % category: [],
                "__%ss" % category: {},
            }
        )

        def _exclude(names, exclude_from=g["__%ss_exclusions" % category]):
            exclude_from.extend(names)

        def _add(
            name,
            path,
            cls_name,
            pkg=g["__package__"],
            add_to=g["__%ss" % category],
            category=category,
        ):
            if path[0] == ".":
                pkg_components = pkg.split(".")
                i = 1
                while path[i] == ".":
                    i += 1
                # We deal with multiple periods at the start
                if i > len(pkg_components):
                    raise ValueError("Path '%s' exits out of metaflow module" % path)
                path = (
                    ".".join(pkg_components[: -i + 1] if i > 1 else pkg_components)
                    + path[i - 1 :]
                )
            _ext_debug(
                "    Adding %s: %s from %s.%s" % (category, name, path, cls_name)
            )
            add_to[name] = (path, cls_name)

        g.update(
            {
                "%s_add" % category: _add,
                "%s_exclude" % category: _exclude,
            }
        )


add_plugin_support(globals())

# Add new CLI commands here
cli_add("package", ".package_cli", "cli")
cli_add("batch", ".aws.batch.batch_cli", "cli")
cli_add("kubernetes", ".kubernetes.kubernetes_cli", "cli")
cli_add("step-functions", ".aws.step_functions.step_functions_cli", "cli")
cli_add("airflow", ".airflow.airflow_cli", "cli")
cli_add("argo-workflows", ".argo.argo_workflows_cli", "cli")
cli_add("card", ".cards.card_cli", "cli")
cli_add("tag", ".tag_cli", "cli")

# Add new step decorators here
step_decorator_add("catch", ".catch_decorator", "CatchDecorator")
step_decorator_add("timeout", ".timeout_decorator", "TimeoutDecorator")
step_decorator_add("environment", ".environment_decorator", "EnvironmentDecorator")
step_decorator_add("parallel", ".parallel_decorator", "ParallelDecorator")
step_decorator_add("retry", ".retry_decorator", "RetryDecorator")
step_decorator_add("resources", ".resources_decorator", "ResourcesDecorator")
step_decorator_add("batch", ".aws.batch.batch_decorator", "BatchDecorator")
step_decorator_add(
    "kubernetes", ".kubernetes.kubernetes_decorator", "KubernetesDecorator"
)
step_decorator_add(
    "argo_workflows_internal",
    ".argo.argo_workflows_decorator",
    "ArgoWorkflowsInternalDecorator",
)
step_decorator_add(
    "step_functions_internal",
    ".aws.step_functions.step_functions_decorator",
    "StepFunctionsInternalDecorator",
)
step_decorator_add(
    "unbounded_test_foreach_internal",
    ".test_unbounded_foreach_decorator",
    "InternalTestUnboundedForeachDecorator",
)
from .test_unbounded_foreach_decorator import InternalTestUnboundedForeachInput

step_decorator_add("conda", ".conda.conda_step_decorator", "CondaStepDecorator")
step_decorator_add("card", ".cards.card_decorator", "CardDecorator")
step_decorator_add(
    "pytorch_parallel", ".frameworks.pytorch", "PytorchParallelDecorator"
)
step_decorator_add(
    "airflow_internal", ".airflow.airflow_decorator", "AirflowInternalDecorator"
)

# Add new flow decorators here
# Every entry here becomes a class-level flow decorator.
# Add an entry here if you need a new flow-level annotation. Be
# careful with the choice of name though - they become top-level
# imports from the metaflow package.
flow_decorator_add("conda_base", ".conda.conda_flow_decorator", "CondaFlowDecorator")
flow_decorator_add(
    "schedule", ".aws.step_functions.schedule_decorator", "ScheduleDecorator"
)
flow_decorator_add("project", ".project_decorator", "ProjectDecorator")

# Add environments here
environment_add("conda", ".conda.conda_environment", "CondaEnvironment")

# Add metadata providers here
metadata_add("service", ".metadata.service", "ServiceMetadataProvider")
metadata_add("local", ".metadata.local", "LocalMetadataProvider")

# Add datastore here
datastore_add("local", ".datastores.local_storage", "LocalStorage")
datastore_add("s3", ".datastores.s3_storage", "S3Storage")
datastore_add("azure", ".datastores.azure_storage", "AzureStorage")

# Add non monitoring/logging sidecars here
sidecar_add(
    "save_logs_periodically",
    "..mflog.save_logs_periodically",
    "SaveLogsPeriodicallySidecar",
)
sidecar_add("heartbeat", "metaflow.metadata.heartbeat", "MetadataHeartBeat")

# Add logging sidecars here
logging_sidecar_add("debugLogger", ".debug_logger", "DebugEventLogger")
logging_sidecar_add("nullSidecarLogger", "metaflow.event_logger", "NullEventLogger")

# Add monitor sidecars here
monitor_sidecar_add("debugMonitor", ".debug_monitor", "DebugMonitor")
monitor_sidecar_add("nullSidecarMonitor", "metaflow.monitor", "NullMonitor")

# Add AWS client providers here
aws_provider_add("boto3", ".aws.aws_client", "Boto3ClientProvider")


def _merge_plugins(base, module, category):
    # Add to base things from the module and remove anything the module wants to remove
    base.update(getattr(module, "__%ss" % category, {}))
    excl = getattr(module, "__%ss_exclusions" % category, [])
    for n in excl:
        _ext_debug("    Module '%s' removing %s %s" % (module.__name__, category, n))
        if n in base:
            del base[n]


def _merge_lists(base, overrides, attr):
    # Merge two lists of classes by comparing them for equality using 'attr'.
    # This function prefers anything in 'overrides'. In other words, if a class
    # is present in overrides and matches (according to the equality criterion) a class in
    # base, it will be used instead of the one in base.
    l = list(overrides)
    existing = set([getattr(o, attr) for o in overrides])
    l.extend([d for d in base if getattr(d, attr) not in existing])
    base[:] = l[:]


def _lazy_plugin_resolve(category):
    d = globals()["__%ss" % category]
    name_extractor = _plugin_categories[category]
    if not name_extractor:
        # If we have no name function, it means we just use the name in the dictionary
        # and we return a dictionary.
        to_return = {}
    else:
        to_return = []
    for name, (path, cls_name) in d.items():
        plugin_module = importlib.import_module(path)
        cls = getattr(plugin_module, cls_name, None)
        if cls is None:
            raise ValueError("'%s' not found in module '%s'" % (cls_name, path))
        if name_extractor and name_extractor(cls) != name:
            raise ValueError(
                "%s.%s: expected name to be '%s' but got '%s' instead"
                % (path, cls_name, name, name_extractor(cls))
            )
        globals()[cls_name] = cls
        if name_extractor is not None:
            to_return.append(cls)
        else:
            to_return[name] = cls
    return to_return


try:
    _modules_to_import = get_modules("plugins")

    multiload_all(_modules_to_import, "plugins", globals())

    # Build an ordered list
    for c in _plugin_categories:
        for m in _modules_to_import:
            _merge_plugins(globals()["__%ss" % c], m.module, c)
except Exception as e:
    _ext_debug("\tWARNING: ignoring all plugins due to error during import: %s" % e)
    print(
        "WARNING: Plugins did not load -- ignoring all of them which may not "
        "be what you want: %s" % e
    )
    traceback.print_exc()


def get_plugin_cli():
    return _lazy_plugin_resolve("cli")


STEP_DECORATORS = _lazy_plugin_resolve("step_decorator")
FLOW_DECORATORS = _lazy_plugin_resolve("flow_decorator")
ENVIRONMENTS = _lazy_plugin_resolve("environment")
METADATA_PROVIDERS = _lazy_plugin_resolve("metadata")
DATASTORES = _lazy_plugin_resolve("datastore")
SIDECARS = _lazy_plugin_resolve("sidecar")
LOGGING_SIDECARS = _lazy_plugin_resolve("logging_sidecar")
MONITOR_SIDECARS = _lazy_plugin_resolve("monitor_sidecar")

SIDECARS.update(LOGGING_SIDECARS)
SIDECARS.update(MONITOR_SIDECARS)

AWS_CLIENT_PROVIDERS = _lazy_plugin_resolve("aws_provider")

# Cards; due to the way cards were designed, it is harder to make them fit
# in the _lazy_plugin_resolve mechanism. This should be OK because it is unlikely that
# cards will need to be *removed*. No card should be too specific (for example, no
# card should be something just for Airflow, or Argo or step-functions -- those should
# be added externally).
from .cards.card_modules.basic import (
    DefaultCard,
    TaskSpecCard,
    ErrorCard,
    BlankCard,
    DefaultCardJSON,
)
from .cards.card_modules.test_cards import (
    TestErrorCard,
    TestTimeoutCard,
    TestMockCard,
    TestPathSpecCard,
    TestEditableCard,
    TestEditableCard2,
    TestNonEditableCard,
)
from .cards.card_modules import MF_EXTERNAL_CARDS

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
_merge_lists(CARDS, MF_EXTERNAL_CARDS, "type")

# Erase all temporary names to avoid leaking things
# We leave '_lazy_plugin_resolve' and whatever it needs
# because it is used in a function (so it needs to stick around)
for _n in [
    "_merge_plugins",
    "_merge_lists",
    "get_modules",
    "multiload_all",
    "_ext_debug",
    "_modules_to_import",
    "c",
    "m",
    "e",
]:
    try:
        del globals()[_n]
    except KeyError:
        pass
del globals()["_n"]
