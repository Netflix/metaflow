# This file can contain "shortcuts" to other parts of Metaflow (integrations)

# This is an alternative to providing an extension package where you would define
# these aliases in the toplevel file.

# It follows a similar pattern to plugins so that the these integration aliases can be
# turned on and off and avoid exposing things that are not necessarily needed/wanted.

from metaflow.extension_support.integrations import process_integration_aliases

# To enable an alias `metaflow.integrations.get_s3_client` to
# `metaflow.plugins.aws.aws_client.get_aws_client`, use the following:
#
# ALIASES_DESC = [("get_s3_client", ".plugins.aws.aws_client.get_aws_client")]
#
# ALIASES_DESC is a list of tuples:
#  - name: name of the integration alias
#  - obj: object it points to
#
ALIASES_DESC = [
    ("ArgoEvent", ".plugins.argo.argo_events.ArgoEvent"),
    ("SFNEvent", ".plugins.aws.step_functions.sfn_event.SFNEvent"),
    ("AirflowEvent", ".plugins.airflow.airflow_event.AirflowEvent"),
]

# Aliases can be enabled or disabled through configuration or extensions:
#  - ENABLED_INTEGRATION_ALIAS: list of alias names to enable.
#  - TOGGLE_INTEGRATION_ALIAS: if ENABLED_INTEGRATION_ALIAS is not set anywhere
#    (environment variable, configuration or extensions), list of integration aliases
#    to toggle (+<name> or <name> enables, -<name> disables) to build
#    ENABLED_INTEGRATION_ALIAS from the concatenation of the names in
#    ALIASES_DESC (concatenation of the names here as well as in the extensions).


def _resolve_event_providers():
    """Lazily resolve all registered event provider classes from the plugin system."""
    from metaflow.plugins import EVENT_PROVIDERS

    return EVENT_PROVIDERS


def send_event(name, payload=None, backend=None, **kwargs):
    """Send a trigger event to wake a deployed @trigger flow.

    Discovers event providers via the plugin system. Each provider class
    exposes ``TYPE`` (e.g. ``"argo-workflows"``) and ``is_configured()``
    which checks whether the required environment variables are set.

    When *backend* is ``None``, the first provider whose ``is_configured()``
    returns ``True`` is used.  When *backend* is given, it is matched against
    the provider's ``TYPE`` (dashes are normalised to underscores).

    New backends are added by registering an ``EVENT_PROVIDERS_DESC`` entry
    in ``metaflow/plugins/__init__.py`` — no changes to this function needed.

    Parameters
    ----------
    name : str
        Event name (must match the @trigger event name on the deployed flow).
    payload : dict, optional
        Key-value pairs delivered with the event, used to set parameters.
    backend : str, optional
        Override auto-detection.  Matched against provider TYPE
        (e.g. ``"argo-workflows"``, ``"step-functions"``, ``"airflow"``).
        Underscores are accepted as well (``"argo_workflows"``).
    **kwargs
        Passed through to the provider class constructor.

    Returns
    -------
    str or None
        Event ID if published successfully.
    """
    providers = _resolve_event_providers()

    if backend is not None:
        # Normalise so "argo_workflows" matches "argo-workflows"
        norm = backend.replace("_", "-")
        for provider_class in providers:
            if provider_class.TYPE == norm:
                return provider_class(name, payload=payload, **kwargs).publish()
        available = [p.TYPE for p in providers]
        raise ValueError(
            "Unknown event backend %r. Available: %s" % (backend, available)
        )

    # Auto-detect: pick the first configured provider
    for provider_class in providers:
        if hasattr(provider_class, "is_configured") and provider_class.is_configured():
            return provider_class(name, payload=payload, **kwargs).publish()

    available = [p.TYPE for p in providers]
    raise RuntimeError(
        "Cannot auto-detect event backend — no provider is configured. "
        "Set the 'backend' parameter explicitly or configure one of: %s" % available
    )


# Keep this line and make sure ALIASES_DESC is above this line.
process_integration_aliases(globals())
