from typing import Any, Dict, List, Union

from metaflow.exception import MetaflowException
from metaflow.plugins.secrets.secrets_decorator import (
    SecretSpec,
    get_secrets_backend_provider,
)


def get_secrets(
    sources: List[Union[str, Dict[str, Any]]] = [], role: str = None
) -> Dict[SecretSpec, Dict[str, str]]:
    """
    Get secrets from sources

    Parameters
    ----------
    sources : List[Union[str, Dict[str, Any]]], default: []
        List of secret specs, defining how the secrets are to be retrieved
    role : str, optional
        Role to use for fetching secrets
    """
    if role is None:
        role = DEFAULT_SECRETS_ROLE

    # List of pairs (secret_spec, env_vars_from_this_spec)
    all_secrets = []
    secret_specs = []

    for secret_spec_str_or_dict in sources:
        if isinstance(secret_spec_str_or_dict, str):
            secret_specs.append(
                SecretSpec.secret_spec_from_str(secret_spec_str_or_dict, role=role)
            )
        elif isinstance(secret_spec_str_or_dict, dict):
            secret_specs.append(
                SecretSpec.secret_spec_from_dict(secret_spec_str_or_dict, role=role)
            )
        else:
            raise MetaflowException(
                "get_secrets sources items must be either a string or a dict"
            )

    for secret_spec in secret_specs:
        secrets_backend_provider = get_secrets_backend_provider(
            secret_spec.secrets_backend_type
        )
        try:
            dict_for_secret = secrets_backend_provider.get_secret_as_dict(
                secret_spec.secret_id,
                options=secret_spec.options,
                role=secret_spec.role,
            )
        except Exception as e:
            raise MetaflowException(
                "Failed to retrieve secret '%s': %s" % (secret_spec.secret_id, e)
            )

        all_secrets.append((secret_spec, dict_for_secret))

    return all_secrets
