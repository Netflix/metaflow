from typing import Any, Dict, Optional, Union

from metaflow.metaflow_config import DEFAULT_SECRETS_ROLE
from metaflow.exception import MetaflowException
from metaflow.plugins.secrets.secrets_spec import SecretSpec
from metaflow.plugins.secrets.utils import get_secrets_backend_provider


def get_secret(
    source: Union[str, Dict[str, Any]], role: Optional[str] = None
) -> Dict[str, str]:
    """
    Get secret from source

    Parameters
    ----------
    source : Union[str, Dict[str, Any]]
        Secret spec, defining how the secret is to be retrieved
    role : str, optional
        Role to use for fetching secrets
    """
    if role is None:
        role = DEFAULT_SECRETS_ROLE

    secret_spec = None

    if isinstance(source, str):
        secret_spec = SecretSpec.secret_spec_from_str(source, role=role)
    elif isinstance(source, dict):
        secret_spec = SecretSpec.secret_spec_from_dict(source, role=role)
    else:
        raise MetaflowException(
            "get_secrets sources items must be either a string or a dict"
        )

    secrets_backend_provider = get_secrets_backend_provider(
        secret_spec.secrets_backend_type
    )
    try:
        dict_for_secret = secrets_backend_provider.get_secret_as_dict(
            secret_spec.secret_id,
            options=secret_spec.options,
            role=secret_spec.role,
        )
        return dict_for_secret
    except Exception as e:
        raise MetaflowException(
            "Failed to retrieve secret '%s': %s" % (secret_spec.secret_id, e)
        )
