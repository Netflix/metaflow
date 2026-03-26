"""
Authentication hooks for ``@huggingface``.

Each task asks the active provider for a Hugging Face API token (or no token) once,
before ``snapshot_download`` or ``model_info``. Core ships ``env``, which returns the
first non-empty value among ``HF_TOKEN``, ``HUGGING_FACE_TOKEN``, and
``HUGGING_FACE_HUB_TOKEN``. For any other source, subclass ``HuggingFaceAuthProvider``,
register it through ``HF_AUTH_PROVIDERS_DESC`` in a ``metaflow_extensions`` ``plugins``
module, and select it with ``METAFLOW_HUGGINGFACE_AUTH_PROVIDER`` (or
``HUGGINGFACE_AUTH_PROVIDER`` in config) using your ``TYPE``. If the deployment sets
``ENABLED_HF_AUTH_PROVIDER``, include your ``TYPE`` in that allowlist.

User-facing documentation: ``docs/huggingface.md`` (Background, Behavior, Register a custom
auth provider). Plugin resolution: ``metaflow/extension_support/plugins.py``.
"""

import abc
from typing import Optional


class HuggingFaceAuthProvider(abc.ABC):
    """
    Subclass this when the Hub token should not come from the default environment
    variables. Flow code does not reference your class; Metaflow loads the provider
    from configuration and extensions.

    Set ``TYPE`` to your provider id. It must match the first element of your
    ``HF_AUTH_PROVIDERS_DESC`` entry and the configured
    ``METAFLOW_HUGGINGFACE_AUTH_PROVIDER`` / ``HUGGINGFACE_AUTH_PROVIDER`` value.
    Implement ``get_token()`` to return a bearer token, ``None`` for unauthenticated
    requests (public repos only), or raise if the step must fail. Metaflow invokes
    ``get_token()`` once per task before any Hugging Face I/O.

    Register the class from ``metaflow_extensions.../plugins/`` with a relative
    ``class_path``, for example
    ``[("acme-vault", ".hf_auth.AcmeVaultProvider")]``. Compare the core ``env`` entry
    in ``metaflow/plugins/__init__.py``:
    ``("env", ".huggingface.env_auth_provider.EnvHuggingFaceAuthProvider")``. The
    stock env implementation is ``EnvHuggingFaceAuthProvider`` in ``env_auth_provider.py``.
    Step-by-step instructions are in ``docs/huggingface.md`` (Register a custom auth provider).

    Example::

        class AcmeVaultProvider(HuggingFaceAuthProvider):
            TYPE = "acme-vault"

            def get_token(self):
                return get_secret("huggingface/token")
    """

    # Must match HF_AUTH_PROVIDERS_DESC name and METAFLOW_HUGGINGFACE_AUTH_PROVIDER
    TYPE = None  # type: Optional[str]

    @abc.abstractmethod
    def get_token(self) -> Optional[str]:
        """
        Return a token string, ``None`` for unauthenticated access, or raise.

        Called once per task before ``snapshot_download`` or ``model_info``.
        """
        pass
