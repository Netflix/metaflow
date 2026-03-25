"""
Pluggable HuggingFace authentication for the @huggingface decorator.

Implement HuggingFaceAuthProvider and register in HF_AUTH_PROVIDERS_DESC
to supply tokens from org-specific backends. If no provider is configured,
the decorator falls back to HF_TOKEN / HUGGING_FACE_HUB_TOKEN environment variables.

Built-in provider:
  - env (default): EnvHuggingFaceAuthProvider, reads HF_TOKEN / HUGGING_FACE_HUB_TOKEN.

Custom provider (e.g. another org's token service):
  1. Subclass HuggingFaceAuthProvider, set TYPE = "my-provider", implement get_token().
  2. Register via a Metaflow extension: in your plugin module set
     HF_AUTH_PROVIDERS_DESC = [("my-provider", "mymodule.MyAuthProvider")] and
     TOGGLE_HF_AUTH_PROVIDER = [("my-provider", "mymodule.MyAuthProvider")].
  3. Set METAFLOW_HUGGINGFACE_AUTH_PROVIDER=my-provider (or in config).
  See metaflow/plugins/__init__.py HF_AUTH_PROVIDERS_DESC and extension_support/plugins.py.
"""

import abc
from typing import Optional


class HuggingFaceAuthProvider(abc.ABC):
    """
    Interface for pluggable HuggingFace authentication.
    Providers are registered via HF_AUTH_PROVIDERS_DESC; the active provider
    is selected by HUGGINGFACE_AUTH_PROVIDER config (env: METAFLOW_HUGGINGFACE_AUTH_PROVIDER; default: env).
    """

    # Unique identifier for this provider
    TYPE = None  # type: Optional[str]

    @abc.abstractmethod
    def get_token(self) -> Optional[str]:
        """
        Return the HuggingFace API token to use for this task, or None if no auth.
        """
        pass
