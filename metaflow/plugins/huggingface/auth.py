"""
Pluggable HuggingFace authentication for the @huggingface decorator.

Implement HuggingFaceAuthProvider and register in HF_AUTH_PROVIDERS_DESC
to supply tokens from org-specific backends. If no provider is configured,
the decorator falls back to HF_TOKEN / HUGGING_FACE_HUB_TOKEN environment variables.
"""

import abc
from typing import Optional


class HuggingFaceAuthProvider(abc.ABC):
    """
    Interface for pluggable HuggingFace authentication.
    Providers are registered via HF_AUTH_PROVIDERS_DESC; the active provider
    is selected by METAFLOW_HUGGINGFACE_AUTH_PROVIDER (default: env).
    """

    # Unique identifier for this provider (e.g. "env", "netflix-internal")
    TYPE = None  # type: Optional[str]

    @abc.abstractmethod
    def get_token(self) -> Optional[str]:
        """
        Return the HuggingFace API token to use for this task, or None if no auth.
        """
        pass
