"""
Default ``@huggingface`` auth provider: read the Hub token from the process environment.

Used when ``HUGGINGFACE_AUTH_PROVIDER`` is ``env`` (the default). See ``docs/huggingface.md``
(Behavior) for variable precedence.
"""

import os
from typing import Optional

from .auth import HuggingFaceAuthProvider


class EnvHuggingFaceAuthProvider(HuggingFaceAuthProvider):
    """
    Returns the first non-empty value among ``HF_TOKEN``, ``HUGGING_FACE_TOKEN``, and
    ``HUGGING_FACE_HUB_TOKEN``, or ``None`` if all are unset.
    """

    TYPE = "env"

    def get_token(self) -> Optional[str]:
        return (
            os.environ.get("HF_TOKEN")
            or os.environ.get("HUGGING_FACE_TOKEN")
            or os.environ.get("HUGGING_FACE_HUB_TOKEN")
        )
