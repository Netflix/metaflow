"""
Default HuggingFace auth provider: read token from environment variables.
"""

import os
from typing import Optional

from .auth import HuggingFaceAuthProvider


class EnvHuggingFaceAuthProvider(HuggingFaceAuthProvider):
    """
    Supplies HuggingFace token from HF_TOKEN, then HUGGING_FACE_TOKEN, then
    HUGGING_FACE_HUB_TOKEN (first non-empty wins).
    """

    TYPE = "env"

    def get_token(self) -> Optional[str]:
        return (
            os.environ.get("HF_TOKEN")
            or os.environ.get("HUGGING_FACE_TOKEN")
            or os.environ.get("HUGGING_FACE_HUB_TOKEN")
        )
