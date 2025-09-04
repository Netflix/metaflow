import abc
from typing import Dict, Optional


class SecretsProvider(abc.ABC):
    TYPE: Optional[str] = None

    @abc.abstractmethod
    def get_secret_as_dict(self, secret_id, options={}, role=None) -> Dict[str, str]:
        """Retrieve the secret from secrets backend, and return a dictionary of
        environment variables."""


from .secrets_func import get_secret
