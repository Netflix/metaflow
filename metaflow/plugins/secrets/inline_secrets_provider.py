from metaflow.plugins.secrets import SecretsProvider


class InlineSecretsProvider(SecretsProvider):
    TYPE = "inline"

    def get_secret_as_dict(self, secret_id, options={}, role=None):
        """Intended to be used for testing purposes only."""
        return options.get("env_vars", {})
